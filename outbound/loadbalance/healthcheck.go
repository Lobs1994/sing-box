package loadbalance

import (
	"context"
	"crypto/tls"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const (
	unhealthyVal uint32 = 0
	healthyVal   uint32 = 1

	checkConcurrency    = 10
	checkOneConcurrency = 5
)

type HealthChecker struct {
	ctx    context.Context
	cancel context.CancelFunc

	outbounds []adapter.Outbound
	interval  time.Duration
	logger    log.ContextLogger

	// BugH修复：构造时缓存解析结果，避免每次 checkOne 重复 parse
	testURL  string
	parsedU  *url.URL
	testDest M.Socksaddr
	testName string

	// BugC修复：status 和 checking 使用各自独立的锁
	status   map[string]*uint32
	statusMu sync.RWMutex

	checking   map[string]*int32
	checkingMu sync.RWMutex

	// BugA修复：runChecks 和 CheckOne 使用独立的 sem
	sem    chan struct{}
	oneSem chan struct{}

	// Bug5修复：追踪后台 loop goroutine，Close() 等待其真正退出
	stopWg sync.WaitGroup
}

func NewHealthChecker(
	ctx context.Context,
	outbounds []adapter.Outbound,
	urlStr string,
	interval time.Duration,
	logger log.ContextLogger,
) *HealthChecker {

	ctx, cancel := context.WithCancel(ctx)

	if urlStr == "" {
		urlStr = "https://www.gstatic.com/generate_204"
	}

	parsedU, err := url.Parse(urlStr)
	if err != nil {
		urlStr = "https://www.gstatic.com/generate_204"
		parsedU, _ = url.Parse(urlStr)
	}

	host := parsedU.Host
	if !strings.Contains(host, ":") {
		host += ":443"
	}

	hc := &HealthChecker{
		ctx:       ctx,
		cancel:    cancel,
		outbounds: outbounds,
		interval:  interval,
		logger:    logger,
		testURL:   urlStr,
		parsedU:   parsedU,
		testDest:  M.ParseSocksaddr(host),
		testName:  parsedU.Hostname(),
		status:    make(map[string]*uint32),
		checking:  make(map[string]*int32),
		sem:       make(chan struct{}, checkConcurrency),
		oneSem:    make(chan struct{}, checkOneConcurrency),
	}

	for _, out := range outbounds {
		v := new(uint32)
		*v = unhealthyVal
		hc.status[out.Tag()] = v

		c := new(int32)
		hc.checking[out.Tag()] = c
	}

	return hc
}

func (hc *HealthChecker) checkingPtr(tag string) *int32 {
	hc.checkingMu.RLock()
	ptr, ok := hc.checking[tag]
	hc.checkingMu.RUnlock()
	if ok {
		return ptr
	}

	hc.checkingMu.Lock()
	defer hc.checkingMu.Unlock()

	if ptr, ok := hc.checking[tag]; ok {
		return ptr
	}

	v := new(int32)
	hc.checking[tag] = v
	return v
}

func (hc *HealthChecker) checkOne(out adapter.Outbound) {
	tag := out.Tag()

	ptr := hc.checkingPtr(tag)
	if !atomic.CompareAndSwapInt32(ptr, 0, 1) {
		return
	}
	defer atomic.StoreInt32(ptr, 0)

	dialer, ok := out.(N.Dialer)
	if !ok {
		hc.setUnhealthy(tag)
		return
	}

	ctx, cancel := context.WithTimeout(hc.ctx, 6*time.Second)
	defer cancel()

	dest := hc.testDest
	hostname := hc.testName
	parsedU := hc.parsedU

	conn, err := dialer.DialContext(ctx, N.NetworkTCP, dest)
	if err != nil {
		hc.setUnhealthy(tag)
		return
	}

	tlsConn := tls.Client(conn, &tls.Config{
		ServerName: hostname,
	})

	err = tlsConn.HandshakeContext(ctx)

	if err != nil {
		tlsConn.Close()

		conn2, err2 := dialer.DialContext(ctx, N.NetworkTCP, dest)
		if err2 != nil {
			hc.setUnhealthy(tag)
			return
		}

		tlsConn = tls.Client(conn2, &tls.Config{
			ServerName:         hostname,
			InsecureSkipVerify: true,
		})

		err = tlsConn.HandshakeContext(ctx)
		if err != nil {
			tlsConn.Close()
			hc.setUnhealthy(tag)
			return
		}
	}

	req := "GET " + parsedU.RequestURI() + " HTTP/1.1\r\n" +
		"Host: " + hostname + "\r\n" +
		"User-Agent: sing-box\r\n" +
		"Connection: close\r\n\r\n"

	_, err = tlsConn.Write([]byte(req))
	if err == nil {
		buf := make([]byte, 256)
		n, err := tlsConn.Read(buf)
		if err == nil || err == io.EOF {
			resp := string(buf[:n])
			lines := strings.Split(resp, "\r\n")
			if len(lines) > 0 {
				line := lines[0]
				if strings.Contains(line, " 2") ||
					strings.Contains(line, " 301") ||
					strings.Contains(line, " 302") {
					tlsConn.Close()
					hc.setHealthy(tag)
					return
				}
			}
		}
	}

	tlsConn.Close()
	hc.setHealthy(tag)
}

// BugD修复：select 等 sem 时监听 ctx；BugE修复：break label 跳出 for
func (hc *HealthChecker) runChecks() {
	var wg sync.WaitGroup

loop:
	for _, out := range hc.outbounds {
		select {
		case hc.sem <- struct{}{}:
		case <-hc.ctx.Done():
			break loop
		}

		wg.Add(1)
		go func(o adapter.Outbound) {
			defer wg.Done()
			defer func() { <-hc.sem }()
			hc.checkOne(o)
		}(out)
	}

	wg.Wait()
}

func (hc *HealthChecker) StartSync() {
	hc.runChecks()
}

func (hc *HealthChecker) StartLoop() {
	interval := hc.interval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	hc.stopWg.Add(1)
	go func() {
		defer hc.stopWg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-hc.ctx.Done():
				return
			case <-ticker.C:
				hc.runChecks()
			}
		}
	}()
}

func (hc *HealthChecker) setHealthy(tag string) {
	ptr := hc.statusPtr(tag)
	atomic.StoreUint32(ptr, healthyVal)
}

func (hc *HealthChecker) setUnhealthy(tag string) {
	ptr := hc.statusPtr(tag)
	atomic.StoreUint32(ptr, unhealthyVal)
}

func (hc *HealthChecker) statusPtr(tag string) *uint32 {
	hc.statusMu.RLock()
	ptr, ok := hc.status[tag]
	hc.statusMu.RUnlock()

	if ok {
		return ptr
	}

	hc.statusMu.Lock()
	defer hc.statusMu.Unlock()

	if ptr, ok := hc.status[tag]; ok {
		return ptr
	}

	v := new(uint32)
	*v = unhealthyVal
	hc.status[tag] = v
	return v
}

func (hc *HealthChecker) IsHealthy(tag string) bool {
	hc.statusMu.RLock()
	ptr, ok := hc.status[tag]
	hc.statusMu.RUnlock()

	if !ok {
		return false
	}
	return atomic.LoadUint32(ptr) == healthyVal
}

func (hc *HealthChecker) ReportFailure(tag string) {
	hc.setUnhealthy(tag)
}

// BugA修复：独立 oneSem，不与 runChecks 竞争
func (hc *HealthChecker) CheckOne(out adapter.Outbound) {
	select {
	case hc.oneSem <- struct{}{}:
		go func() {
			defer func() { <-hc.oneSem }()
			hc.checkOne(out)
		}()
	default:
	}
}

func (hc *HealthChecker) Close() {
	hc.cancel()
	hc.stopWg.Wait()
}