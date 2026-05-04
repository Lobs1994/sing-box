package loadbalance

import (
	"context"
	"hash/fnv"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

var (
	_ adapter.Outbound      = (*LoadBalance)(nil)
	_ adapter.OutboundGroup = (*LoadBalance)(nil)
)

type LoadBalance struct {
	myOutboundAdapter

	ctx    context.Context
	logger log.ContextLogger

	tags      []string
	objective string
	strategy  string

	healthURL      string
	healthInterval time.Duration

	outboundManager adapter.OutboundManager
	outbounds       []adapter.Outbound

	health *HealthChecker

	rr uint64

	mu      sync.RWMutex
	started bool

	// BugI/J修复：initialCheckDone 的读写都在 mu 保护下进行，
	// Start() goroutine 持有本地副本而非直接读结构体字段，
	// 彻底消除 Close 并发时的 data race 和新 channel 永不关闭问题
	initialCheckDone chan struct{}
}

type myOutboundAdapter struct {
	protocol     string
	network      []string
	tag          string
	dependencies []string
}

func (a *myOutboundAdapter) Type() string           { return a.protocol }
func (a *myOutboundAdapter) Tag() string            { return a.tag }
func (a *myOutboundAdapter) Network() []string      { return a.network }
func (a *myOutboundAdapter) Dependencies() []string { return a.dependencies }

func NewLoadBalance(
	ctx context.Context,
	router adapter.Router,
	logger log.ContextLogger,
	tag string,
	options option.LoadBalanceOutboundOptions,
) (adapter.Outbound, error) {

	om := service.FromContext[adapter.OutboundManager](ctx)

	objective := options.Objective
	if objective == "" {
		objective = "alive"
	}
	strategy := options.Strategy
	if strategy == "" {
		strategy = "roundrobin"
	}

	healthInterval := time.Minute
	if options.Interval > 0 {
		healthInterval = time.Duration(options.Interval)
	}
	healthURL := options.URL
	if healthURL == "" {
		healthURL = "https://www.gstatic.com/generate_204"
	}

	return &LoadBalance{
		myOutboundAdapter: myOutboundAdapter{
			protocol:     C.TypeLoadBalance,
			network:      []string{N.NetworkTCP, N.NetworkUDP},
			tag:          tag,
			dependencies: options.Outbounds,
		},
		ctx:             ctx,
		logger:          logger,
		tags:            options.Outbounds,
		objective:       objective,
		strategy:        strategy,
		healthURL:       healthURL,
		healthInterval:  healthInterval,
		outboundManager: om,
		// BugF修复：构造时初始化为永不关闭的占位 channel，
		// Start() 前调用 DialContext 时 ctx.Done() 能正常返回错误而非永久阻塞
		initialCheckDone: make(chan struct{}),
	}, nil
}

func (lb *LoadBalance) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStatePostStart {
		return nil
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	if lb.started {
		return nil
	}

	for _, tag := range lb.tags {
		if out, ok := lb.outboundManager.Outbound(tag); ok {
			lb.outbounds = append(lb.outbounds, out)
			lb.logger.Info("loadbalance: added outbound: ", tag)
		} else {
			lb.logger.Error("loadbalance: outbound not found: ", tag)
		}
	}

	if len(lb.outbounds) == 0 {
		return E.New("loadbalance: no valid outbounds")
	}

	lb.health = NewHealthChecker(
		lb.ctx,
		lb.outbounds,
		lb.healthURL,
		lb.healthInterval,
		lb.logger,
	)

	// BugB修复：每次 Start() 新建 channel，避免 double-close panic
	// BugJ修复：将新建的 channel 同时赋给本地变量 doneCh，
	// goroutine 闭包捕获本地变量而非结构体字段，
	// 即使 Close() 后 lb.initialCheckDone 被替换，goroutine 仍然 close 正确的那个 channel
	doneCh := make(chan struct{})
	lb.initialCheckDone = doneCh

	lb.started = true

	go func() {
		lb.health.StartSync()
		// BugJ修复：close 本地副本 doneCh，与 lb.initialCheckDone 的后续替换无关
		close(doneCh)
		lb.health.StartLoop()
	}()

	return nil
}

func (lb *LoadBalance) Close() error {
	// Bug10修复：锁内只更新字段，锁外再调用 health.Close()
	lb.mu.Lock()
	health := lb.health
	lb.health = nil
	lb.started = false
	lb.outbounds = nil
	// BugF修复：重置为新的未关闭 channel，
	// 防止 Close 后残留请求读到已关闭 channel 而意外放行
	// BugI修复：在 mu.Lock() 内写 initialCheckDone，与 readInitialCheckDone() 的 RLock 配合，
	// 消除对该字段的并发 data race
	lb.initialCheckDone = make(chan struct{})
	lb.mu.Unlock()

	if health != nil {
		health.Close()
	}
	return nil
}

// readInitialCheckDone 在 mu.RLock() 保护下读取 initialCheckDone
// BugI修复：消除 DialContext 裸读与 Close() 写之间的 data race
func (lb *LoadBalance) readInitialCheckDone() chan struct{} {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.initialCheckDone
}

/* ---------------- filtering ---------------- */

func (lb *LoadBalance) filter() []adapter.Outbound {
	var result []adapter.Outbound
	for _, out := range lb.outbounds {
		if lb.health.IsHealthy(out.Tag()) {
			result = append(result, out)
		}
	}
	if len(result) == 0 {
		lb.logger.Warn("loadbalance: all nodes unhealthy")
		return nil
	}
	return result
}

/* ---------------- selection ---------------- */

func (lb *LoadBalance) orderedCandidates(ctx context.Context, dest M.Socksaddr) []adapter.Outbound {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if !lb.started {
		return nil
	}

	pool := lb.filter()
	n := len(pool)
	if n == 0 {
		return nil
	}

	var startIndex uint64

	switch lb.strategy {
	case "consistenthash", "consistent_hash":
		// Bug7修复：Jump Consistent Hash
		h := fnv.New64a()
		h.Write([]byte(dest.String()))
		startIndex = jumpConsistentHash(h.Sum64(), n)

	default:
		startIndex = atomic.AddUint64(&lb.rr, 1) % uint64(n)
	}

	ordered := make([]adapter.Outbound, n)
	for i := 0; i < n; i++ {
		ordered[i] = pool[(startIndex+uint64(i))%uint64(n)]
	}
	return ordered
}

/* ---------------- dialing ---------------- */

// healthSnapshot 在锁保护下安全读取 health 引用（BugG修复）
func (lb *LoadBalance) healthSnapshot() *HealthChecker {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return lb.health
}

func (lb *LoadBalance) DialContext(
	ctx context.Context,
	network string,
	dest M.Socksaddr,
) (net.Conn, error) {
	// Bug4修复：等待初次健康检查完成
	// BugI修复：通过 readInitialCheckDone() 在锁内读取，消除 data race
	select {
	case <-lb.readInitialCheckDone():
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	ordered := lb.orderedCandidates(ctx, dest)
	if len(ordered) == 0 {
		return nil, E.New("loadbalance: no outbound available")
	}

	// BugG修复：锁内读取 health 快照
	health := lb.healthSnapshot()
	if health == nil {
		return nil, E.New("loadbalance: not started")
	}

	var lastErr error
	for _, picked := range ordered {
		dialer, ok := picked.(N.Dialer)
		if !ok {
			continue
		}

		conn, err := dialer.DialContext(ctx, network, dest)
		if err != nil {
			lb.logger.Debug("loadbalance: ", picked.Tag(), " failed: ", err)
			lastErr = err
			// Bug8修复：失败立即熔断 + 触发快速重检
			health.ReportFailure(picked.Tag())
			health.CheckOne(picked)
			continue
		}

		return conn, nil
	}

	return nil, E.New("loadbalance: all outbounds failed, last error: ", lastErr)
}

func (lb *LoadBalance) ListenPacket(
	ctx context.Context,
	dest M.Socksaddr,
) (net.PacketConn, error) {
	// BugI修复：同 DialContext
	select {
	case <-lb.readInitialCheckDone():
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	ordered := lb.orderedCandidates(ctx, dest)
	if len(ordered) == 0 {
		return nil, E.New("loadbalance: no outbound available")
	}

	health := lb.healthSnapshot()
	if health == nil {
		return nil, E.New("loadbalance: not started")
	}

	var lastErr error
	for _, picked := range ordered {
		dialer, ok := picked.(N.Dialer)
		if !ok {
			continue
		}

		conn, err := dialer.ListenPacket(ctx, dest)
		if err != nil {
			lb.logger.Debug("loadbalance: ", picked.Tag(), " failed: ", err)
			lastErr = err
			health.ReportFailure(picked.Tag())
			health.CheckOne(picked)
			continue
		}

		return conn, nil
	}

	return nil, E.New("loadbalance: all outbounds failed, last error: ", lastErr)
}

/* ---------------- Clash API ---------------- */

func (lb *LoadBalance) Now() string {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if !lb.started || len(lb.outbounds) == 0 {
		return ""
	}

	candidates := lb.filter()
	if len(candidates) == 0 {
		return ""
	}
	n := uint64(len(candidates))
	nextRR := atomic.LoadUint64(&lb.rr) + 1
	return candidates[nextRR%n].Tag()
}

func (lb *LoadBalance) All() []string {
	return lb.tags
}

func (lb *LoadBalance) CheckOutbounds() []adapter.Outbound {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	out := make([]adapter.Outbound, len(lb.outbounds))
	copy(out, lb.outbounds)
	return out
}

// jumpConsistentHash 实现 Google Jump Consistent Hash 算法
// 论文: https://arxiv.org/abs/1406.2294
func jumpConsistentHash(key uint64, numBuckets int) uint64 {
	var b, j int64
	b = -1
	j = 0
	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(math.Floor(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1))))
	}
	return uint64(b)
}