package option

import "github.com/sagernet/sing/common/json/badoption"

// LoadBalanceOutboundOptions is the options for balancer outbound
type LoadBalanceOutboundOptions struct {
	DialerOptions
	Outbounds []string           `json:"outbounds"`
	URL       string             `json:"url,omitempty"`
	Interval  badoption.Duration `json:"interval,omitempty"`
	Objective string             `json:"objective,omitempty"`
	Strategy  string             `json:"strategy,omitempty"`
}