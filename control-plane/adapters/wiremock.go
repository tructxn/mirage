package adapters

import "github.com/tructxn/mirage/control-plane/session"

type WireMockAdapter struct{ baseURL string }

func NewWireMockAdapter(baseURL string) *WireMockAdapter { return &WireMockAdapter{baseURL: baseURL} }
func (a *WireMockAdapter) PushRule(sessionID string, rule *session.Rule) error  { return nil }
func (a *WireMockAdapter) DeleteRule(sessionID string, rule session.Rule) error  { return nil }
func (a *WireMockAdapter) Reset(sessionID string, rules []session.Rule) error    { return nil }
func (a *WireMockAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil
}
func (a *WireMockAdapter) Healthy() bool { return true }
