package adapters

import (
	"maps"

	"github.com/tructxn/mirage/control-plane/session"
)

type BackendAdapter interface {
	PushRule(sessionID string, rule *session.Rule) error
	DeleteRule(sessionID string, rule session.Rule) error
	Reset(sessionID string, rules []session.Rule) error
	Traffic(sessionID string) ([]session.TrafficRecord, error)
	Healthy() bool
}

type Registry struct {
	adapters map[string]BackendAdapter
}

func NewRegistry() *Registry { return &Registry{adapters: make(map[string]BackendAdapter)} }
func (r *Registry) Register(protocol string, adapter BackendAdapter) {
	r.adapters[protocol] = adapter
}
func (r *Registry) Get(protocol string) (BackendAdapter, bool) {
	a, ok := r.adapters[protocol]
	return a, ok
}
func (r *Registry) All() map[string]BackendAdapter {
	out := make(map[string]BackendAdapter, len(r.adapters))
	maps.Copy(out, r.adapters)
	return out
}
