package adapters

import "github.com/tructxn/mirage/control-plane/session"

type RedisAdapter struct{ addr string }

func NewRedisAdapter(addr string) *RedisAdapter { return &RedisAdapter{addr: addr} }
func (a *RedisAdapter) PushRule(sessionID string, rule *session.Rule) error  { return nil }
func (a *RedisAdapter) DeleteRule(sessionID string, rule session.Rule) error  { return nil }
func (a *RedisAdapter) Reset(sessionID string, rules []session.Rule) error    { return nil }
func (a *RedisAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	return nil, nil
}
func (a *RedisAdapter) Healthy() bool { return true }
