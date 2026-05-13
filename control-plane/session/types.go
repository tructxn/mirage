package session

import "time"

type Rule struct {
	ID        string                 `json:"id"`
	Protocol  string                 `json:"protocol"`
	Match     map[string]interface{} `json:"match"`
	Response  interface{}            `json:"response"`
	Priority  int                    `json:"priority"`
	BackendID string                 `json:"-"`
}

type TrafficRecord struct {
	Timestamp     time.Time              `json:"timestamp"`
	Protocol      string                 `json:"protocol"`
	Request       map[string]interface{} `json:"request"`
	MatchedRuleID *string                `json:"matched_rule_id"`
	NearMiss      *NearMiss              `json:"near_miss,omitempty"`
	Response      interface{}            `json:"response"`
	DurationMS    int64                  `json:"duration_ms"`
}

type NearMiss struct {
	RuleID      string `json:"rule_id"`
	FailedField string `json:"failed_field"`
}

type Session struct {
	ID    string
	Rules []Rule
}
