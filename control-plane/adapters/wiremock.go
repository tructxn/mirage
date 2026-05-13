package adapters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/tructxn/mirage/control-plane/session"
)

// RuleStore is the subset of session.Store the WireMock adapter needs.
type RuleStore interface {
	RuleByBackendID(sessionID, backendID string) (session.Rule, bool)
}

type WireMockAdapter struct {
	baseURL string
	client  *http.Client
	store   RuleStore
}

func NewWireMockAdapter(baseURL string) *WireMockAdapter {
	return &WireMockAdapter{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (a *WireMockAdapter) WithStore(store RuleStore) *WireMockAdapter {
	a.store = store
	return a
}

// PushRule creates a WireMock stub from the rule.
// Sets rule.BackendID to the WireMock stub UUID on success.
func (a *WireMockAdapter) PushRule(sessionID string, rule *session.Rule) error {
	method, _ := rule.Match["method"].(string)
	path, _ := rule.Match["path"].(string)

	resp, _ := rule.Response.(map[string]any)
	status := 200
	if s, ok := resp["status"].(float64); ok {
		status = int(s)
	}
	body := ""
	if b, ok := resp["body"].(string); ok {
		body = b
	}

	payload := map[string]any{
		"request": map[string]any{
			"method": method,
			"url":    path,
		},
		"response": map[string]any{
			"status": status,
			"body":   body,
		},
		"metadata": map[string]string{
			"mirage-session": sessionID,
			"mirage-rule":    rule.ID,
		},
	}

	data, _ := json.Marshal(payload)
	res, err := a.client.Post(a.baseURL+"/__admin/mappings", "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("wiremock post: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		return fmt.Errorf("wiremock returned %d", res.StatusCode)
	}

	var wmResp map[string]string
	json.NewDecoder(res.Body).Decode(&wmResp)
	rule.BackendID = wmResp["id"]
	return nil
}

func (a *WireMockAdapter) DeleteRule(sessionID string, rule session.Rule) error {
	if rule.BackendID == "" {
		return nil
	}
	req, _ := http.NewRequest(http.MethodDelete, a.baseURL+"/__admin/mappings/"+rule.BackendID, nil)
	res, err := a.client.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	return nil
}

func (a *WireMockAdapter) Reset(sessionID string, rules []session.Rule) error {
	for _, r := range rules {
		_ = a.DeleteRule(sessionID, r)
	}
	return nil
}

func (a *WireMockAdapter) fetchMatchedRequests(sessionID string) ([]session.TrafficRecord, error) {
	res, err := a.client.Get(a.baseURL + "/__admin/requests")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var payload struct {
		Requests []struct {
			Request struct {
				Method  string            `json:"method"`
				URL     string            `json:"url"`
				Headers map[string]string `json:"headers"`
			} `json:"request"`
			WasMatched    bool   `json:"wasMatched"`
			StubMappingID string `json:"stubMappingId"`
			ResponseDef   struct {
				Status int    `json:"status"`
				Body   string `json:"body"`
			} `json:"responseDefinition"`
		} `json:"requests"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}

	var records []session.TrafficRecord
	for _, req := range payload.Requests {
		rec := session.TrafficRecord{
			Timestamp: time.Now().UTC(),
			Protocol:  "http",
			Request: map[string]any{
				"method":  req.Request.Method,
				"url":     req.Request.URL,
				"headers": req.Request.Headers,
			},
			Response: map[string]any{
				"status": req.ResponseDef.Status,
				"body":   req.ResponseDef.Body,
			},
		}
		if req.WasMatched && req.StubMappingID != "" && a.store != nil {
			if rule, ok := a.store.RuleByBackendID(sessionID, req.StubMappingID); ok {
				id := rule.ID
				rec.MatchedRuleID = &id
			}
		}
		records = append(records, rec)
	}
	return records, nil
}

func (a *WireMockAdapter) fetchNearMisses() ([]session.TrafficRecord, error) {
	res, err := a.client.Get(a.baseURL + "/__admin/requests/unmatched")
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var payload struct {
		Requests []struct {
			Request struct {
				Method string `json:"method"`
				URL    string `json:"url"`
			} `json:"request"`
			NearMissRequests []struct {
				Mapping struct {
					ID string `json:"id"`
				} `json:"mapping"`
				RequestMatchResult struct {
					AttributeMatchResults []struct {
						Key     string `json:"key"`
						Matched bool   `json:"matched"`
					} `json:"attributeMatchResults"`
				} `json:"requestMatchResult"`
			} `json:"nearMissRequests"`
		} `json:"requests"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nil, err
	}

	var records []session.TrafficRecord
	for _, req := range payload.Requests {
		rec := session.TrafficRecord{
			Timestamp: time.Now().UTC(),
			Protocol:  "http",
			Request: map[string]any{
				"method": req.Request.Method,
				"url":    req.Request.URL,
			},
		}
		if len(req.NearMissRequests) > 0 {
			nm := req.NearMissRequests[0]
			failedField := "unknown"
			for _, f := range nm.RequestMatchResult.AttributeMatchResults {
				if !f.Matched {
					failedField = f.Key
					break
				}
			}
			var ruleID string
			if a.store != nil {
				if rule, ok := a.store.RuleByBackendID("", nm.Mapping.ID); ok {
					ruleID = rule.ID
				}
			}
			rec.NearMiss = &session.NearMiss{
				RuleID:      ruleID,
				FailedField: failedField,
			}
		}
		records = append(records, rec)
	}
	return records, nil
}

func (a *WireMockAdapter) Traffic(sessionID string) ([]session.TrafficRecord, error) {
	matched, err := a.fetchMatchedRequests(sessionID)
	if err != nil {
		return nil, err
	}
	unmatched, _ := a.fetchNearMisses()
	return append(matched, unmatched...), nil
}

func (a *WireMockAdapter) Healthy() bool {
	res, err := a.client.Get(a.baseURL + "/__admin/health")
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode == http.StatusOK
}
