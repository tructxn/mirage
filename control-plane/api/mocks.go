package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/tructxn/mirage/control-plane/session"
)

func (s *Server) createMock(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	var rule session.Rule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	rule.ID = uuid.NewString()

	if adapter, ok := s.registry.Get(rule.Protocol); ok {
		if err := adapter.PushRule(id, &rule); err != nil {
			http.Error(w, "backend error: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	s.store.AddRule(id, rule)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": rule.ID})
}

func (s *Server) listMocks(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	rules := s.store.Rules(id)
	if rules == nil {
		rules = []session.Rule{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(rules)
}

func (s *Server) deleteMock(w http.ResponseWriter, r *http.Request) {
	sessID := chi.URLParam(r, "id")
	ruleID := chi.URLParam(r, "ruleId")

	if s.store.Get(sessID) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	rules := s.store.Rules(sessID)
	var found *session.Rule
	for i := range rules {
		if rules[i].ID == ruleID {
			found = &rules[i]
			break
		}
	}
	if found == nil {
		http.Error(w, "rule not found", http.StatusNotFound)
		return
	}

	if adapter, ok := s.registry.Get(found.Protocol); ok {
		_ = adapter.DeleteRule(sessID, *found)
	}
	s.store.DeleteRule(sessID, ruleID)
	w.WriteHeader(http.StatusNoContent)
}
