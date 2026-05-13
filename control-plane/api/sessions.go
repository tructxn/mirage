package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tructxn/mirage/control-plane/session"
)

func (s *Server) createSession(w http.ResponseWriter, r *http.Request) {
	sess := s.store.Create()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"id": sess.ID})
}

func (s *Server) deleteSession(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	rules := s.store.Rules(id)
	for proto, adapter := range s.registry.All() {
		protoRules := filterByProtocol(rules, proto)
		if len(protoRules) > 0 {
			_ = adapter.Reset(id, protoRules)
		}
	}
	s.store.Delete(id)
	w.WriteHeader(http.StatusNoContent)
}

func filterByProtocol(rules []session.Rule, proto string) []session.Rule {
	var out []session.Rule
	for _, r := range rules {
		if r.Protocol == proto {
			out = append(out, r)
		}
	}
	return out
}
