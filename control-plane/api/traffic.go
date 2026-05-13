package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/tructxn/mirage/control-plane/session"
)

func (s *Server) getTraffic(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if s.store.Get(id) == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	var records []session.TrafficRecord
	rules := s.store.Rules(id)

	seen := map[string]bool{}
	for _, rule := range rules {
		if seen[rule.Protocol] {
			continue
		}
		seen[rule.Protocol] = true
		if adapter, ok := s.registry.Get(rule.Protocol); ok {
			got, err := adapter.Traffic(id)
			if err == nil {
				records = append(records, got...)
			}
		}
	}
	if records == nil {
		records = []session.TrafficRecord{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(records)
}
