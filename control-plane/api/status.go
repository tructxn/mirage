package api

import (
	"encoding/json"
	"net/http"
	"time"
)

type backendStatus struct {
	Healthy   bool      `json:"healthy"`
	CheckedAt time.Time `json:"checked_at"`
}

func (s *Server) getStatus(w http.ResponseWriter, r *http.Request) {
	result := make(map[string]backendStatus)
	for proto, adapter := range s.registry.All() {
		result[proto] = backendStatus{
			Healthy:   adapter.Healthy(),
			CheckedAt: time.Now().UTC(),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}
