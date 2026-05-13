package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

type Server struct {
	store    *session.Store
	registry *adapters.Registry
}

func NewServer(store *session.Store, registry *adapters.Registry) http.Handler {
	s := &Server{store: store, registry: registry}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Post("/sessions", s.createSession)
	r.Delete("/sessions/{id}", s.deleteSession)

	r.Post("/sessions/{id}/mocks", s.createMock)
	r.Get("/sessions/{id}/mocks", s.listMocks)
	r.Delete("/sessions/{id}/mocks/{ruleId}", s.deleteMock)

	r.Get("/sessions/{id}/traffic", s.getTraffic)

	r.Get("/status", s.getStatus)

	return r
}
