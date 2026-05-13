package api

import (
	"net/http"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/session"
)

type Server struct {
	store    *session.Store
	registry *adapters.Registry
}

func NewServer(store *session.Store, registry *adapters.Registry) http.Handler {
	return http.NewServeMux()
}
