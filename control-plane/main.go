package main

import (
	"log"
	"net/http"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/api"
	"github.com/tructxn/mirage/control-plane/session"
)

func main() {
	store := session.NewStore()
	registry := adapters.NewRegistry()
	registry.Register("http", adapters.NewWireMockAdapter("http://wiremock:8080"))
	registry.Register("redis", adapters.NewRedisAdapter("redis:6380"))
	srv := api.NewServer(store, registry)
	log.Println("mirage control plane listening on :9000")
	if err := http.ListenAndServe(":9000", srv); err != nil {
		log.Fatal(err)
	}
}
