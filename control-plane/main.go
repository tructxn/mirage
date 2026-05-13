package main

import (
	"log"
	"net/http"
	"os"

	"github.com/tructxn/mirage/control-plane/adapters"
	"github.com/tructxn/mirage/control-plane/api"
	"github.com/tructxn/mirage/control-plane/session"
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	store := session.NewStore()
	registry := adapters.NewRegistry()

	wireMockURL := getenv("WIREMOCK_URL", "http://wiremock:8080")
	registry.Register("http", adapters.NewWireMockAdapter(wireMockURL).WithStore(store))

	redisAddr := getenv("REDIS_ADDR", "redis:6380")
	registry.Register("redis", adapters.NewRedisAdapter(redisAddr))

	kafkaBroker := getenv("KAFKA_BROKER", "redpanda:9093")
	registry.Register("kafka", adapters.NewKafkaAdapter(kafkaBroker))

	mysqlDSN := getenv("MYSQL_DSN", "root:mirage@tcp(mysql:3307)/mirage")
	registry.Register("mysql", adapters.NewMySQLAdapter(mysqlDSN))

	srv := api.NewServer(store, registry)

	addr := getenv("LISTEN_ADDR", ":9000")
	log.Printf("mirage control plane listening on %s", addr)
	if err := http.ListenAndServe(addr, srv); err != nil {
		log.Fatal(err)
	}
}
