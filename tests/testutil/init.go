package testutil

import (
	"os"
	"sync"

	"github.com/tiz36/zmsg/zmsg"
)

var (
	testConfig     zmsg.Config
	testConfigOnce sync.Once
)

func InitEnv() {
	testConfigOnce.Do(func() {
		cfg := zmsg.DefaultConfig()
		cfg.Postgres.DSN = getenv("test_POSTGRES_DSN", "postgresql://postgres:postgres@localhost/test?sslmode=disable")
		cfg.Redis.Addr = getenv("test_REDIS_ADDR", "localhost:6379")
		cfg.Redis.Password = "123456"
		cfg.Queue.Addr = getenv("test_QUEUE_ADDR", cfg.Redis.Addr)
		cfg.Queue.Password = "123456"
		testConfig = cfg
	})
}

func NewConfig() zmsg.Config {
	InitEnv()
	return testConfig
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
