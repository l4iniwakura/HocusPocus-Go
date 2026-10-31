// Command hocuspocus runs the standalone collaboration server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/extensions/database"
	hpRedis "github.com/l4iniwakura/HocusPocus-Go/extensions/redis"
	hpSQLite "github.com/l4iniwakura/HocusPocus-Go/extensions/sqlite"
	redisclient "github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	address := flag.String("listen", "127.0.0.1:1234", "HTTP/WebSocket listen address")
	maxConnections := flag.Int("max-connections", 10000, "maximum concurrent sockets (0 disables limit)")
	sqlitePath := flag.String("sqlite", "", "SQLite file; empty uses ephemeral memory")
	redisAddress := flag.String("redis", "", "Redis address for cross-process collaboration")
	redisPrefix := flag.String("redis-prefix", "hocuspocus", "Redis namespace")
	flag.Parse()
	cfg := hp.DefaultConfiguration()
	cfg.MaxConnections = *maxConnections
	cfg.Extensions = []hp.Extension{&database.Database{Storage: database.NewMemory()}}
	if *sqlitePath != "" {
		storage, err := hpSQLite.New(*sqlitePath)
		if err != nil {
			return err
		}
		cfg.Extensions = []hp.Extension{storage}
	}
	if *redisAddress != "" {
		client := redisclient.NewClient(&redisclient.Options{Addr: *redisAddress, Password: os.Getenv("REDIS_PASSWORD")})
		defer client.Close()
		cfg.Extensions = append(cfg.Extensions, &hpRedis.Redis{Client: client, Prefix: *redisPrefix, DisconnectDelay: time.Second})
	}
	cfg.Hooks = hp.Hooks{
		hp.OnRequest: func(_ context.Context, p *hp.Payload) error {
			if p.Request.URL.Path == "/health" {
				p.Response.Header().Set("Content-Type", "text/plain")
				_, err := p.Response.Write([]byte("ok\n"))
				return err
			}
			p.Response.WriteHeader(404)
			return nil
		},
		hp.OnStateless: func(_ context.Context, p *hp.Payload) error { p.Connection.SendStateless(p.Message); return nil },
	}
	s, err := hp.New(cfg)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		return err
	}
	fmt.Printf("LISTEN %s\n", listener.Addr())
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := make(chan error, 1)
	go func() { result <- s.Serve(listener) }()
	select {
	case err = <-result:
		return err
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err = s.Shutdown(shutdown)
	_ = listener.Close()
	return err
}
