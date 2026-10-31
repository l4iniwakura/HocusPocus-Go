//go:build integration

package redis

import (
	"context"
	"os"
	"testing"
	"time"

	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
	"github.com/l4iniwakura/HocusPocus-Go/extensions/database"
	client "github.com/redis/go-redis/v9"
)

func TestRealRedisCrossNodeAndWarmLoad(t *testing.T) {
	address := os.Getenv("TEST_REDIS_ADDR")
	if address == "" {
		t.Fatal("TEST_REDIS_ADDR is required for integration tests")
	}
	ctx := context.Background()
	storage := database.NewMemory()
	prefix := "hocuspocus-go-test-" + randomID()
	servers := []*hp.Server{}
	for range 3 {
		redisClient := client.NewClient(&client.Options{Addr: address})
		t.Cleanup(func() { redisClient.Close() })
		cfg := hp.DefaultConfiguration()
		cfg.Debounce = time.Hour
		cfg.MaxDebounce = time.Hour
		cfg.Extensions = []hp.Extension{&Redis{Client: redisClient, Prefix: prefix}, &database.Database{Storage: storage}}
		s, err := hp.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		servers = append(servers, s)
		t.Cleanup(func() {
			shutdown, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := s.Shutdown(shutdown); err != nil {
				t.Error(err)
			}
		})
	}
	a, err := servers[0].OpenDirect(ctx, "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Transact(ctx, func(doc *crdt.Doc) error {
		doc.Transact(func(tx *crdt.Transaction) { tx.GetText("body").Insert(tx, 0, "first", nil) })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	b, err := servers[1].OpenDirect(ctx, "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := servers[1].Documents()[0].Doc.GetText("body").ToString(); got != "first" {
		t.Fatalf("warm load missed unpersisted peer state: %q", got)
	}
	if err = b.Transact(ctx, func(doc *crdt.Doc) error {
		doc.Transact(func(tx *crdt.Transaction) { tx.GetText("body").Insert(tx, 0, "second ", nil) })
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for servers[0].Documents()[0].Doc.GetText("body").ToString() != "second first" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := servers[0].Documents()[0].Doc.GetText("body").ToString(); got != "second first" {
		t.Fatal(got)
	}
	c, err := servers[2].OpenDirect(ctx, "note", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := servers[2].Documents()[0].Doc.GetText("body").ToString(); got != "second first" {
		t.Fatal(got)
	}
	for _, connection := range []*hp.DirectConnection{a, b, c} {
		if err = connection.Disconnect(ctx, true); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := storage.Fetch(ctx, "note")
	if err != nil {
		t.Fatal(err)
	}
	doc := crdt.New()
	if err = doc.ApplyUpdate(stored); err != nil {
		t.Fatal(err)
	}
	if got := doc.GetText("body").ToString(); got != "second first" {
		t.Fatal(got)
	}
}
