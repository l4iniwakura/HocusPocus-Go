// Package redis implements the Hocuspocus Redis pub/sub framing and storage lock.
package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	redisclient "github.com/redis/go-redis/v9"
	"log/slog"
	"sync"
	"time"
)

type room struct {
	doc      *hp.Document
	sub      *redisclient.PubSub
	caughtUp chan struct{}
	once     sync.Once
	cancel   context.CancelFunc
	publish  chan struct{}
}
type Redis struct {
	Client                                                redisclient.UniversalClient
	Prefix, Identifier                                    string
	LockTimeout, DisconnectDelay, AwaitInitialSyncTimeout time.Duration
	mu                                                    sync.Mutex
	rooms                                                 map[string]*room
}

func (*Redis) Priority() int { return 1000 }
func (*Redis) Handles(h hp.Hook) bool {
	return h == hp.OnConfigure || h == hp.AfterLoadDocument || h == hp.OnChange || h == hp.OnAwarenessUpdate || h == hp.BeforeBroadcastStateless || h == hp.OnStoreDocument || h == hp.BeforeUnloadDocument || h == hp.AfterUnloadDocument || h == hp.OnDestroy
}
func (r *Redis) key(name string) string    { return r.Prefix + ":" + name }
func (r *Redis) replyKey(id string) string { return r.Prefix + "#reply:" + id }
func randomID() string {
	var bytes [16]byte
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}
func (r *Redis) encode(data []byte) []byte {
	out := []byte{byte(len(r.Identifier))}
	out = append(out, r.Identifier...)
	return append(out, data...)
}
func (r *Redis) publish(ctx context.Context, channel string, data []byte) error {
	return r.Client.Publish(ctx, channel, r.encode(data)).Err()
}
func (r *Redis) Handle(ctx context.Context, h hp.Hook, p *hp.Payload) error {
	switch h {
	case hp.OnConfigure:
		if r.Client == nil {
			return errors.New("Redis client required")
		}
		if r.Prefix == "" {
			r.Prefix = "hocuspocus"
		}
		if r.Identifier == "" {
			r.Identifier = "host-" + randomID()
		}
		if len(r.Identifier) > 255 {
			return errors.New("Redis identifier exceeds one-byte prefix")
		}
		if r.LockTimeout <= 0 {
			r.LockTimeout = time.Second
		}
		if r.AwaitInitialSyncTimeout <= 0 {
			r.AwaitInitialSyncTimeout = time.Second
		}
		r.rooms = map[string]*room{}
		return r.Client.Ping(ctx).Err()
	case hp.AfterLoadDocument:
		return r.load(ctx, p.Document)
	case hp.OnChange:
		if p.Origin.Source == "redis" {
			return nil
		}
		r.mu.Lock()
		rr := r.rooms[p.DocumentName]
		r.mu.Unlock()
		if rr != nil && rr.doc == p.Document {
			select {
			case rr.publish <- struct{}{}:
			default:
			}
		}
		return nil
	case hp.OnAwarenessUpdate:
		if p.Origin.Source == "redis" {
			return nil
		}
		return r.publish(ctx, r.key(p.DocumentName), p.Document.AwarenessRelay(p.States, p.RemovedClients))
	case hp.BeforeBroadcastStateless:
		return r.publish(ctx, r.key(p.DocumentName), p.Document.StatelessRelay(p.Message))
	case hp.OnStoreDocument:
		key := r.key(p.DocumentName) + ":lock"
		token := randomID()
		ok, err := r.Client.SetNX(ctx, key, token, r.LockTimeout).Result()
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("another instance is storing the document; retry")
		}
		p.Defer(func() {
			release, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = r.Client.Eval(release, "if redis.call('get',KEYS[1]) == ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end", []string{key}, token).Err()
		})
		return nil
	case hp.BeforeUnloadDocument:
		if r.DisconnectDelay > 0 {
			timer := time.NewTimer(r.DisconnectDelay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	case hp.AfterUnloadDocument:
		r.mu.Lock()
		rr := r.rooms[p.DocumentName]
		if rr != nil && rr.doc == p.Document {
			delete(r.rooms, p.DocumentName)
			rr.cancel()
			_ = rr.sub.Close()
		}
		r.mu.Unlock()
	case hp.OnDestroy:
		r.mu.Lock()
		for _, rr := range r.rooms {
			rr.cancel()
			_ = rr.sub.Close()
		}
		r.rooms = map[string]*room{}
		r.mu.Unlock()
	}
	return nil
}
func (r *Redis) load(ctx context.Context, doc *hp.Document) error {
	sub := r.Client.Subscribe(ctx, r.key(doc.Name), r.replyKey(r.Identifier))
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return err
	}
	life, cancel := context.WithCancel(context.Background())
	rr := &room{doc: doc, sub: sub, caughtUp: make(chan struct{}), cancel: cancel, publish: make(chan struct{}, 1)}
	r.mu.Lock()
	if old := r.rooms[doc.Name]; old != nil {
		old.cancel()
		_ = old.sub.Close()
	}
	r.rooms[doc.Name] = rr
	r.mu.Unlock()
	go func() {
		for {
			select {
			case <-life.Done():
				return
			case <-rr.publish:
				if err := r.publish(life, r.key(doc.Name), doc.SyncRequest()); err != nil && life.Err() == nil {
					slog.Error("Redis sync publish failed", "document", doc.Name, "error", err)
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-life.Done():
				return
			case msg, ok := <-sub.Channel():
				if !ok {
					return
				}
				data := []byte(msg.Payload)
				if len(data) < 1 {
					continue
				}
				n := int(data[0])
				if len(data) < n+1 {
					continue
				}
				id := string(data[1 : n+1])
				if id == r.Identifier {
					continue
				}
				state, err := doc.ReceiveRelay(life, data[n+1:], func(reply []byte) error { return r.publish(life, r.replyKey(id), reply) })
				if err == nil && state && msg.Channel == r.replyKey(r.Identifier) {
					rr.once.Do(func() { close(rr.caughtUp) })
				}
			}
		}
	}()
	failed := true
	defer func() {
		if failed {
			cancel()
			_ = sub.Close()
			r.mu.Lock()
			if r.rooms[doc.Name] == rr {
				delete(r.rooms, doc.Name)
			}
			r.mu.Unlock()
		}
	}()
	counts, err := r.Client.PubSubNumSub(ctx, r.key(doc.Name)).Result()
	if err != nil {
		return fmt.Errorf("Redis subscriber count: %w", err)
	}
	if err = r.publish(ctx, r.key(doc.Name), doc.SyncRequest()); err != nil {
		return err
	}
	if err = r.publish(ctx, r.key(doc.Name), doc.AwarenessRequest()); err != nil {
		return err
	}
	if counts[r.key(doc.Name)] > 1 {
		timer := time.NewTimer(r.AwaitInitialSyncTimeout)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rr.caughtUp:
		case <-timer.C:
		}
	}
	failed = false
	return nil
}
