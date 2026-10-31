package throttle

import (
	"context"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRollingWindowAndBan(t *testing.T) {
	limiter := New()
	limiter.Limit = 2
	limiter.Window = time.Second
	limiter.BanTime = time.Minute
	request := httptest.NewRequest("GET", "http://localhost", nil)
	request.RemoteAddr = "127.0.0.1:1234"
	p := &hp.Payload{Request: request}
	for range 2 {
		if err := limiter.Handle(context.Background(), hp.OnConnect, p); err != nil {
			t.Fatal(err)
		}
	}
	if err := limiter.Handle(context.Background(), hp.OnConnect, p); err != hp.ErrForbidden {
		t.Fatal(err)
	}
	request.RemoteAddr = "127.0.0.2:1234"
	if err := limiter.Handle(context.Background(), hp.OnConnect, p); err != nil {
		t.Fatal(err)
	}
}
