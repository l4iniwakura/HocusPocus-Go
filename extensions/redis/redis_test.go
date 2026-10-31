package redis

import (
	"context"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"testing"
)

func TestMissingClientRejected(t *testing.T) {
	cfg := hp.DefaultConfiguration()
	cfg.Extensions = []hp.Extension{&Redis{}}
	if _, err := hp.New(cfg); err == nil {
		t.Fatal("missing Redis client accepted")
	}
}
func TestIdentifierFraming(t *testing.T) {
	r := &Redis{Identifier: "node-a"}
	got := r.encode([]byte{1, 2})
	if len(got) != 9 || got[0] != 6 || string(got[1:7]) != "node-a" || got[7] != 1 {
		t.Fatal(got)
	}
	if err := (&Redis{}).Handle(context.Background(), hp.OnChange, &hp.Payload{Origin: hp.Origin{Source: "redis"}}); err != nil {
		t.Fatal(err)
	}
}
