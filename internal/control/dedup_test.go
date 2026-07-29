package control

import (
	"fmt"
	"testing"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

func TestAckCacheIdempotency(t *testing.T) {
	c := newAckCache(1024)

	if _, ok := c.get("cmd-1"); ok {
		t.Fatal("empty cache should not hit")
	}

	ack1 := &owlsfuv1.CommandAck{CommandId: "cmd-1", Ok: true}
	c.put(ack1)

	got, ok := c.get("cmd-1")
	if !ok {
		t.Fatal("expected cache hit for cmd-1")
	}
	if got != ack1 {
		t.Fatal("expected the exact cached ack to be replayed")
	}

	// 同 command_id 覆盖不重复入队
	ack1b := &owlsfuv1.CommandAck{CommandId: "cmd-1", Ok: false, ErrorCode: "X"}
	c.put(ack1b)
	if got, _ := c.get("cmd-1"); got != ack1b {
		t.Fatal("expected overwrite for same command_id")
	}
	if len(c.order) != 1 {
		t.Fatalf("expected order queue length 1, got %d", len(c.order))
	}
}

func TestAckCacheEviction(t *testing.T) {
	const limit = 1024
	c := newAckCache(limit)

	for i := 0; i < limit+10; i++ {
		c.put(&owlsfuv1.CommandAck{CommandId: fmt.Sprintf("cmd-%d", i), Ok: true})
	}

	// 最旧的 10 条被淘汰
	for i := 0; i < 10; i++ {
		if _, ok := c.get(fmt.Sprintf("cmd-%d", i)); ok {
			t.Fatalf("cmd-%d should have been evicted", i)
		}
	}
	// 窗口内的仍在
	for i := 10; i < limit+10; i++ {
		if _, ok := c.get(fmt.Sprintf("cmd-%d", i)); !ok {
			t.Fatalf("cmd-%d should still be cached", i)
		}
	}
	if len(c.m) != limit || len(c.order) != limit {
		t.Fatalf("cache size exceeded limit: m=%d order=%d", len(c.m), len(c.order))
	}
}

func TestAckCacheIgnoresEmptyID(t *testing.T) {
	c := newAckCache(4)
	c.put(&owlsfuv1.CommandAck{CommandId: "", Ok: true})
	if len(c.m) != 0 {
		t.Fatal("empty command_id should not be cached")
	}
}
