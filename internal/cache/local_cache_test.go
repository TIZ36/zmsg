package cache

import (
	"testing"
	"time"
)

func init() {}

func TestLocalCacheEviction(t *testing.T) {
	c := newLocalCache(2, 0)

	c.Set("a", true)
	c.Set("b", true)
	c.Set("c", true)

	if _, ok := c.Get("a"); ok {
		t.Fatalf("expected 'a' to be evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatalf("expected 'b' to exist")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatalf("expected 'c' to exist")
	}
}

func TestLocalCacheTTL(t *testing.T) {
	c := newLocalCache(10, 10*time.Millisecond)

	c.Set("k", true)
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("expected key to exist before ttl")
	}

	time.Sleep(15 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatalf("expected key to expire after ttl")
	}
}
