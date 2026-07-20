package signal

import (
	"sync"
	"time"
)

// ipRateLimiter 为简化版每 IP 令牌桶（/rtt 限速用）。
type ipRateLimiter struct {
	mu      sync.Mutex
	perSec  int
	buckets map[string]*bucket
}

type bucket struct {
	tokens  int
	resetAt time.Time
}

func newIPRateLimiter(perSec int) *ipRateLimiter {
	return &ipRateLimiter{perSec: perSec, buckets: make(map[string]*bucket)}
}

// allow 判断该 IP 本秒内是否还有配额；顺带清理过期桶防内存膨胀。
func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	b, ok := l.buckets[ip]
	if !ok || now.After(b.resetAt) {
		if len(l.buckets) > 65536 {
			l.buckets = make(map[string]*bucket)
		}
		l.buckets[ip] = &bucket{tokens: l.perSec - 1, resetAt: now.Add(time.Second)}
		return true
	}
	if b.tokens <= 0 {
		return false
	}
	b.tokens--
	return true
}
