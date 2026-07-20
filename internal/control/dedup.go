package control

import (
	"sync"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
)

// ackCache 为 command_id 幂等去重窗口：
// 保留最近 N 条指令的 Ack，重复 command_id 直接重发上次结果。
type ackCache struct {
	mu    sync.Mutex
	limit int
	m     map[string]*owlsfuv1.CommandAck
	order []string // FIFO 淘汰队列
}

func newAckCache(limit int) *ackCache {
	return &ackCache{
		limit: limit,
		m:     make(map[string]*owlsfuv1.CommandAck, limit),
	}
}

// get 返回缓存的 Ack（命中即为重放指令）。
func (c *ackCache) get(commandID string) (*owlsfuv1.CommandAck, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ack, ok := c.m[commandID]
	return ack, ok
}

// put 记录指令处理结果，超窗淘汰最旧条目。
func (c *ackCache) put(ack *owlsfuv1.CommandAck) {
	if ack.GetCommandId() == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.m[ack.GetCommandId()]; !exists {
		c.order = append(c.order, ack.GetCommandId())
		for len(c.order) > c.limit {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.m, oldest)
		}
	}
	c.m[ack.GetCommandId()] = ack
}
