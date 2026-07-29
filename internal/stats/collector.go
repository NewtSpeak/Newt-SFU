// Package stats 采集节点容量与负载，供控制通道心跳上报。
package stats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

// Collector 聚合 users/rooms/CPU/内存/出口带宽。
type Collector struct {
	maxUsers uint32

	// countsFn / screenFn 由 room.Manager 注入，返回当前 (users, rooms) / 屏幕轨数。
	countsMu sync.RWMutex
	countsFn func() (users, rooms int)
	screenFn func() int

	forwardedBytes atomic.Uint64

	rateMu    sync.Mutex
	lastBytes uint64
	lastAt    time.Time
	lastMbps  float64
}

// NewCollector 创建采集器。
func NewCollector(maxUsers int) *Collector {
	return &Collector{maxUsers: uint32(maxUsers), lastAt: time.Now()}
}

// SetCountsFunc 注入实时 users/rooms 计数来源。
func (c *Collector) SetCountsFunc(fn func() (int, int)) {
	c.countsMu.Lock()
	c.countsFn = fn
	c.countsMu.Unlock()
}

// SetScreenTracksFunc 注入实时屏幕轨计数来源（心跳 screen_tracks，docs 14 BD.3）。
func (c *Collector) SetScreenTracksFunc(fn func() int) {
	c.countsMu.Lock()
	c.screenFn = fn
	c.countsMu.Unlock()
}

// AddForwardedBytes 累计转发字节（RTP 转发路径调用）。
func (c *Collector) AddForwardedBytes(n int) {
	c.forwardedBytes.Add(uint64(n))
}

// Capacity 生成一次心跳容量快照。
func (c *Collector) Capacity() *owlsfuv1.NodeCapacity {
	users, rooms, screens := 0, 0, 0
	c.countsMu.RLock()
	if c.countsFn != nil {
		users, rooms = c.countsFn()
	}
	if c.screenFn != nil {
		screens = c.screenFn()
	}
	c.countsMu.RUnlock()

	cap := &owlsfuv1.NodeCapacity{
		MaxUsers:         c.maxUsers,
		CurrentUsers:     uint32(users),
		RoomCount:        uint32(rooms),
		ScreenTracks:     uint32(screens),
		BandwidthOutMbps: c.outMbps(),
	}
	// cpu.Percent(0,...) 返回自上次调用以来的均值，非阻塞；首次调用为 0。
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cap.CpuPct = pcts[0]
	}
	if vm, err := mem.VirtualMemory(); err == nil {
		cap.MemPct = vm.UsedPercent
	}
	return cap
}

// outMbps 用转发字节计数器差分估算出口带宽。
func (c *Collector) outMbps() float64 {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()
	now := time.Now()
	cur := c.forwardedBytes.Load()
	dt := now.Sub(c.lastAt).Seconds()
	if dt >= 1 {
		c.lastMbps = float64(cur-c.lastBytes) * 8 / dt / 1e6
		c.lastBytes = cur
		c.lastAt = now
	}
	return c.lastMbps
}
