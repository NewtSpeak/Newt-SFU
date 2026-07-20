package cascade

import (
	"fmt"
	"time"
)

// Lease 为逻辑房的 Anchor 租约（08 §3.1）：Server 权威下发，
// SFU 仅在 epoch 匹配且未过期时执行跨节点转发。
type Lease struct {
	RoomID       string
	AnchorNodeID string
	Epoch        uint64
	ExpireUnixMs int64
}

// Expired 判断租约在 now 是否已过期。
func (l *Lease) Expired(now time.Time) bool {
	return l == nil || now.UnixMilli() >= l.ExpireUnixMs
}

// Edge 为某 epoch 边集中与本节点相关的一条边（08 §4.2）。
type Edge struct {
	RoomID         string
	Epoch          uint64
	ParentNodeID   string
	ChildNodeID    string
	ParentEndpoint string // child 主动连的 parent 级联端口 host:port
	Token          string // Server 签发的 cascade token（可为空，见 server.go 校验说明）
}

// key 唯一标识一条边（同 epoch 内 parent+child 唯一）。
func (e Edge) key() string { return e.ParentNodeID + "|" + e.ChildNodeID }

// localPub 为本地在播的一条轨（音频 / 屏幕 / 系统音频伴轨）。
type localPub struct {
	uid  string
	kind string // room.KindAudio / KindScreen / KindScreenAudio
}

// roomState 为单个逻辑房的级联状态（由 Manager.mu 保护）。
type roomState struct {
	roomID string
	lease  *Lease
	epoch  uint64          // 当前边集 epoch
	edges  map[string]Edge // key() → 边定义（仅与本节点相关的边）
	// sessions 当前边会话（已建立或建立中）；键 = Edge.key()
	sessions map[string]*edgeSession
	// localSpeakers 本地在播轨：trackKey（room.TrackKey(sid, kind)）→ 发布者
	localSpeakers map[string]localPub
	// remoteSpeakers 级联送入的远端轨：源 trackKey → 状态
	remoteSpeakers map[string]*remoteSpeaker

	// lastAllowed 上次 recompute 时的转发授权（租约过期沿检测）
	lastAllowed bool
	// activeTracks / prunedTracks 出向轨活跃/被剪数（剪枝率指标输入）
	activeTracks, prunedTracks int
}

func newRoomState(roomID string) *roomState {
	return &roomState{
		roomID:         roomID,
		edges:          make(map[string]Edge),
		sessions:       make(map[string]*edgeSession),
		localSpeakers:  make(map[string]localPub),
		remoteSpeakers: make(map[string]*remoteSpeaker),
	}
}

// forwardingAllowed 判断跨节点转发是否被授权（08 §3.1：epoch 匹配 + 租约未过期）。
func (rs *roomState) forwardingAllowed(now time.Time) bool {
	return rs.lease != nil && rs.lease.Epoch == rs.epoch && !rs.lease.Expired(now)
}

// applyLease 应用租约；epoch 只增不减（旧 epoch 租约视为过期指令）。
func (rs *roomState) applyLease(l Lease) error {
	if rs.lease != nil && l.Epoch < rs.lease.Epoch {
		return fmt.Errorf("stale lease epoch %d < current %d", l.Epoch, rs.lease.Epoch)
	}
	rs.lease = &l
	return nil
}

// applyEdges 全量替换边集（08 §4.2：替换图必须整体切 epoch；同 epoch 重发幂等）。
// 返回需要拆除的旧边会话与新的边集；epoch 回退视为过期指令。
func (rs *roomState) applyEdges(epoch uint64, edges []Edge) (removed []*edgeSession, err error) {
	if epoch < rs.epoch {
		return nil, fmt.Errorf("stale edges epoch %d < current %d", epoch, rs.epoch)
	}
	next := make(map[string]Edge, len(edges))
	for _, e := range edges {
		e.RoomID = rs.roomID
		e.Epoch = epoch
		next[e.key()] = e
	}
	// 不在新边集（或参数变化）的会话拆除；其余保留（幂等重发不断边）
	for k, s := range rs.sessions {
		ne, keep := next[k]
		if !keep || !sameEdge(s.edge, ne) {
			removed = append(removed, s)
			delete(rs.sessions, k)
		} else {
			s.edge = ne // 刷新 epoch/token
		}
	}
	rs.epoch = epoch
	rs.edges = next
	return removed, nil
}

// sameEdge 判断边定义是否等价（endpoint 变化需要重建连接；token 轮换不断边）。
func sameEdge(a, b Edge) bool {
	return a.ParentNodeID == b.ParentNodeID &&
		a.ChildNodeID == b.ChildNodeID &&
		a.ParentEndpoint == b.ParentEndpoint
}
