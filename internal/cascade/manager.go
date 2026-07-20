package cascade

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
	"github.com/owlspeak/owl-sfu/internal/observability"
	"github.com/owlspeak/owl-sfu/internal/room"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// cascadeKeyframeMinInterval 屏幕轨 PLI 沿级联回传的每轨节流间隔（与 room 层
// keyframeMinInterval 对齐；每跳独立节流，多观看端请求逐跳合并）。
const cascadeKeyframeMinInterval = 300 * time.Millisecond

// Reporter 上报 EdgeStatus 到控制通道（由 control.Client 实现）。
type Reporter interface {
	ReportEdgeStatus(es *owlsfuv1.EdgeStatus)
}

// Config 为级联模块配置。
type Config struct {
	NodeID string
	// Listen 级联 mTLS 信令监听地址（15 BH：默认 tcp/8843，可配）。
	Listen string
	// Cert / CAPool 复用 enrollment 的节点证书与集群 CA（BG.2 mTLS 双向校验）。
	Cert   tls.Certificate
	CAPool *x509.CertPool
	// GetCert 可选的证书热更新回调（证书续期后返回新证书；nil 时恒用 Cert）。
	// 监听侧经 GetCertificate、拨号侧经 GetClientCertificate 逐次握手取用。
	GetCert func() *tls.Certificate
	// VerifyToken 级联 token 验签回调（生产接 auth.Verifier.VerifyCascade：
	// EdDSA 签名 + TTL + room/epoch/edge 绑定，BG.2 三重校验之三）。
	// nil 时退化为「与 Server 下发副本等值比较」（进程内测试路径）。
	VerifyToken func(token, roomID string, epoch uint64, parentNodeID, childNodeID string) error
}

// Manager 为级联总控：执行 SetAnchorLease/SetCascadeEdges，维护边会话、
// NodeWant 剪枝、环路防御与 EdgeStatus 上报。
type Manager struct {
	log     *slog.Logger
	cfg     Config
	engine  *owlsfu.Engine
	rooms   *room.Manager
	metrics *observability.Metrics

	repMu    sync.RWMutex
	reporter Reporter

	mu      sync.Mutex
	states  map[string]*roomState // logical_room_id → 状态
	dialing map[string]bool       // roomID+"|"+edgeKey → child 拨号协程存活

	ln       net.Listener
	closed   atomic.Bool
	stopCh   chan struct{}
	stopOnce sync.Once
}

// remoteSpeaker 为级联送入的远端轨状态（音频 / 屏幕 / 系统音频伴轨）。
type remoteSpeaker struct {
	key     string // trackKey = room.TrackKey(baseSID, kind)
	baseSID string
	uid     string
	kind    string
	ssrc    uint32 // 来向轨 SSRC（屏幕轨 PLI 回传用；onRemoteTrack 注册前写入）
	origin  *edgeSession
	// rp 本地房间的远端发布者句柄（房间未建时为 nil，recompute 补试）
	rp atomic.Pointer[room.RemotePublisher]
	// sinks 该轨需继续转发到的其它边出向轨快照（读循环无锁读）
	sinks atomic.Pointer[[]*outTrack]
	// keyReqAtMs 上次向源方向回传关键帧请求的时间（节流，防 PLI 风暴）
	keyReqAtMs atomic.Int64
}

// SetReporter 注入 EdgeStatus 上报器（control.Client 建成后调用）。
func (m *Manager) SetReporter(r Reporter) {
	m.repMu.Lock()
	m.reporter = r
	m.repMu.Unlock()
}

func (m *Manager) report(es *owlsfuv1.EdgeStatus) {
	m.repMu.RLock()
	r := m.reporter
	m.repMu.RUnlock()
	if r != nil {
		r.ReportEdgeStatus(es)
	}
}

// ensureStateLocked 取（或建）房间级联状态。
func (m *Manager) ensureStateLocked(roomID string) *roomState {
	rs, ok := m.states[roomID]
	if !ok {
		rs = newRoomState(roomID)
		m.states[roomID] = rs
	}
	return rs
}

// ---- 控制指令入口（control 包调用）----

// SetAnchorLease 应用 Anchor 租约（08 B.5）。
func (m *Manager) SetAnchorLease(roomID, anchorNodeID string, epoch uint64, expireUnixMs int64) error {
	m.mu.Lock()
	rs := m.ensureStateLocked(roomID)
	err := rs.applyLease(Lease{RoomID: roomID, AnchorNodeID: anchorNodeID, Epoch: epoch, ExpireUnixMs: expireUnixMs})
	m.mu.Unlock()
	if err != nil {
		return err
	}
	m.log.Info("anchor lease applied", "room", roomID, "anchor", anchorNodeID,
		"epoch", epoch, "expire_unix_ms", expireUnixMs)
	m.recompute(roomID)
	return nil
}

// SetCascadeEdges 全量替换某 epoch 边集（08 §4.2）：旧边拆除、新 child 边拨号。
func (m *Manager) SetCascadeEdges(roomID string, epoch uint64, protoEdges []*owlsfuv1.CascadeEdge) error {
	edges := make([]Edge, 0, len(protoEdges))
	for _, pe := range protoEdges {
		e := Edge{
			RoomID:         roomID,
			Epoch:          epoch,
			ParentNodeID:   pe.GetParentNodeId(),
			ChildNodeID:    pe.GetChildNodeId(),
			ParentEndpoint: pe.GetParentCascadeEndpoint(),
			Token:          pe.GetCascadeToken(),
		}
		// 只保留与本节点相关的边（Server 可能全量下发整图）
		if e.ParentNodeID != m.cfg.NodeID && e.ChildNodeID != m.cfg.NodeID {
			continue
		}
		if e.ParentNodeID == e.ChildNodeID {
			return fmt.Errorf("invalid edge: parent == child (%s)", e.ParentNodeID)
		}
		edges = append(edges, e)
	}

	m.mu.Lock()
	rs := m.ensureStateLocked(roomID)
	removed, err := rs.applyEdges(epoch, edges)
	if err != nil {
		m.mu.Unlock()
		return err
	}
	// child 边：确保拨号协程存活
	for key, e := range rs.edges {
		if e.ChildNodeID != m.cfg.NodeID {
			continue
		}
		dk := roomID + "|" + key
		if !m.dialing[dk] {
			m.dialing[dk] = true
			go m.childDialLoop(roomID, key)
		}
	}
	m.mu.Unlock()

	for _, s := range removed {
		s.close("edge set replaced (epoch " + fmt.Sprint(epoch) + ")")
	}
	m.log.Info("cascade edges applied", "room", roomID, "epoch", epoch, "edges", len(edges))
	m.recompute(roomID)
	return nil
}

// CloseRoom 回收房间级联状态（CloseLogicalRoom 时由 control 调用）。
func (m *Manager) CloseRoom(roomID string) {
	m.mu.Lock()
	rs, ok := m.states[roomID]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.states, roomID)
	sessions := make([]*edgeSession, 0, len(rs.sessions))
	for _, s := range rs.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.close("room closed")
	}
}

// ---- room.CascadeHooks 实现 ----

// OnPublishChanged 本地发布者上/下轨（音频/屏幕/伴轨）：维护 localSpeakers 并重算转发图。
func (m *Manager) OnPublishChanged(roomID, sid, uid, kind string, active bool) {
	key := room.TrackKey(sid, kind)
	m.mu.Lock()
	rs := m.ensureStateLocked(roomID)
	if active {
		rs.localSpeakers[key] = localPub{uid: uid, kind: kind}
	} else {
		delete(rs.localSpeakers, key)
	}
	m.mu.Unlock()
	m.recompute(roomID)
}

// OnDemandChanged 本地订阅需求变化：重算 NodeWant 并向 peer 通报（剪枝）。
func (m *Manager) OnDemandChanged(roomID string) {
	m.recompute(roomID)
}

// ---- 边事件 ----

// onPeerWant 对端 NodeWant 到达（音频与屏幕两套需求集合同帧携带）。
func (m *Manager) onPeerWant(s *edgeSession, audio, screen WantSet) {
	m.mu.Lock()
	s.peerWant = audio
	s.peerScreenWant = screen
	s.peerWantSeen = true
	m.mu.Unlock()
	m.recompute(s.edge.RoomID)
}

// onRemoteTrack 接收边上到达远端轨：环路防御 + kind 校验 + 注册 + 起读循环。
func (m *Manager) onRemoteTrack(s *edgeSession, track *webrtc.TrackRemote) {
	key, uid := track.ID(), track.StreamID()
	baseSID, kind := room.SplitTrackKey(key)
	roomID := s.edge.RoomID

	m.mu.Lock()
	rs := m.states[roomID]
	reject := classifyRemoteTrack(rs, s, key)
	// kind 与媒体类型一致性校验（防御：轨 id 约定被篡改/实现不一致）
	if reject == "" {
		isVideo := track.Kind() == webrtc.RTPCodecTypeVideo
		if isVideo != (kind == room.KindScreen) {
			reject = "kind_mismatch"
		}
	}
	var oldRP *room.RemotePublisher
	if reject == "" {
		if old, dup := rs.remoteSpeakers[key]; dup {
			// 同边 renegotiation 重挂：替换旧注册（旧读循环随轨结束自然退出）
			delete(rs.remoteSpeakers, key)
			oldRP = old.rp.Swap(nil)
		}
	}
	if reject != "" {
		m.mu.Unlock()
		m.log.Warn("cascade remote track rejected", "room", roomID, "key", key, "uid", uid, "reason", reject)
		go m.drainAndDrop(track)
		return
	}
	rsp := &remoteSpeaker{key: key, baseSID: baseSID, uid: uid, kind: kind,
		ssrc: uint32(track.SSRC()), origin: s}
	rs.remoteSpeakers[key] = rsp
	m.mu.Unlock()

	// 旧注册必须先注销（同 key 下行轨先摘后建）：若延后到新注册之后，
	// 旧 Close 会把刚为新轨补挂的下行轨一并摘除，导致重挂后转发中断。
	if oldRP != nil {
		oldRP.Close()
	}
	if err := m.registerRemotePub(roomID, rsp); err != nil {
		// 房间尚未 Ensure：先跨边转发，recompute 会补试本地注册
		m.log.Warn("remote publisher room attach deferred", "room", roomID, "key", key, "err", err)
	}
	m.log.Info("cascade remote track up", "room", roomID, "key", key, "uid", uid,
		"kind", kind, "from", s.peerNodeID())
	m.recompute(roomID)
	go m.remoteReadLoop(s, rsp, track)
}

// registerRemotePub 把远端轨注册进本地房间；屏幕轨挂关键帧回传钩子
// （本地观看端 PLI → requestUpstreamKeyframe → 沿级联回源节点）。
func (m *Manager) registerRemotePub(roomID string, rsp *remoteSpeaker) error {
	rp, err := m.rooms.AddRemotePublisher(roomID, rsp.baseSID, rsp.uid, rsp.kind)
	if err != nil {
		return err
	}
	if rsp.kind == room.KindScreen {
		rp.SetKeyframeRequester(func() { m.requestUpstreamKeyframe(rsp) })
		// 注册期间已挂上的本地观看端错过了 attach 时的关键帧请求（钩子彼时未注入），
		// 此处补发一次（经节流合并）。
		rp.RequestKeyframe()
	}
	rsp.rp.Store(rp)
	// 竞态兜底：注册期间该轨已结束（onRemoteSpeakerEnded 已摘除 rsp）→ 立即注销
	m.mu.Lock()
	rs := m.states[roomID]
	stale := rs == nil || rs.remoteSpeakers[rsp.key] != rsp
	m.mu.Unlock()
	if stale {
		if cur := rsp.rp.Swap(nil); cur != nil {
			cur.Close()
		}
	}
	return nil
}

// classifyRemoteTrack 环路/非预期来源判定（08 C.5，纯逻辑，单测锚点）：
// 返回非空 reason 表示该轨必须丢弃。key 为轨 trackKey（音频与屏幕/伴轨同一套判定）。
func classifyRemoteTrack(rs *roomState, s *edgeSession, key string) string {
	switch {
	case rs == nil || rs.sessions[s.edge.key()] != s || s.closed.Load():
		return "edge_not_current" // 非当前 epoch/边集的来源（非父/子预期来源）
	}
	if _, local := rs.localSpeakers[key]; local {
		return "speaker_is_local" // 本地轨被绕回来 = 环路
	}
	if old, dup := rs.remoteSpeakers[key]; dup && old.origin != s {
		return "duplicate_origin" // 同轨从第二来源到达 = 环路
	}
	return ""
}

// requestUpstreamKeyframe 向某远端屏幕轨的来向边回传 PLI（keyframeMinInterval 节流）。
// 逐跳传播：中继节点在出向轨 RTCP 读到 PLI 后经 onKeyframeRequest 再次到达这里，
// 最终在发布节点由 room.RequestScreenKeyframe 转发给发布客户端。
func (m *Manager) requestUpstreamKeyframe(rsp *remoteSpeaker) {
	now := time.Now().UnixMilli()
	last := rsp.keyReqAtMs.Load()
	if now-last < cascadeKeyframeMinInterval.Milliseconds() ||
		!rsp.keyReqAtMs.CompareAndSwap(last, now) {
		return
	}
	s := rsp.origin
	s.pcMu.Lock()
	pc := s.recvPC
	s.pcMu.Unlock()
	if pc == nil || s.closed.Load() {
		return
	}
	if err := pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: rsp.ssrc}}); err != nil {
		s.log.Debug("cascade upstream keyframe request failed", "key", rsp.key, "err", err)
	}
}

// onKeyframeRequest 出向屏幕轨收到对端 PLI/FIR：按源头路由——
// 本地发布者 → room 层转发发布客户端；远端轨 → 继续向来向边回传（多跳级联）。
func (m *Manager) onKeyframeRequest(roomID, key string) {
	m.mu.Lock()
	rs := m.states[roomID]
	var rsp *remoteSpeaker
	isLocal := false
	if rs != nil {
		if _, ok := rs.localSpeakers[key]; ok {
			isLocal = true
		} else {
			rsp = rs.remoteSpeakers[key]
		}
	}
	m.mu.Unlock()

	switch {
	case isLocal:
		sid, _ := room.SplitTrackKey(key)
		m.rooms.RequestScreenKeyframe(roomID, sid)
	case rsp != nil:
		m.requestUpstreamKeyframe(rsp)
	}
}

// drainAndDrop 持续读取被拒绝的轨并计入环路丢包指标（不读会阻塞对端拥塞控制）。
func (m *Manager) drainAndDrop(track *webrtc.TrackRemote) {
	buf := make([]byte, 1500)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
		m.metrics.CascadeLoopDropped.Inc()
	}
}

// remoteReadLoop 远端 speaker 读循环：写入本地房间 + 继续沿树转发到其它边。
func (m *Manager) remoteReadLoop(s *edgeSession, rsp *remoteSpeaker, track *webrtc.TrackRemote) {
	buf := make([]byte, 1500)
	var pkt rtp.Packet
	for {
		n, _, err := track.Read(buf)
		if err != nil {
			break
		}
		s.rxPackets.Inc()
		s.rxBytes.Add(float64(n))
		// 复用 pkt：清空上一包扩展防残留
		pkt.Header.Extensions = pkt.Header.Extensions[:0]
		pkt.Header.Extension = false
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		// 租约过期 / epoch 失配：停止跨节点转发（08 §10）
		if !s.active.Load() {
			continue
		}
		if rp := rsp.rp.Load(); rp != nil {
			rp.WriteRTP(&pkt, n)
		}
		if sinks := rsp.sinks.Load(); sinks != nil {
			for _, ot := range *sinks {
				_ = ot.sink.WriteRTP(&pkt)
			}
		}
	}
	m.onRemoteSpeakerEnded(rsp)
}

// onRemoteSpeakerEnded 远端轨结束（发布撤销/边断）。
func (m *Manager) onRemoteSpeakerEnded(rsp *remoteSpeaker) {
	m.mu.Lock()
	if rs := m.states[rsp.origin.edge.RoomID]; rs != nil && rs.remoteSpeakers[rsp.key] == rsp {
		delete(rs.remoteSpeakers, rsp.key)
	}
	m.mu.Unlock()
	if rp := rsp.rp.Swap(nil); rp != nil {
		rp.Close()
	}
	m.recompute(rsp.origin.edge.RoomID)
}

// onEdgeClosed 边会话关闭：清会话/该边源的远端 speaker/本地 sink，上报 EdgeDown。
func (m *Manager) onEdgeClosed(s *edgeSession) {
	roomID := s.edge.RoomID
	m.mu.Lock()
	rs := m.states[roomID]
	if rs != nil && rs.sessions[s.edge.key()] == s {
		delete(rs.sessions, s.edge.key())
	}
	var ended []*remoteSpeaker
	if rs != nil {
		for key, rsp := range rs.remoteSpeakers {
			if rsp.origin == s {
				delete(rs.remoteSpeakers, key)
				ended = append(ended, rsp)
			}
		}
	}
	for key, ot := range s.outTracks {
		if ot.detach != nil {
			ot.detach()
		}
		delete(s.outTracks, key)
	}
	m.mu.Unlock()

	for _, rsp := range ended {
		if rp := rsp.rp.Swap(nil); rp != nil {
			rp.Close()
		}
	}
	if s.established.CompareAndSwap(true, false) {
		m.metrics.CascadeEdges.Dec()
		m.metrics.CascadeEdgeDown.Inc()
		m.reportEdgeStatus(s.edge, owlsfuv1.EdgeStatus_STATE_EDGE_DOWN, s.RTT())
	}
	m.recompute(roomID)
}

// reportEdgeStatus 经控制通道上报边状态（08 §6.1）。
// loss_pct 首期恒为 0：纯转发链路无 RTCP 汇聚点，二期由 sender report 差分补齐。
func (m *Manager) reportEdgeStatus(e Edge, state owlsfuv1.EdgeStatus_State, rttMs float64) {
	m.report(&owlsfuv1.EdgeStatus{
		RoomId:       e.RoomID,
		Epoch:        e.Epoch,
		ParentNodeId: e.ParentNodeID,
		ChildNodeId:  e.ChildNodeID,
		State:        state,
		RttMs:        rttMs,
	})
}

// edgeEstablished 边握手完成（双侧各自调用）：登记会话并上报 EdgeUp。
// 返回 false 表示边已不在当前边集（拆除中），调用方应关闭会话。
func (m *Manager) edgeEstablished(s *edgeSession) bool {
	key := s.edge.key()
	m.mu.Lock()
	rs := m.states[s.edge.RoomID]
	cur, ok := rs.edgesLookup(key)
	if rs == nil || !ok || !sameEdge(cur, s.edge) || cur.Epoch != s.edge.Epoch {
		m.mu.Unlock()
		return false
	}
	// 同边旧会话让位（重连）：预先摘掉 established 标记，避免新边刚 Up 又上报 Down
	if old := rs.sessions[key]; old != nil && old != s {
		delete(rs.sessions, key)
		if old.established.CompareAndSwap(true, false) {
			m.metrics.CascadeEdges.Dec()
		}
		defer old.close("superseded by new connection")
	}
	rs.sessions[key] = s
	s.established.Store(true)
	m.mu.Unlock()

	m.metrics.CascadeEdges.Inc()
	m.metrics.CascadeEdgeUp.Inc()
	m.reportEdgeStatus(s.edge, owlsfuv1.EdgeStatus_STATE_EDGE_UP, 0)
	m.log.Info("cascade edge up", "room", s.edge.RoomID, "edge", key, "is_parent", s.isParent)
	m.recompute(s.edge.RoomID)
	return true
}

// edgesLookup 取当前边集中的边定义。
func (rs *roomState) edgesLookup(key string) (Edge, bool) {
	if rs == nil {
		return Edge{}, false
	}
	e, ok := rs.edges[key]
	return e, ok
}

// ---- 核心重算：出向轨 diff + NodeWant 通报 ----

// recompute 重算某房间的级联转发图：
//  1. 刷新每边转发授权（lease epoch 匹配 + 未过期，08 §3.1）
//  2. 按对端 NodeWant（音频/屏幕两套需求）对出向轨做增删（剪枝，08 §5.1 D.4）
//  3. 重算并向各 peer 通报我方 NodeWant（本地需求 ∪ 其它边需求）
//  4. 重建远端轨的跨边转发 sink 快照
func (m *Manager) recompute(roomID string) {
	now := time.Now()
	var wantSends []func()

	m.mu.Lock()
	rs := m.states[roomID]
	if rs == nil {
		m.mu.Unlock()
		return
	}
	allowed := rs.forwardingAllowed(now)
	if !allowed && rs.lastAllowed && rs.lease.Expired(now) {
		m.metrics.CascadeLeaseExpired.Inc()
		m.log.Warn("anchor lease expired, cross-node forwarding halted", "room", roomID)
	}
	rs.lastAllowed = allowed

	localWant, localScreenWant := WantNone(), WantNone()
	if want, except := m.rooms.LocalDemand(roomID); want {
		localWant = WantAllExcept(except)
	}
	if want, except := m.rooms.LocalScreenDemand(roomID); want {
		localScreenWant = WantAllExcept(except)
	}

	rs.activeTracks, rs.prunedTracks = 0, 0
	for _, s := range rs.sessions {
		est := s.established.Load()
		s.active.Store(allowed && est)
		if !est {
			continue
		}

		// 1) 期望出向轨集合（对端未通报 want 前不送任何轨）；
		//    屏幕轨与系统音频伴轨按 peerScreenWant 剪枝（未订阅不跨节点拉流）。
		type cand struct {
			uid, kind string
			local     bool
		}
		peerWants := func(kind, uid string) bool {
			if kind == room.KindAudio {
				return s.peerWant.Wants(uid)
			}
			return s.peerScreenWant.Wants(uid)
		}
		desired := make(map[string]cand)
		if allowed && s.peerWantSeen {
			for key, lp := range rs.localSpeakers {
				if peerWants(lp.kind, lp.uid) {
					desired[key] = cand{uid: lp.uid, kind: lp.kind, local: true}
				} else {
					rs.prunedTracks++
				}
			}
			for key, rsp := range rs.remoteSpeakers {
				if rsp.origin == s {
					continue // 不回送来源边（环路防御的发送侧）
				}
				if peerWants(rsp.kind, rsp.uid) {
					desired[key] = cand{uid: rsp.uid, kind: rsp.kind, local: false}
				} else {
					rs.prunedTracks++
				}
			}
		}
		needNeg := false
		for key := range s.outTracks {
			if _, keep := desired[key]; !keep {
				needNeg = s.removeOutTrackLocked(key) || needNeg
			}
		}
		for key, c := range desired {
			if _, ok := s.outTracks[key]; !ok {
				needNeg = s.addOutTrackLocked(key, c.uid, c.kind, c.local) || needNeg
			}
		}
		rs.activeTracks += len(s.outTracks)

		// 2) 我方 NodeWant = 本地需求 ∪ 其它边需求（08 §5.1 聚合；音频/屏幕独立聚合）
		myWant, myScreenWant := localWant, localScreenWant
		for _, o := range rs.sessions {
			if o != s && o.established.Load() && o.peerWantSeen {
				myWant = Union(myWant, o.peerWant)
				myScreenWant = Union(myScreenWant, o.peerScreenWant)
			}
		}
		if !s.myWantSent || !myWant.Equal(s.myWant) || !myScreenWant.Equal(s.myScreenWant) {
			s.myWant = myWant
			s.myScreenWant = myScreenWant
			s.myWantSent = true
			ww, sw := myWant.toWire(), myScreenWant.toWire()
			sess := s
			wantSends = append(wantSends, func() {
				if err := sess.send(&frame{T: frameWant, Want: &ww, ScreenWant: &sw}); err != nil {
					sess.close("send want: " + err.Error())
				}
			})
		}
		if needNeg {
			s.negotiateSend()
		}
	}

	// 3) 远端轨的跨边 sink 快照 + 房间注册补试
	for key, rsp := range rs.remoteSpeakers {
		var sinks []*outTrack
		for _, s := range rs.sessions {
			if s == rsp.origin {
				continue
			}
			if ot := s.outTracks[key]; ot != nil {
				sinks = append(sinks, ot)
			}
		}
		rsp.sinks.Store(&sinks)
	}
	pending := make([]*remoteSpeaker, 0)
	for _, rsp := range rs.remoteSpeakers {
		if rsp.rp.Load() == nil {
			pending = append(pending, rsp)
		}
	}

	// 4) 剪枝率指标（全节点聚合）
	var act, pruned int
	for _, st := range m.states {
		act += st.activeTracks
		pruned += st.prunedTracks
	}
	m.mu.Unlock()

	// 房间注册补试在锁外执行（AddRemotePublisher 会广播/协商，避免长持锁）
	for _, rsp := range pending {
		_ = m.registerRemotePub(roomID, rsp)
	}
	m.metrics.CascadeOutboundTracks.WithLabelValues("active").Set(float64(act))
	m.metrics.CascadeOutboundTracks.WithLabelValues("pruned").Set(float64(pruned))
	for _, fn := range wantSends {
		fn()
	}
}

// watchdog 周期任务：租约过期检测触发 recompute + 周期性 EdgeUp(RTT) 刷新上报。
func (m *Manager) watchdog() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastReport := time.Now()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-tick.C:
			var dirty []string
			var reports []*edgeSession
			m.mu.Lock()
			for roomID, rs := range m.states {
				if rs.forwardingAllowed(now) != rs.lastAllowed {
					dirty = append(dirty, roomID)
				}
				if now.Sub(lastReport) >= 15*time.Second {
					for _, s := range rs.sessions {
						if s.established.Load() {
							reports = append(reports, s)
						}
					}
				}
			}
			m.mu.Unlock()
			if now.Sub(lastReport) >= 15*time.Second {
				lastReport = now
			}
			for _, roomID := range dirty {
				m.recompute(roomID)
			}
			for _, s := range reports {
				m.reportEdgeStatus(s.edge, owlsfuv1.EdgeStatus_STATE_EDGE_UP, s.RTT())
			}
		}
	}
}
