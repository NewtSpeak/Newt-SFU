// cascade_api.go 提供级联（internal/cascade）所需的房间侧接口：
//   - CascadeHooks：本地发布/订阅需求变化的事件回调（room → cascade）
//   - RemotePublisher：级联送入的远端轨（音频/屏幕/伴轨），向本地订阅者 fanout
//   - AttachCascadeSink：把本地发布者 RTP 旁路注入级联上行轨（零额外协程）
//   - LocalDemand / LocalScreenDemand：本地订阅需求聚合（NodeWant 输入，08 §5.1）
//
// 约定：room 内部绝不在持有 Manager.mu 时调用 hooks（防止与 cascade 锁互等）。
package room

import (
	"fmt"
	"sync/atomic"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/owlspeak/owl-sfu/internal/auth"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// CascadeHooks 由 internal/cascade 实现；room 在关键事件后（锁外）回调。
type CascadeHooks interface {
	// OnPublishChanged 本地发布者上/下轨（kind = KindAudio/KindScreen/KindScreenAudio；
	// active = 有上行轨且持对应发布 cap）。
	OnPublishChanged(roomID, sid, uid, kind string, active bool)
	// OnDemandChanged 本地订阅需求可能变化（成员增删/退订/重订/caps 变化）。
	OnDemandChanged(roomID string)
}

// SetCascade 注入级联钩子（nil 安全：未注入时全部为 no-op）。
func (m *Manager) SetCascade(h CascadeHooks) { m.cascade = h }

// cascadePublish 锁外调用发布变化钩子。
func (m *Manager) cascadePublish(roomID, sid, uid, kind string, active bool) {
	if m.cascade != nil {
		m.cascade.OnPublishChanged(roomID, sid, uid, kind, active)
	}
}

// cascadeDemand 锁外调用需求变化钩子。
func (m *Manager) cascadeDemand(roomID string) {
	if m.cascade != nil {
		m.cascade.OnDemandChanged(roomID)
	}
}

// 远端轨的等效 caps。授权已在源节点执行（08 §5.2：级联层只转发已允许的 track），
// 本地按持有对应发布 cap 对待。
var (
	remotePubCaps    = auth.NewCapSet([]string{auth.CapPublishAudio})
	remoteScreenCaps = auth.NewCapSet([]string{auth.CapPublishScreen})
)

// RemotePublisher 表示级联送入的一条远端轨（非本节点会话）：
// kind ∈ {audio, screen, screen_audio}，级联层读到 RTP 后经 WriteRTP 分发给本地订阅者。
type RemotePublisher struct {
	room *Room
	sid  string // 源会话 sid（不含 kind 后缀）
	uid  string
	kind string

	fanout atomic.Pointer[[]*downTrack]
	closed atomic.Bool
	// keyReq 屏幕轨关键帧回传钩子（cascade 注入：本地观看端 PLI → 沿级联回源节点）
	keyReq atomic.Pointer[func()]
}

// SID 返回源会话 ID。
func (rp *RemotePublisher) SID() string { return rp.sid }

// UID 返回源用户 ID。
func (rp *RemotePublisher) UID() string { return rp.uid }

// Kind 返回轨道类型。
func (rp *RemotePublisher) Kind() string { return rp.kind }

// trackKey 返回该远端轨的下行轨 id / 索引键。
func (rp *RemotePublisher) trackKey() string { return TrackKey(rp.sid, rp.kind) }

// SetKeyframeRequester 注入关键帧回传钩子（仅 kind=screen 有意义；并发安全）。
func (rp *RemotePublisher) SetKeyframeRequester(f func()) { rp.keyReq.Store(&f) }

// RequestKeyframe 触发关键帧回传（新观看端就位 / 收到 PLI 时调用；节流在级联层）。
func (rp *RemotePublisher) RequestKeyframe() {
	if f := rp.keyReq.Load(); f != nil {
		(*f)()
	}
}

// AddRemotePublisher 注册远端轨：广播 track_published（kind 透传）并给本地订阅者挂下行轨。
// 房间必须已存在（级联边只对已 Ensure 的逻辑房生效）；同轨（sid+kind）重复注册报错。
func (m *Manager) AddRemotePublisher(roomID, sid, uid, kind string) (*RemotePublisher, error) {
	switch kind {
	case KindAudio, KindScreen, KindScreenAudio:
	default:
		return nil, fmt.Errorf("unknown remote publisher kind %q", kind)
	}
	rp := &RemotePublisher{sid: sid, uid: uid, kind: kind}
	key := rp.trackKey()
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("room %s not found", roomID)
	}
	if _, dup := r.remotePubs[key]; dup {
		m.mu.Unlock()
		return nil, fmt.Errorf("remote publisher %s already registered", key)
	}
	rp.room = r
	r.remotePubs[key] = rp
	m.mu.Unlock()

	r.broadcast(nil, "track_published", map[string]any{"user_id": uid, "kind": kind})
	for _, sub := range r.snapshotParts() {
		r.ensureRemoteDownTrack(rp, sub)
	}
	r.rebuildRemoteFanout(rp)
	return rp, nil
}

// WriteRTP 把远端轨的一包写入本地全部活跃下行轨（级联读循环调用）。
func (rp *RemotePublisher) WriteRTP(pkt *rtp.Packet, n int) {
	if rp.closed.Load() {
		return
	}
	fanout := rp.fanout.Load()
	if fanout == nil || len(*fanout) == 0 {
		return
	}
	for _, dt := range *fanout {
		// 单个订阅者写失败（如 PC 关闭中）不影响其余
		_ = dt.track.WriteRTP(pkt)
	}
	cnt := len(*fanout)
	r := rp.room
	r.mgr.metrics.RTPForwardedPackets.Add(float64(cnt))
	r.mgr.metrics.RTPForwardedBytes.Add(float64(n * cnt))
	r.mgr.stats.AddForwardedBytes(n * cnt)
}

// Close 注销远端轨（幂等）：广播 track_ended（kind 透传）并停止转发。
// 下行轨不摘除、留待复用：级联剪枝→恢复（静音/重订、边闪断重建）会以同一
// trackKey 快速重挂，新注册直接接管既有下行轨即可零协商恢复（与本地订阅
// 「退订轨保留」语义一致），也规避了同 id 轨快速 RemoveTrack/AddTrack 的
// 重协商竞态；残留的静默 m-line 随订阅者会话结束回收。
func (rp *RemotePublisher) Close() {
	if !rp.closed.CompareAndSwap(false, true) {
		return
	}
	r := rp.room
	key := rp.trackKey()
	r.mgr.mu.Lock()
	if r.remotePubs[key] == rp {
		delete(r.remotePubs, key)
	}
	r.mgr.mu.Unlock()
	rp.fanout.Store(nil)

	r.broadcast(nil, "track_ended", map[string]any{"user_id": rp.uid, "kind": rp.kind})
}

// remoteShouldForward 远端轨的转发决策：音频同本地音频语义，屏幕/伴轨同屏幕语义。
func (rp *RemotePublisher) remoteShouldForward(sub *Participant, unsub bool) bool {
	if rp.kind == KindAudio {
		return ShouldForward(remotePubCaps, sub.Caps(), unsub)
	}
	return ShouldForwardScreen(remoteScreenCaps, sub.Caps(), unsub)
}

// ensureRemoteDownTrack 给 sub 补挂来自远端轨的下行轨（与本地 ensure*DownTrack 对称）。
func (r *Room) ensureRemoteDownTrack(rp *RemotePublisher, sub *Participant) {
	if rp.closed.Load() || sub.closed.Load() {
		return
	}
	if !rp.remoteShouldForward(sub, sub.isUnsubscribed(rp.uid, rp.kind)) {
		return
	}
	key := rp.trackKey()
	codec := owlsfu.OpusCodec
	if rp.kind == KindScreen {
		codec = owlsfu.VP8Codec
	}
	sub.mu.Lock()
	tracks := sub.downTracks
	if rp.kind != KindAudio {
		tracks = sub.screenDown
	}
	if dt, ok := tracks[key]; ok {
		// 同 key 既有下行轨（同名远端轨重挂：剪枝恢复/边重建/renegotiation）：
		// 新注册直接接管，零协商恢复转发（轨保留语义，见 RemotePublisher.Close）。
		dt.owner = rp
		dt.active = !sub.hasUnsubscribedLocked(rp.uid, rp.kind)
		sub.mu.Unlock()
		return
	}
	sub.mu.Unlock()

	local, err := webrtc.NewTrackLocalStaticRTP(codec, key, rp.uid)
	if err != nil {
		sub.log.Warn("create remote down track failed", "err", err)
		return
	}
	sender, err := sub.pc.AddTrack(local)
	if err != nil {
		sub.log.Warn("add remote down track failed", "err", err)
		return
	}
	if rp.kind == KindScreen {
		// 屏幕轨 RTCP：观看端 PLI/FIR 经级联回传源节点（节流在级联层）。
		go forwardRemoteScreenRTCP(sender, rp)
	} else {
		go drainRTCP(sender)
	}

	sub.mu.Lock()
	dt := &downTrack{pubUID: rp.uid, track: local, sender: sender,
		active: !sub.hasUnsubscribedLocked(rp.uid, rp.kind), owner: rp}
	if rp.kind == KindAudio {
		sub.downTracks[key] = dt
	} else {
		sub.screenDown[key] = dt
	}
	sub.mu.Unlock()
	sub.negotiate()
	if rp.kind == KindScreen {
		// 新观看端就位：向源节点请求关键帧，缩短首帧等待。
		rp.RequestKeyframe()
	}
}

// forwardRemoteScreenRTCP 读取本地观看端对远端屏幕轨的 RTCP，PLI/FIR 触发级联回传。
func forwardRemoteScreenRTCP(sender *webrtc.RTPSender, rp *RemotePublisher) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				rp.RequestKeyframe()
			}
		}
	}
}

// hasUnsubscribedLocked 按轨 kind 查退订标记；需在 sub.mu 已持有时调用。
func (p *Participant) hasUnsubscribedLocked(pubUID, kind string) bool {
	u := p.unsubscribed[pubUID]
	if kindIsVideo(kind) {
		return u.video
	}
	return u.audio
}

// rebuildRemoteFanout 重建远端轨的下行轨快照。
func (r *Room) rebuildRemoteFanout(rp *RemotePublisher) {
	if rp.closed.Load() {
		return
	}
	key := rp.trackKey()
	var list []*downTrack
	for _, sub := range r.snapshotParts() {
		if sub.closed.Load() {
			continue
		}
		sub.mu.Lock()
		tracks := sub.downTracks
		if rp.kind != KindAudio {
			tracks = sub.screenDown
		}
		dt := tracks[key]
		active := dt != nil && dt.owner == rp && dt.active
		unsub := sub.hasUnsubscribedLocked(rp.uid, rp.kind)
		sub.mu.Unlock()
		if active && rp.remoteShouldForward(sub, unsub) {
			list = append(list, dt)
		}
	}
	rp.fanout.Store(&list)
}

// snapshotRemotePubs 在 Manager 锁下拷贝远端发布者列表。
func (r *Room) snapshotRemotePubs() []*RemotePublisher {
	r.mgr.mu.RLock()
	defer r.mgr.mu.RUnlock()
	out := make([]*RemotePublisher, 0, len(r.remotePubs))
	for _, rp := range r.remotePubs {
		out = append(out, rp)
	}
	return out
}

// rebuildAllRemoteFanouts 重建房内全部远端发布者的 fanout（成员/caps 变化后）。
func (r *Room) rebuildAllRemoteFanouts() {
	for _, rp := range r.snapshotRemotePubs() {
		r.rebuildRemoteFanout(rp)
	}
}

// AttachCascadeSink 把本地发布者某条轨（trackKey = TrackKey(sid, kind)）的上行 RTP
// 旁路注入 sink：sink 会随对应 fanout 快照被转发循环直接写入，无需额外读协程。
// 返回 detach 函数（幂等性由调用方保证只调一次）。
func (m *Manager) AttachCascadeSink(roomID, trackKey string, sink RTPWriter) (func(), error) {
	pubSID, kind := SplitTrackKey(trackKey)
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("room %s not found", roomID)
	}
	p := r.parts[pubSID]
	if p == nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("publisher %s not found in room %s", pubSID, roomID)
	}
	dt := &downTrack{pubUID: p.uid, track: sink, active: true}
	r.cascadeSinks[trackKey] = append(r.cascadeSinks[trackKey], dt)
	m.mu.Unlock()
	r.rebuildCascadeFanout(p, kind)

	detach := func() {
		m.mu.Lock()
		list := r.cascadeSinks[trackKey]
		for i, x := range list {
			if x == dt {
				r.cascadeSinks[trackKey] = append(list[:i:i], list[i+1:]...)
				break
			}
		}
		if len(r.cascadeSinks[trackKey]) == 0 {
			delete(r.cascadeSinks, trackKey)
		}
		p2 := r.parts[pubSID]
		m.mu.Unlock()
		if p2 != nil {
			r.rebuildCascadeFanout(p2, kind)
		}
	}
	return detach, nil
}

// rebuildCascadeFanout 按 kind 重建对应 fanout（AttachCascadeSink/detach 后调用）。
func (r *Room) rebuildCascadeFanout(p *Participant, kind string) {
	switch kind {
	case KindScreen:
		r.rebuildScreenFanout(p)
	case KindScreenAudio:
		r.rebuildScreenAudioFanout(p)
	default:
		r.rebuildFanout(p)
	}
}

// LocalDemand 聚合本地听众的音频订阅需求（NodeWant 的本地分量，08 §5.1）：
//   - want=false：本地无持 subscribe_audio 的成员，任何 speaker 都不需要；
//   - want=true：需要「除 except 外的全部 speaker」。x ∈ except 当且仅当
//     每个本地听众要么就是 x 本人、要么已显式退订 x（首期：静音=退订）。
func (m *Manager) LocalDemand(roomID string) (want bool, except []string) {
	return m.localDemand(roomID, auth.CapSubscribeAudio, false)
}

// LocalScreenDemand 聚合本地观看端的屏幕轨订阅需求（伴轨跟随屏幕，共用此需求）：
// 与 LocalDemand 同构，但观看侧仅要求 join（无独立 subscribe_screen cap，见
// ShouldForwardScreen），且按退订位图的 video 维度聚合（协议 §2.1 kinds）：
// 「本地全部成员都退订了 x 的视频」⇔「x 的屏幕轨不得跨节点拉流」（08 D.4/D.5）。
func (m *Manager) LocalScreenDemand(roomID string) (want bool, except []string) {
	return m.localDemand(roomID, auth.CapJoin, true)
}

func (m *Manager) localDemand(roomID, listenerCap string, video bool) (want bool, except []string) {
	m.mu.RLock()
	r, ok := m.rooms[roomID]
	m.mu.RUnlock()
	if !ok {
		return false, nil
	}

	type listener struct {
		uid   string
		unsub map[string]struct{}
	}
	var listeners []listener
	for _, p := range r.snapshotParts() {
		if p.closed.Load() || !p.Caps().Has(listenerCap) {
			continue
		}
		p.mu.Lock()
		u := make(map[string]struct{}, len(p.unsubscribed))
		for k, marks := range p.unsubscribed {
			if (video && marks.video) || (!video && marks.audio) {
				u[k] = struct{}{}
			}
		}
		p.mu.Unlock()
		listeners = append(listeners, listener{uid: p.uid, unsub: u})
	}
	if len(listeners) == 0 {
		return false, nil
	}

	// except 候选 ⊆ 各听众退订集合的并集
	cand := make(map[string]struct{})
	for _, l := range listeners {
		for x := range l.unsub {
			cand[x] = struct{}{}
		}
	}
	for x := range cand {
		wanted := false
		for _, l := range listeners {
			if l.uid == x {
				continue // speaker 本人不构成对自己的需求
			}
			if _, off := l.unsub[x]; !off {
				wanted = true
				break
			}
		}
		if !wanted {
			except = append(except, x)
		}
	}
	return true, except
}
