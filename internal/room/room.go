package room

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"

	"github.com/owlspeak/owl-sfu/internal/auth"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// speakingInterval 为 speaking 事件聚合推送节流周期。
const speakingInterval = 250 * time.Millisecond

// Room 为本节点上的一个逻辑房间实例。
type Room struct {
	id  string
	mgr *Manager

	// parts 由 Manager.mu 保护（房间成员变更均持 Manager 锁，避免双层锁序问题）
	parts map[string]*Participant // key = sid
	// ensured 表示经 EnsureLogicalRoom 显式建房（空房不回收，等 CloseLogicalRoom）
	ensured bool
	// removed 表示已从 Manager.rooms 摘除（防重复回收/重复减计数）
	removed bool

	// remotePubs：级联送入的远端轨（键=TrackKey(源 sid, kind)）；由 Manager.mu 保护
	remotePubs map[string]*RemotePublisher
	// cascadeSinks：本地发布者轨（键=TrackKey(sid, kind)）→ 级联上行旁路轨；由 Manager.mu 保护
	cascadeSinks map[string][]*downTrack

	stopSpeaking chan struct{}
	stopOnce     sync.Once
	prevSpeaking string // 上次推送的 speaking uid 集合指纹
}

// stopLoop 幂等停止 speaking 聚合协程。
func (r *Room) stopLoop() {
	r.stopOnce.Do(func() { close(r.stopSpeaking) })
}

func newRoom(id string, mgr *Manager, ensured bool) *Room {
	r := &Room{
		id:           id,
		mgr:          mgr,
		parts:        make(map[string]*Participant),
		ensured:      ensured,
		stopSpeaking: make(chan struct{}),
		remotePubs:   make(map[string]*RemotePublisher),
		cascadeSinks: make(map[string][]*downTrack),
	}
	go r.speakingLoop()
	return r
}

// snapshotParts 在 Manager 锁下拷贝成员列表。
func (r *Room) snapshotParts() []*Participant {
	r.mgr.mu.RLock()
	defer r.mgr.mu.RUnlock()
	out := make([]*Participant, 0, len(r.parts))
	for _, p := range r.parts {
		out = append(out, p)
	}
	return out
}

// broadcast 向房内全部（或除 except 外）成员发事件。
func (r *Room) broadcast(except *Participant, op string, d any) {
	for _, p := range r.snapshotParts() {
		if p == except || p.closed.Load() {
			continue
		}
		if err := p.msgr.Send(op, d); err != nil {
			p.log.Debug("broadcast send failed", "op", op, "err", err)
		}
	}
}

// onTrack 发布者上行轨到达：按 kind 分发到音频/屏幕路径。
func (r *Room) onTrack(p *Participant, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	switch track.Kind() {
	case webrtc.RTPCodecTypeAudio:
		r.onAudioTrack(p, track, receiver)
	case webrtc.RTPCodecTypeVideo:
		// 视频轨 = 屏幕共享（docs 14；摄像头视频不在范围内），见 screen.go。
		r.onScreenTrack(p, track, receiver)
	default:
		p.log.Warn("unknown track kind ignored", "kind", track.Kind().String())
	}
}

// onAudioTrack 上行音频轨：广播 track_published、给订阅者挂下行轨、起转发循环。
// 同会话第二条 audio 轨 = 屏幕共享系统音频伴轨（docs 14 BA.4，见 screen.go）。
//
// 无 publish_audio cap 的轨（STAGE 模式 AUDIENCE，docs 11 AD.4）**挂起接纳**而非
// 丢弃：读循环照常启动，转发在 forwardLoop 内按包级 caps 门控保持静默；抱上麦
// （bring-up → UpdateParticipantCaps 授予 cap）后 applyCaps 立即 track_published +
// 挂订阅者下行轨，无需客户端重新发布，达成「bring-up 后 <1s 可发声」（15 BM M5）。
func (r *Room) onAudioTrack(p *Participant, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	if p.publishing.Load() {
		r.onScreenAudioTrack(p, track, receiver)
		return
	}
	p.publishing.Store(true)
	if p.Caps().Has(auth.CapPublishAudio) {
		p.log.Info("publisher track up", "sid", p.sid, "uid", p.uid)
		// 音频审计：开始录制该说话者上行音频（旁路，不影响转发）。
		p.startAudit()
		r.broadcast(p, "track_published", map[string]any{"user_id": p.uid, "kind": KindAudio})
		r.attachSubscribersToPublisher(p)
		r.mgr.cascadePublish(r.id, p.sid, p.uid, KindAudio, true)
	} else {
		p.log.Info("audio track held pending publish_audio cap (audience, docs 11 AD.4)",
			"sid", p.sid, "uid", p.uid)
	}

	extID := audioLevelExtensionID(receiver)
	go func() {
		r.forwardLoop(p, func(b []byte) (int, error) {
			n, _, err := track.Read(b)
			return n, err
		}, extID)
		// 读循环退出 = 上行轨结束（PC 关闭或 transceiver 停止）。
		// 仅在当前仍持 publish_audio（曾对外发布且未被 applyCaps 收回时摘除）
		// 才广播 track_ended / 拆下行轨——挂起未发布的轨结束时对外无事件。
		if p.publishing.CompareAndSwap(true, false) && !p.closed.Load() &&
			p.Caps().Has(auth.CapPublishAudio) {
			r.broadcast(p, "track_ended", map[string]any{"user_id": p.uid, "kind": KindAudio})
			r.detachPublisher(p)
			r.mgr.cascadePublish(r.id, p.sid, p.uid, KindAudio, false)
		}
		// 上行轨结束即收尾审计录音（若开启）；participant 离开路径的 finishAudit 幂等。
		p.finishAudit()
	}()
}

// audioLevelExtensionID 从协商结果中取 ssrc-audio-level 扩展 ID（未协商返回 0）。
func audioLevelExtensionID(receiver *webrtc.RTPReceiver) uint8 {
	for _, ext := range receiver.GetParameters().HeaderExtensions {
		if ext.URI == sdp.AudioLevelURI {
			return uint8(ext.ID)
		}
	}
	return 0
}

// attachSubscribersToPublisher 为房内所有合规订阅者补挂 pub 的下行轨并触发协商。
func (r *Room) attachSubscribersToPublisher(pub *Participant) {
	for _, sub := range r.snapshotParts() {
		if sub == pub {
			continue
		}
		r.ensureDownTrack(pub, sub)
	}
	r.rebuildFanout(pub)
}

// attachPublishersToSubscriber 给新订阅者补挂房内全部在播发布者的下行轨（音频 + 屏幕 + 伴轨）。
func (r *Room) attachPublishersToSubscriber(sub *Participant) {
	for _, pub := range r.snapshotParts() {
		if pub == sub {
			continue
		}
		if pub.Publishing() {
			r.ensureDownTrack(pub, sub)
			r.rebuildFanout(pub)
		}
		if pub.ScreenPublishing() {
			r.ensureScreenDownTrack(pub, sub)
			r.rebuildScreenFanout(pub)
		}
		if pub.ScreenAudioPublishing() {
			r.ensureScreenAudioDownTrack(pub, sub)
			r.rebuildScreenAudioFanout(pub)
		}
	}
	// 级联送入的远端 speaker 同样补挂
	for _, rp := range r.snapshotRemotePubs() {
		r.ensureRemoteDownTrack(rp, sub)
		r.rebuildRemoteFanout(rp)
	}
}

// ensureDownTrack 确保 sub 持有来自 pub 的下行轨（caps/订阅允许时）；返回是否新建。
func (r *Room) ensureDownTrack(pub, sub *Participant) bool {
	if sub.closed.Load() || !pub.Publishing() {
		return false
	}
	if !ShouldForward(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid, KindAudio)) {
		return false
	}
	sub.mu.Lock()
	if _, ok := sub.downTracks[pub.sid]; ok {
		sub.mu.Unlock()
		return false
	}
	sub.mu.Unlock()

	local, err := webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, pub.sid, pub.uid)
	if err != nil {
		sub.log.Warn("create down track failed", "err", err)
		return false
	}
	sender, err := sub.pc.AddTrack(local)
	if err != nil {
		sub.log.Warn("add down track failed", "err", err)
		return false
	}
	go drainRTCP(sender)

	sub.mu.Lock()
	sub.downTracks[pub.sid] = &downTrack{pubUID: pub.uid, track: local, sender: sender, active: true}
	sub.mu.Unlock()
	sub.negotiate()
	return true
}

// drainRTCP 排空 sender 的 RTCP（interceptor 要求持续读取）。
func drainRTCP(sender *webrtc.RTPSender) {
	buf := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buf); err != nil {
			return
		}
	}
}

// removeDownTrack 摘除 sub 上来自 pub 的下行轨并触发协商。
func (r *Room) removeDownTrack(pubSID string, sub *Participant) {
	sub.mu.Lock()
	dt, ok := sub.downTracks[pubSID]
	if ok {
		delete(sub.downTracks, pubSID)
	}
	sub.mu.Unlock()
	if !ok || sub.closed.Load() {
		return
	}
	if err := sub.pc.RemoveTrack(dt.sender); err != nil {
		sub.log.Debug("remove down track", "err", err)
	}
	sub.negotiate()
}

// detachPublisher 摘除 pub 在所有订阅者上的下行轨（track 结束/发布权收回/离房）。
func (r *Room) detachPublisher(pub *Participant) {
	pub.fanout.Store(nil)
	for _, sub := range r.snapshotParts() {
		if sub == pub {
			continue
		}
		r.removeDownTrack(pub.sid, sub)
	}
}

// rebuildFanout 重建发布者的下行轨快照（订阅/caps/成员变化后调用）。
func (r *Room) rebuildFanout(pub *Participant) {
	var list []*downTrack
	pubCaps := pub.Caps()
	for _, sub := range r.snapshotParts() {
		if sub == pub || sub.closed.Load() {
			continue
		}
		sub.mu.Lock()
		dt := sub.downTracks[pub.sid]
		active := dt != nil && dt.active
		unsub := sub.hasUnsubscribedLocked(pub.uid, KindAudio)
		sub.mu.Unlock()
		if active && ShouldForward(pubCaps, sub.Caps(), unsub) {
			list = append(list, dt)
		}
	}
	// 级联上行旁路轨：发布者持 publish_audio 时随快照一并被 forwardLoop 写入
	if pubCaps.Has(auth.CapPublishAudio) {
		r.mgr.mu.RLock()
		list = append(list, r.cascadeSinks[pub.sid]...)
		r.mgr.mu.RUnlock()
	}
	pub.fanout.Store(&list)
}

// setSubscription 处理 subscribe/unsubscribe(user_id, kinds)：按 mask 选中的轨类型
// 维度对该发布者生效（协议 §2.1；kinds 缺省 = 全部维度，旧客户端行为不变）。
// audio 维度作用于音频轨；video 维度作用于屏幕轨与系统音频伴轨（伴轨跟随屏幕
// 会话，docs 14 BA.4）。退订状态持久在参与者会话上：先退订、发布者后发布的轨
// 在 ensure*DownTrack 时按维度检查，不会误挂。
func (r *Room) setSubscription(sub *Participant, pubUID string, mask unsubKinds, want bool) {
	if mask.none() {
		return
	}
	sub.mu.Lock()
	cur := sub.unsubscribed[pubUID]
	if mask.audio {
		cur.audio = !want
	}
	if mask.video {
		cur.video = !want
	}
	if cur.none() {
		delete(sub.unsubscribed, pubUID)
	} else {
		sub.unsubscribed[pubUID] = cur
	}
	if mask.audio {
		for _, dt := range sub.downTracks {
			if dt.pubUID == pubUID {
				dt.active = want
			}
		}
	}
	if mask.video {
		for _, dt := range sub.screenDown {
			if dt.pubUID == pubUID {
				dt.active = want
			}
		}
	}
	sub.mu.Unlock()

	for _, pub := range r.snapshotParts() {
		if pub.uid != pubUID || pub == sub {
			continue
		}
		if want {
			// 重订阅：若此前从未建轨（如进房前已退订）则按维度补建
			if mask.audio {
				r.ensureDownTrack(pub, sub)
			}
			if mask.video {
				r.ensureScreenDownTrack(pub, sub)
				r.ensureScreenAudioDownTrack(pub, sub)
			}
		}
		if mask.audio {
			r.rebuildFanout(pub)
		}
		if mask.video {
			r.rebuildScreenFanout(pub)
			r.rebuildScreenAudioFanout(pub)
			if want && pub.ScreenPublishing() {
				// 视频重订阅（点观看）：主动请求关键帧缩短首帧等待（节流合并）
				r.requestScreenKeyframe(pub, nil)
			}
		}
	}
	// 远端发布者（级联送入）同语义生效
	for _, rp := range r.snapshotRemotePubs() {
		if rp.uid != pubUID {
			continue
		}
		if kindIsVideo(rp.kind) {
			if !mask.video {
				continue
			}
		} else if !mask.audio {
			continue
		}
		if want {
			r.ensureRemoteDownTrack(rp, sub)
			if rp.kind == KindScreen {
				rp.RequestKeyframe()
			}
		}
		r.rebuildRemoteFanout(rp)
	}
	// 退订/重订影响 NodeWant 聚合（08 §5.1：静音/未观看=退订 → 向上剪枝）
	r.mgr.cascadeDemand(r.id)
}

// applyCaps 执行 caps 全量替换并让转发行为实时生效。
func (r *Room) applyCaps(p *Participant, caps auth.CapSet) {
	old := p.Caps()
	p.setCaps(caps)

	if err := p.msgr.Send("caps_updated", map[string]any{"caps": caps.Slice()}); err != nil {
		p.log.Debug("send caps_updated failed", "err", err)
	}

	// 发布权收回：停止转发其包并摘除其在订阅者上的下行轨
	if old.Has(auth.CapPublishAudio) && !caps.Has(auth.CapPublishAudio) {
		p.fanout.Store(nil)
		if p.Publishing() {
			r.broadcast(p, "track_ended", map[string]any{"user_id": p.uid, "kind": KindAudio})
			r.detachPublisher(p)
			r.mgr.cascadePublish(r.id, p.sid, p.uid, KindAudio, false)
		}
	}
	// 发布权恢复/授予：上行轨若仍在流（含挂起接纳的 AUDIENCE 轨，onAudioTrack）
	// 则立即对外分发——bring-up 后 <1s 可发声（docs 11 AD.4 / 15 BM M5）。
	if !old.Has(auth.CapPublishAudio) && caps.Has(auth.CapPublishAudio) && p.Publishing() {
		p.startAudit()
		r.broadcast(p, "track_published", map[string]any{"user_id": p.uid, "kind": KindAudio})
		r.attachSubscribersToPublisher(p)
		r.mgr.cascadePublish(r.id, p.sid, p.uid, KindAudio, true)
	}

	// 订阅权收回：摘除其全部下行轨
	if old.Has(auth.CapSubscribeAudio) && !caps.Has(auth.CapSubscribeAudio) {
		p.mu.Lock()
		pubSIDs := make([]string, 0, len(p.downTracks))
		for sid := range p.downTracks {
			pubSIDs = append(pubSIDs, sid)
		}
		p.mu.Unlock()
		for _, sid := range pubSIDs {
			r.removeDownTrack(sid, p)
		}
	}
	// 订阅权恢复：补挂全部在播发布者
	if !old.Has(auth.CapSubscribeAudio) && caps.Has(auth.CapSubscribeAudio) {
		r.attachPublishersToSubscriber(p)
	}

	// 屏幕发布权变化（docs 14）：收回 → 立即停收上行 + track_ended 广播，见 screen.go。
	r.applyScreenCaps(p, old, caps)

	// 其余情况统一重建 fanout（覆盖 publish/subscribe 组合变化）
	for _, pub := range r.snapshotParts() {
		r.rebuildFanout(pub)
		r.rebuildScreenFanout(pub)
		r.rebuildScreenAudioFanout(pub)
	}
	r.rebuildAllRemoteFanouts()
	// caps 变化影响本地订阅需求聚合
	r.mgr.cascadeDemand(r.id)
}

// speakingLoop 每 250ms 聚合一次 speaking 状态，有变化才推送。
func (r *Room) speakingLoop() {
	t := time.NewTicker(speakingInterval)
	defer t.Stop()
	for {
		select {
		case <-r.stopSpeaking:
			return
		case now := <-t.C:
			var uids []string
			for _, p := range r.snapshotParts() {
				if !p.Publishing() || !p.lastSpeaking.Load() {
					continue
				}
				if now.UnixMilli()-p.lastLevelAtMs.Load() > speakingStaleAfter.Milliseconds() {
					continue
				}
				uids = append(uids, p.uid)
			}
			sort.Strings(uids)
			fp := strings.Join(uids, ",")
			if fp == r.prevSpeaking {
				continue
			}
			r.prevSpeaking = fp
			if uids == nil {
				uids = []string{}
			}
			r.broadcast(nil, "speaking", map[string]any{"user_ids": uids})
			r.mgr.metrics.SpeakingEventsPublished.Inc()
		}
	}
}

// removeParticipant 从房间摘除成员：清双向轨、广播 left、上报控制面、必要时回收空房。
func (r *Room) removeParticipant(p *Participant, reason string) {
	r.mgr.mu.Lock()
	if _, ok := r.parts[p.sid]; !ok {
		r.mgr.mu.Unlock()
		return
	}
	delete(r.parts, p.sid)
	delete(r.mgr.bySID, p.sid)
	roomGone := len(r.parts) == 0 && !r.ensured && !r.removed
	if roomGone {
		delete(r.mgr.rooms, r.id)
		r.removed = true
	}
	r.mgr.mu.Unlock()

	r.mgr.metrics.Participants.Dec()
	if roomGone {
		r.stopLoop()
		r.mgr.metrics.Rooms.Dec()
	}

	// 其作为发布者的下行轨（音频 + 屏幕 + 伴轨）从各订阅者摘除；其自身 PC 已关，无需摘自身下行轨
	r.finishScreenPublish(p) // closed 已置位：仅校正计数与级联通知，事件语义由 participant_left 覆盖
	r.finishScreenAudioPublish(p)
	r.detachPublisher(p)
	r.detachScreenPublisher(p)
	r.detachScreenAudioPublisher(p)
	// 清理远端 fanout 中指向该成员的失效下行轨，并通知级联：发布结束 + 需求变化
	r.rebuildAllRemoteFanouts()
	r.mgr.cascadePublish(r.id, p.sid, p.uid, KindAudio, false)
	r.mgr.cascadeDemand(r.id)

	// 隐身临场：不广播其离开（加入时也未广播，保持对齐）。
	if !p.hidden {
		r.broadcast(nil, "participant_left", map[string]any{"user_id": p.uid, "session_id": p.sid, "is_bot": p.isBot})
	}
	// 审计录音收尾：finalize 并上传主节点（若开启）。
	p.finishAudit()
	r.mgr.events.ParticipantLeft(r.id, p.sid, p.uid, reason)
	p.log.Info("participant left", "room", r.id, "sid", p.sid, "uid", p.uid, "reason", reason)
}
