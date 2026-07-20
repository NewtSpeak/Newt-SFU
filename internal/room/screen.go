// 屏幕共享（视频轨）接收、选路转发、订阅 fanout 与 RTCP 关键帧回传（docs 14 / docs 15 §7.1）。
//
// 设计要点：
//   - 选路 RTP 转发、不解码不转码（BA.3）；转发路径复用 rtpBufPool 零分配原则；
//   - publish_screen caps 校验：无 cap 的 video 轨经 renegotiation 剥离（不直接关会话，
//     避免「Server 已发 caps、SFU 指令未达」竞态误杀；CAP_DENIED 关闭码保留给 auth 路径）；
//   - 每用户同时 1 路屏幕（AX.4）：第二条 video 轨同样剥离；同一路的 simulcast
//     多 rid 层（BA.3）算 1 路，SFU 按观看端 set_layer 请求选层转发；
//   - 观看端 PLI/FIR 回传发布者（节流防风暴）；REMB/TWCC 取舍见 sfu/interceptors.go；
//   - 系统音频伴轨（BA.4）：同会话第二条 audio 轨，跟随屏幕会话转发，见 screen-audio 段。
package room

import (
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/owlspeak/owl-sfu/internal/auth"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// keyframeMinInterval 关键帧请求（PLI/FIR）向发布者转发的最小间隔，
// 多观看端同时请求时合并为一次，防 PLI 风暴。
const keyframeMinInterval = 300 * time.Millisecond

// 屏幕 simulcast 质量档（set_layer 信令取值；BA.3）。
const (
	LayerHigh   = "high"
	LayerMedium = "medium"
	LayerLow    = "low"
)

// layerFallback 选层回退顺序：请求档缺失时取仍在发布的最高档。
var layerFallback = []string{LayerHigh, LayerMedium, LayerLow}

// ridQuality 映射发布端 rid → 质量档（约定 rid ∈ {a,b,c} 或 {h,m,l}，BA.3）；
// 空 rid = 未开 simulcast 的单层（按 high 对待）；未知 rid 返回 ""（该层丢弃）。
func ridQuality(rid string) string {
	switch rid {
	case "", "a", "h":
		return LayerHigh
	case "b", "m":
		return LayerMedium
	case "c", "l":
		return LayerLow
	}
	return ""
}

// screenLayer 为屏幕上行的一个编码层（非 simulcast 单层 quality=high、rid=""）。
type screenLayer struct {
	rid     string
	quality string
	ssrc    atomic.Uint32
	// fanout 订阅了该层的下行轨快照（rebuildScreenFanout 按观看端选层分桶）
	fanout atomic.Pointer[[]*downTrack]
}

// ShouldForwardScreen 为屏幕轨（及系统音频伴轨）纯转发决策函数（单测锚点）：
// 发布者需持 publish_screen；观看侧无独立 subscribe_screen cap（协议 caps 枚举仅四项），
// 房内成员默认可看（docs 14 §7.4「同频道成员通过事件知悉谁在共享」），故仅要求 join；
// 退订位图按轨类型分维（subscribe/unsubscribe 的 kinds 字段，协议 §2.1）：
// subUnsubscribed 传入 video 维度的退订标记，与音频维度独立。
func ShouldForwardScreen(pubCaps, subCaps auth.CapSet, subUnsubscribed bool) bool {
	return pubCaps.Has(auth.CapPublishScreen) &&
		subCaps.Has(auth.CapJoin) &&
		!subUnsubscribed
}

// onScreenTrack 发布者上行视频轨到达：caps/路数校验 → 广播 → 建下行轨 → 起转发循环。
// simulcast 场景下同一 transceiver 会按 rid 触发多次（每层一轨），只有首层广播/上报。
func (r *Room) onScreenTrack(p *Participant, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	// 1. publish_screen caps 校验（docs 15 §7.1）：无 cap → renegotiation 剥离。
	if !p.Caps().Has(auth.CapPublishScreen) {
		p.log.Warn("video track without publish_screen cap, stripping", "sid", p.sid, "uid", p.uid)
		r.stripVideoTransceiver(p, receiver)
		return
	}
	quality := ridQuality(track.RID())
	if quality == "" {
		// 未知 rid 层：持续排空（不读会阻塞对端拥塞控制）但不转发。
		p.log.Warn("unknown simulcast rid, draining", "sid", p.sid, "rid", track.RID())
		go drainRemoteTrack(track)
		return
	}

	// 2. 每用户同时 1 路屏幕（docs 14 AX.4）：第二条 video 轨剥离；
	//    同一 transceiver 的其余 simulcast rid 层属于同一路，直接收编。
	//    准入与层注册同锁完成：多个 rid 层的 OnTrack 可能并发触发。
	tcv := transceiverOfReceiver(p.pc, receiver)
	layer := &screenLayer{rid: track.RID(), quality: quality}
	layer.ssrc.Store(uint32(track.SSRC()))

	p.mu.Lock()
	first := !p.screenPublishing.Load()
	if first {
		p.screenPublishing.Store(true)
		p.screenTcv = tcv
	} else if track.RID() == "" || p.screenTcv == nil || p.screenTcv != tcv {
		p.mu.Unlock()
		p.log.Warn("second video track rejected (1 screen per user)", "sid", p.sid, "uid", p.uid)
		r.stripVideoTransceiver(p, receiver)
		return
	}
	if p.screenLayers == nil {
		p.screenLayers = make(map[string]*screenLayer)
	}
	p.screenLayers[quality] = layer // 同 quality 重挂（renegotiation）：替换旧层
	p.mu.Unlock()

	if first {
		r.mgr.screenTracks.Add(1)
		r.mgr.metrics.ScreenTracks.Inc()
		p.log.Info("screen track up", "sid", p.sid, "uid", p.uid,
			"codec", track.Codec().MimeType, "rid", track.RID())
		// 广播 kind=screen 的发布事件 + 控制面上报（Server 据此 RESERVED→ACTIVE，docs 14 BC.1 步骤 5）。
		r.broadcast(p, "track_published", map[string]any{"user_id": p.uid, "kind": KindScreen})
		r.mgr.events.ScreenTrackActive(r.id, p.sid, p.uid)
		r.attachScreenSubscribers(p)
		r.mgr.cascadePublish(r.id, p.sid, p.uid, KindScreen, true)
	} else {
		p.log.Info("screen simulcast layer up", "sid", p.sid, "rid", track.RID(), "quality", quality)
		r.rebuildScreenFanout(p)
	}

	go func() {
		r.forwardScreenLayerLoop(p, layer, track)
		// 读循环退出 = 该层上行结束；全部层结束才算屏幕发布结束
		//（客户端停止共享 / caps 收回停收 / PC 关闭）。
		p.mu.Lock()
		if p.screenLayers[quality] == layer {
			delete(p.screenLayers, quality)
		}
		remaining := len(p.screenLayers)
		p.mu.Unlock()
		if remaining == 0 {
			r.finishScreenPublish(p)
		} else {
			r.rebuildScreenFanout(p)
		}
	}()
}

// drainRemoteTrack 持续读取并丢弃被拒绝的上行轨。
func drainRemoteTrack(track *webrtc.TrackRemote) {
	buf := make([]byte, 1500)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
	}
}

// finishScreenPublish 幂等收尾屏幕发布：广播 track_ended、摘下行轨、控制面上报。
// caps 收回路径与读循环退出路径都会到达这里，以 CompareAndSwap 保证只执行一次。
// 系统音频伴轨与屏幕同一共享会话（BA.4），屏幕结束时一并收尾。
func (r *Room) finishScreenPublish(p *Participant) {
	if !p.screenPublishing.CompareAndSwap(true, false) {
		return
	}
	p.mu.Lock()
	p.screenLayers = nil
	p.mu.Unlock()
	r.mgr.screenTracks.Add(-1)
	r.mgr.metrics.ScreenTracks.Dec()
	r.mgr.cascadePublish(r.id, p.sid, p.uid, KindScreen, false)
	r.finishScreenAudioPublish(p)
	if p.closed.Load() {
		// 会话已关闭：participant_left 已覆盖客户端侧语义；配额释放由 Server 离房路径处理。
		return
	}
	r.broadcast(p, "track_ended", map[string]any{"user_id": p.uid, "kind": KindScreen})
	r.detachScreenPublisher(p)
	r.mgr.events.ScreenTrackEnded(r.id, p.sid, p.uid)
}

// stripVideoTransceiver 经 renegotiation 剥离非法 video 上行（stop transceiver → SFU 发 offer）。
func (r *Room) stripVideoTransceiver(p *Participant, receiver *webrtc.RTPReceiver) {
	if tcv := transceiverOfReceiver(p.pc, receiver); tcv != nil {
		if err := tcv.Stop(); err != nil {
			p.log.Debug("stop video transceiver failed", "err", err)
		}
	}
	p.negotiate()
}

// transceiverOfReceiver 反查 receiver 所属 transceiver。
func transceiverOfReceiver(pc *webrtc.PeerConnection, receiver *webrtc.RTPReceiver) *webrtc.RTPTransceiver {
	for _, tcv := range pc.GetTransceivers() {
		if tcv.Receiver() == receiver {
			return tcv
		}
	}
	return nil
}

// forwardScreenLayerLoop 读取发布者某层上行视频 RTP 并写入该层 fanout 快照
// （不解码不转码，BA.3）。转发前剥离 RTP 头扩展：上行的 mid/rid（simulcast 分流用）
// 与 transport-cc 序号只对本跳有意义，透传会污染下行/级联对端的扩展语义。
func (r *Room) forwardScreenLayerLoop(p *Participant, layer *screenLayer, track *webrtc.TrackRemote) {
	bufp := rtpBufPool.Get().(*[]byte)
	defer rtpBufPool.Put(bufp)
	buf := *bufp

	var pkt rtp.Packet
	for {
		n, _, err := track.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !p.closed.Load() {
				p.log.Debug("screen track read ended", "err", err)
			}
			return
		}
		pkt.Header.Extensions = pkt.Header.Extensions[:0]
		pkt.Header.Extension = false
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		pkt.Header.Extensions = pkt.Header.Extensions[:0]
		pkt.Header.Extension = false

		// caps 热更兜底：publish_screen 被收回时立即丢弃（真正停收上行由
		// applyScreenCaps 停 transceiver 完成，本判断覆盖指令与停轨间的窗口）。
		if !p.Caps().Has(auth.CapPublishScreen) {
			continue
		}

		fanout := layer.fanout.Load()
		if fanout == nil {
			continue
		}
		for _, dt := range *fanout {
			// 单个观看端写失败（如 PC 正在关闭）不影响其余观看端
			_ = dt.track.WriteRTP(&pkt)
		}
		if cnt := len(*fanout); cnt > 0 {
			r.mgr.metrics.RTPForwardedPackets.Add(float64(cnt))
			r.mgr.metrics.RTPForwardedBytes.Add(float64(n * cnt))
			r.mgr.stats.AddForwardedBytes(n * cnt)
		}
	}
}

// attachScreenSubscribers 为房内所有观看端补挂 pub 的屏幕下行轨并重建 fanout。
func (r *Room) attachScreenSubscribers(pub *Participant) {
	for _, sub := range r.snapshotParts() {
		if sub == pub {
			continue
		}
		r.ensureScreenDownTrack(pub, sub)
	}
	r.rebuildScreenFanout(pub)
}

// ensureScreenDownTrack 确保 sub 持有来自 pub 的屏幕下行轨；返回是否新建。
func (r *Room) ensureScreenDownTrack(pub, sub *Participant) bool {
	if sub.closed.Load() || !pub.ScreenPublishing() {
		return false
	}
	if !ShouldForwardScreen(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid, KindScreen)) {
		return false
	}
	key := TrackKey(pub.sid, KindScreen)
	sub.mu.Lock()
	if _, ok := sub.screenDown[key]; ok {
		sub.mu.Unlock()
		return false
	}
	sub.mu.Unlock()

	// track id 加 "#screen" 后缀与音频轨（id=pub.sid）区分；stream id 沿用发布者 uid。
	local, err := webrtc.NewTrackLocalStaticRTP(owlsfu.VP8Codec, key, pub.uid)
	if err != nil {
		sub.log.Warn("create screen down track failed", "err", err)
		return false
	}
	sender, err := sub.pc.AddTrack(local)
	if err != nil {
		sub.log.Warn("add screen down track failed", "err", err)
		return false
	}
	// 屏幕下行轨的 RTCP 不只排空：解析观看端 PLI/FIR 并回传发布者。
	go r.forwardScreenRTCP(sender, pub)

	sub.mu.Lock()
	sub.screenDown[key] = &downTrack{pubUID: pub.uid, track: local, sender: sender, active: true}
	sub.mu.Unlock()
	sub.negotiate()
	// 新观看端就位后主动向发布者请求一次关键帧，缩短首帧等待（经节流合并）。
	r.requestScreenKeyframe(pub, nil)
	return true
}

// removeScreenDownTrack 摘除 sub 上来自 pub 的屏幕下行轨（按 trackKey）并触发协商。
func (r *Room) removeScreenDownTrack(key string, sub *Participant) {
	sub.mu.Lock()
	dt, ok := sub.screenDown[key]
	if ok {
		delete(sub.screenDown, key)
	}
	sub.mu.Unlock()
	if !ok || sub.closed.Load() {
		return
	}
	if err := sub.pc.RemoveTrack(dt.sender); err != nil {
		sub.log.Debug("remove screen down track", "err", err)
	}
	sub.negotiate()
}

// detachScreenPublisher 摘除 pub 的屏幕轨在所有观看端上的下行轨。
func (r *Room) detachScreenPublisher(pub *Participant) {
	pub.mu.Lock()
	for _, layer := range pub.screenLayers {
		layer.fanout.Store(nil)
	}
	pub.mu.Unlock()
	key := TrackKey(pub.sid, KindScreen)
	for _, sub := range r.snapshotParts() {
		if sub == pub {
			continue
		}
		r.removeScreenDownTrack(key, sub)
	}
}

// rebuildScreenFanout 重建发布者屏幕轨的下行快照（订阅/caps/成员/选层变化后调用）：
// 按每个观看端的 set_layer 请求（缺省 high、请求档缺失时回退最高可用档）把下行轨
// 分桶到对应编码层；级联上行旁路轨恒走最高可用档（跨节点选层为二期）。
func (r *Room) rebuildScreenFanout(pub *Participant) {
	pubCaps := pub.Caps()
	key := TrackKey(pub.sid, KindScreen)

	pub.mu.Lock()
	layers := make(map[string]*screenLayer, len(pub.screenLayers))
	for q, l := range pub.screenLayers {
		layers[q] = l
	}
	pub.mu.Unlock()
	if len(layers) == 0 {
		return
	}
	buckets := make(map[*screenLayer][]*downTrack, len(layers))

	for _, sub := range r.snapshotParts() {
		if sub == pub || sub.closed.Load() {
			continue
		}
		sub.mu.Lock()
		dt := sub.screenDown[key]
		active := dt != nil && dt.active
		unsub := sub.hasUnsubscribedLocked(pub.uid, KindScreen)
		want := sub.screenLayerSel[pub.uid]
		sub.mu.Unlock()
		if active && ShouldForwardScreen(pubCaps, sub.Caps(), unsub) {
			if layer := pickScreenLayer(layers, want); layer != nil {
				buckets[layer] = append(buckets[layer], dt)
			}
		}
	}
	// 级联上行旁路轨（发布者持 publish_screen 时生效）：走最高可用档。
	if pubCaps.Has(auth.CapPublishScreen) {
		r.mgr.mu.RLock()
		sinks := r.cascadeSinks[key]
		r.mgr.mu.RUnlock()
		if len(sinks) > 0 {
			if layer := pickScreenLayer(layers, LayerHigh); layer != nil {
				buckets[layer] = append(buckets[layer], sinks...)
			}
		}
	}
	for _, layer := range layers {
		list := buckets[layer]
		layer.fanout.Store(&list)
	}
}

// pickScreenLayer 选层：请求档存在则用之，否则按 high→medium→low 回退最高可用档。
func pickScreenLayer(layers map[string]*screenLayer, want string) *screenLayer {
	if l := layers[want]; l != nil {
		return l
	}
	for _, q := range layerFallback {
		if l := layers[q]; l != nil {
			return l
		}
	}
	return nil
}

// SetScreenLayer 处理观看端 set_layer 信令（BA.3 最小版：仅 rid 透传选层，无自动切换）：
// 记录选层请求并重建目标发布者的屏幕 fanout；发布端未开 simulcast 时自然无感（单层恒选）。
// 目前仅对本地发布者生效；级联送入的远端屏幕轨恒为源节点的最高档（跨节点选层为二期）。
func (p *Participant) SetScreenLayer(pubUID, quality string) {
	switch quality {
	case LayerHigh, LayerMedium, LayerLow:
	default:
		p.log.Debug("set_layer with unknown quality ignored", "quality", quality)
		return
	}
	p.mu.Lock()
	if p.screenLayerSel == nil {
		p.screenLayerSel = make(map[string]string)
	}
	p.screenLayerSel[pubUID] = quality
	p.mu.Unlock()

	for _, pub := range p.room.snapshotParts() {
		if pub.uid != pubUID || pub == p || !pub.ScreenPublishing() {
			continue
		}
		p.room.rebuildScreenFanout(pub)
		// 切层后新层需要关键帧才能起解（经节流合并）。
		p.room.requestScreenKeyframe(pub, nil)
	}
}

// applyScreenCaps 执行 publish_screen 维度的 caps 变化（applyCaps 调用）。
func (r *Room) applyScreenCaps(p *Participant, old, caps auth.CapSet) {
	// 收回 publish_screen（如 screen/stop、抱下、配额掐最早）：
	// 立即停止转发、广播 track_ended、上报控制面，并停收上行（stop transceiver + renegotiation）。
	// 系统音频伴轨与屏幕同一共享会话（BA.4）：一并停收。
	if old.Has(auth.CapPublishScreen) && !caps.Has(auth.CapPublishScreen) {
		r.finishScreenPublish(p)
		p.stopScreenUplink()
		p.stopScreenAudioUplink()
	}
	// 恢复 publish_screen：上行 transceiver 已在收回时停掉，须由客户端重新 offer 发布
	//（与 docs 14 BC.1 一致：重新走 screen/start → 授权 → publish 流程），此处无需动作。
}

// stopScreenUplink 停掉屏幕上行 transceiver 并触发协商（真正停收上行流量）。
func (p *Participant) stopScreenUplink() {
	p.mu.Lock()
	tcv := p.screenTcv
	p.screenTcv = nil
	p.mu.Unlock()
	if tcv == nil {
		return
	}
	if err := tcv.Stop(); err != nil {
		p.log.Debug("stop screen uplink transceiver failed", "err", err)
	}
	p.negotiate()
}

// forwardScreenRTCP 持续读取观看端屏幕下行轨的 RTCP；PLI/FIR 关键帧请求回传给发布者，
// 其余（RR 等）仅排空（interceptor 要求持续读取）。sender 被 RemoveTrack/PC 关闭后退出。
func (r *Room) forwardScreenRTCP(sender *webrtc.RTPSender, pub *Participant) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch fb := pkt.(type) {
			case *rtcp.PictureLossIndication:
				r.requestScreenKeyframe(pub, nil)
			case *rtcp.FullIntraRequest:
				r.requestScreenKeyframe(pub, fb)
			}
		}
	}
}

// requestScreenKeyframe 向发布者转发关键帧请求（keyframeMinInterval 节流合并）。
// simulcast 多层时对全部在播层请求（观看端可能分布在任意层）；
// fir 非 nil 时保留 FIR 语义透传（改写 SSRC），否则发 PLI。
func (r *Room) requestScreenKeyframe(pub *Participant, fir *rtcp.FullIntraRequest) {
	if pub.closed.Load() {
		return
	}
	pub.mu.Lock()
	ssrcs := make([]uint32, 0, len(pub.screenLayers))
	for _, layer := range pub.screenLayers {
		if ssrc := layer.ssrc.Load(); ssrc != 0 {
			ssrcs = append(ssrcs, ssrc)
		}
	}
	pub.mu.Unlock()
	if len(ssrcs) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	last := pub.screenKeyReqAtMs.Load()
	if now-last < keyframeMinInterval.Milliseconds() || !pub.screenKeyReqAtMs.CompareAndSwap(last, now) {
		return
	}
	out := make([]rtcp.Packet, 0, len(ssrcs))
	for _, ssrc := range ssrcs {
		if fir != nil {
			entries := make([]rtcp.FIREntry, 0, len(fir.FIR))
			for _, e := range fir.FIR {
				entries = append(entries, rtcp.FIREntry{SSRC: ssrc, SequenceNumber: e.SequenceNumber})
			}
			out = append(out, &rtcp.FullIntraRequest{MediaSSRC: ssrc, FIR: entries})
		} else {
			out = append(out, &rtcp.PictureLossIndication{MediaSSRC: ssrc})
		}
	}
	if err := pub.pc.WriteRTCP(out); err != nil {
		pub.log.Debug("forward keyframe request failed", "err", err)
	}
}

// RequestScreenKeyframe 请求某本地发布者的屏幕关键帧（级联 PLI 回传入口，
// 发布节点收到对端边的 PLI 后经此转发给发布客户端；经 keyframeMinInterval 节流）。
func (m *Manager) RequestScreenKeyframe(roomID, pubSID string) {
	m.mu.RLock()
	r, ok := m.rooms[roomID]
	var p *Participant
	if ok {
		p = r.parts[pubSID]
	}
	m.mu.RUnlock()
	if p == nil {
		return
	}
	r.requestScreenKeyframe(p, nil)
}

// ---- 系统音频伴轨（docs 14 BA.4）----

// onScreenAudioTrack 同会话第二条 audio 轨 = 屏幕共享系统音频伴轨：
// 仅当该会话持 publish_screen cap（屏幕轨活跃或即将活跃——cap 在 screen/start
// 授权时下发，伴轨可先于屏幕轨到达）时接受；不占屏幕路数配额（BA.4）。
func (r *Room) onScreenAudioTrack(p *Participant, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	if !p.Caps().Has(auth.CapPublishScreen) {
		p.log.Warn("second audio track without publish_screen cap, stripping (docs 14 BA.4)",
			"sid", p.sid, "uid", p.uid)
		r.stripVideoTransceiver(p, receiver) // 同一剥离路径对 audio transceiver 同样适用
		return
	}
	if !p.screenAudioPublishing.CompareAndSwap(false, true) {
		p.log.Warn("third audio track rejected (1 companion per session)", "sid", p.sid, "uid", p.uid)
		r.stripVideoTransceiver(p, receiver)
		return
	}
	p.mu.Lock()
	p.screenAudioTcv = transceiverOfReceiver(p.pc, receiver)
	p.mu.Unlock()

	p.log.Info("screen system-audio companion track up", "sid", p.sid, "uid", p.uid)
	r.broadcast(p, "track_published", map[string]any{"user_id": p.uid, "kind": KindScreenAudio})
	r.attachScreenAudioSubscribers(p)
	r.mgr.cascadePublish(r.id, p.sid, p.uid, KindScreenAudio, true)

	go func() {
		r.forwardScreenAudioLoop(p, track)
		r.finishScreenAudioPublish(p)
	}()
}

// finishScreenAudioPublish 幂等收尾伴轨发布（读循环退出 / caps 收回 / 屏幕会话结束）。
func (r *Room) finishScreenAudioPublish(p *Participant) {
	if !p.screenAudioPublishing.CompareAndSwap(true, false) {
		return
	}
	p.screenAudioFanout.Store(nil)
	r.mgr.cascadePublish(r.id, p.sid, p.uid, KindScreenAudio, false)
	if p.closed.Load() {
		return
	}
	r.broadcast(p, "track_ended", map[string]any{"user_id": p.uid, "kind": KindScreenAudio})
	r.detachScreenAudioPublisher(p)
}

// stopScreenAudioUplink 停掉伴轨上行 transceiver 并触发协商。
func (p *Participant) stopScreenAudioUplink() {
	p.mu.Lock()
	tcv := p.screenAudioTcv
	p.screenAudioTcv = nil
	p.mu.Unlock()
	if tcv == nil {
		return
	}
	if err := tcv.Stop(); err != nil {
		p.log.Debug("stop screen-audio uplink transceiver failed", "err", err)
	}
	p.negotiate()
}

// forwardScreenAudioLoop 读取伴轨上行 RTP 并写入伴轨 fanout（转发门控随 publish_screen）。
func (r *Room) forwardScreenAudioLoop(p *Participant, track *webrtc.TrackRemote) {
	bufp := rtpBufPool.Get().(*[]byte)
	defer rtpBufPool.Put(bufp)
	buf := *bufp

	var pkt rtp.Packet
	for {
		n, _, err := track.Read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !p.closed.Load() {
				p.log.Debug("screen-audio track read ended", "err", err)
			}
			return
		}
		pkt.Header.Extensions = pkt.Header.Extensions[:0]
		pkt.Header.Extension = false
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		// 伴轨跟随屏幕会话：publish_screen 收回即停止转发。
		if !p.Caps().Has(auth.CapPublishScreen) {
			continue
		}
		fanout := p.screenAudioFanout.Load()
		if fanout == nil {
			continue
		}
		for _, dt := range *fanout {
			_ = dt.track.WriteRTP(&pkt)
		}
		if cnt := len(*fanout); cnt > 0 {
			r.mgr.metrics.RTPForwardedPackets.Add(float64(cnt))
			r.mgr.metrics.RTPForwardedBytes.Add(float64(n * cnt))
			r.mgr.stats.AddForwardedBytes(n * cnt)
		}
	}
}

// attachScreenAudioSubscribers 为房内所有观看端补挂伴轨下行轨并重建 fanout。
func (r *Room) attachScreenAudioSubscribers(pub *Participant) {
	for _, sub := range r.snapshotParts() {
		if sub == pub {
			continue
		}
		r.ensureScreenAudioDownTrack(pub, sub)
	}
	r.rebuildScreenAudioFanout(pub)
}

// ensureScreenAudioDownTrack 确保 sub 持有来自 pub 的伴轨下行轨（订阅/退订跟随屏幕：
// 决策函数与屏幕轨相同）；返回是否新建。
func (r *Room) ensureScreenAudioDownTrack(pub, sub *Participant) bool {
	if sub.closed.Load() || !pub.ScreenAudioPublishing() {
		return false
	}
	if !ShouldForwardScreen(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid, KindScreenAudio)) {
		return false
	}
	key := TrackKey(pub.sid, KindScreenAudio)
	sub.mu.Lock()
	if _, ok := sub.screenDown[key]; ok {
		sub.mu.Unlock()
		return false
	}
	sub.mu.Unlock()

	local, err := webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, key, pub.uid)
	if err != nil {
		sub.log.Warn("create screen-audio down track failed", "err", err)
		return false
	}
	sender, err := sub.pc.AddTrack(local)
	if err != nil {
		sub.log.Warn("add screen-audio down track failed", "err", err)
		return false
	}
	go drainRTCP(sender)

	sub.mu.Lock()
	sub.screenDown[key] = &downTrack{pubUID: pub.uid, track: local, sender: sender, active: true}
	sub.mu.Unlock()
	sub.negotiate()
	return true
}

// detachScreenAudioPublisher 摘除 pub 的伴轨在所有观看端上的下行轨。
func (r *Room) detachScreenAudioPublisher(pub *Participant) {
	pub.screenAudioFanout.Store(nil)
	key := TrackKey(pub.sid, KindScreenAudio)
	for _, sub := range r.snapshotParts() {
		if sub == pub {
			continue
		}
		r.removeScreenDownTrack(key, sub)
	}
}

// rebuildScreenAudioFanout 重建伴轨下行快照（订阅/caps/成员变化后调用）。
func (r *Room) rebuildScreenAudioFanout(pub *Participant) {
	var list []*downTrack
	pubCaps := pub.Caps()
	key := TrackKey(pub.sid, KindScreenAudio)
	for _, sub := range r.snapshotParts() {
		if sub == pub || sub.closed.Load() {
			continue
		}
		sub.mu.Lock()
		dt := sub.screenDown[key]
		active := dt != nil && dt.active
		unsub := sub.hasUnsubscribedLocked(pub.uid, KindScreenAudio)
		sub.mu.Unlock()
		if active && ShouldForwardScreen(pubCaps, sub.Caps(), unsub) {
			list = append(list, dt)
		}
	}
	// 级联上行旁路轨随快照被 forwardScreenAudioLoop 写入
	if pubCaps.Has(auth.CapPublishScreen) {
		r.mgr.mu.RLock()
		list = append(list, r.cascadeSinks[key]...)
		r.mgr.mu.RUnlock()
	}
	pub.screenAudioFanout.Store(&list)
}
