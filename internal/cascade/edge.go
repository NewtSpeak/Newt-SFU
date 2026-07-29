package cascade

import (
	"encoding/json"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
	"github.com/newtspeak/newt-sfu/internal/room"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
)

const (
	pingInterval  = 5 * time.Second
	readIdleLimit = 25 * time.Second // 连续无帧（含 pong）即判边断
)

// outTrack 为一条出向轨（本节点 → 对端；音频 / 屏幕 / 系统音频伴轨）。
type outTrack struct {
	key    string // trackKey = room.TrackKey(源 sid, kind)，同时是轨 id
	uid    string
	kind   string
	local  bool // 源为本地发布者（true）或另一条边转发（false）
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	sink   *countingSink // 供写入方（room fanout 或级联读循环）使用
	// detach 本地 speaker 的 room 旁路卸载函数（远端源为 nil）
	detach func()
}

// countingSink 包装出向轨：租约/epoch 门控 + 每边 tx 指标计数。
// 实现 room.RTPWriter，可直接注入 room fanout。
type countingSink struct {
	track   *webrtc.TrackLocalStaticRTP
	active  *atomic.Bool // 指向所属边的转发授权开关
	bytes   prometheus.Counter
	packets prometheus.Counter
	// total 可选：累计字节（供 EdgeStatus 拓扑上报读回；nil 时仅 prometheus）。
	total *atomic.Uint64
}

// WriteRTP 门控写入：边未授权（租约过期/epoch 不匹配/未建立）时静默丢弃。
func (cs *countingSink) WriteRTP(p *rtp.Packet) error {
	if !cs.active.Load() {
		return nil
	}
	if err := cs.track.WriteRTP(p); err != nil {
		return err
	}
	n := uint64(p.MarshalSize())
	cs.packets.Inc()
	cs.bytes.Add(float64(n))
	if cs.total != nil {
		cs.total.Add(n)
	}
	return nil
}

// edgeSession 为一条级联边的会话：mTLS 信令连接 + 两个单向媒体 PC。
type edgeSession struct {
	mgr      *Manager
	log      *slog.Logger
	edge     Edge
	isParent bool // 本节点为该边的 parent
	conn     net.Conn

	wmu sync.Mutex
	enc *json.Encoder
	// handshakeDec child 侧握手期创建的 decoder（可能缓冲了后续帧，runWith 必须复用）
	handshakeDec *json.Decoder

	established atomic.Bool // 握手完成
	closed      atomic.Bool
	// active：跨节点转发授权（established && lease epoch 匹配未过期），
	// 由 Manager.recompute/watchdog 维护；读循环与 countingSink 无锁读。
	active atomic.Bool

	// 以下字段由 Manager.mu 保护
	peerWant       WantSet // 对端通报的音频需求
	peerScreenWant WantSet // 对端通报的屏幕轨（含伴轨）需求
	peerWantSeen   bool    // 是否已收到过 want（未收到前不发任何轨）
	myWant         WantSet // 我方上次发送的需求（去重）
	myScreenWant   WantSet
	myWantSent     bool
	outTracks      map[string]*outTrack // trackKey → 出向轨

	// PC 协商状态
	pcMu        sync.Mutex
	sendPC      *webrtc.PeerConnection // 我方为 offerer（发送方向）
	recvPC      *webrtc.PeerConnection // 对端为 offerer（接收方向）
	negMu       sync.Mutex
	negotiating bool
	negPending  bool

	rttMs atomic.Uint64 // math.Float64bits

	// 累计 RTP 字节（本端视角；EdgeStatus 上报给 Server 做拓扑流量差分）
	totalTxBytes atomic.Uint64
	totalRxBytes atomic.Uint64

	// 媒体选中路径快照（ICE selected pair；path.go 解析）
	pathMu     sync.Mutex
	pathType   owlsfuv1.EdgeStatus_PathType
	localCand  string
	remoteCand string

	// 预解析的每边指标（热路径避免 WithLabelValues 查表）
	txBytes, txPackets, rxBytes, rxPackets prometheus.Counter
}

// newEdgeSession 构建边会话（conn 已完成 mTLS 与 hello 握手校验）。
func newEdgeSession(mgr *Manager, edge Edge, isParent bool, conn net.Conn) *edgeSession {
	peer := edge.ChildNodeID
	if !isParent {
		peer = edge.ParentNodeID
	}
	s := &edgeSession{
		mgr:       mgr,
		log:       mgr.log.With("room", edge.RoomID, "edge", edge.key(), "peer", peer),
		edge:      edge,
		isParent:  isParent,
		conn:      conn,
		enc:       json.NewEncoder(conn),
		outTracks: make(map[string]*outTrack),
		txBytes:   mgr.metrics.CascadeEdgeBytes.WithLabelValues(edge.RoomID, peer, "tx"),
		txPackets: mgr.metrics.CascadeEdgePackets.WithLabelValues(edge.RoomID, peer, "tx"),
		rxBytes:   mgr.metrics.CascadeEdgeBytes.WithLabelValues(edge.RoomID, peer, "rx"),
		rxPackets: mgr.metrics.CascadeEdgePackets.WithLabelValues(edge.RoomID, peer, "rx"),
	}
	return s
}

// peerNodeID 返回对端 node_id。
func (s *edgeSession) peerNodeID() string {
	if s.isParent {
		return s.edge.ChildNodeID
	}
	return s.edge.ParentNodeID
}

// sendDir 返回我方作为 offerer 的媒体方向标签。
func (s *edgeSession) sendDir() string {
	if s.isParent {
		return dirDown
	}
	return dirUp
}

// send 序列化一帧（并发安全）。
func (s *edgeSession) send(f *frame) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	return s.enc.Encode(f)
}

// runWith 读帧循环 + ping 保活；退出即视为边断（EdgeDown）。
// dec 必须是握手期在同一连接上创建的 decoder（缓冲连续性）。
func (s *edgeSession) runWith(dec *json.Decoder) {
	go s.pingLoop()
	for {
		_ = s.conn.SetReadDeadline(time.Now().Add(readIdleLimit))
		var f frame
		if err := dec.Decode(&f); err != nil {
			s.close("read: " + err.Error())
			return
		}
		s.handleFrame(&f)
	}
}

// pingLoop 周期发 ping 测 RTT（EdgeStatus 的 rtt_ms 来源）。
func (s *edgeSession) pingLoop() {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for range t.C {
		if s.closed.Load() {
			return
		}
		if err := s.send(&frame{T: framePing, TS: time.Now().UnixNano()}); err != nil {
			return
		}
	}
}

// handleFrame 分发一帧。
func (s *edgeSession) handleFrame(f *frame) {
	switch f.T {
	case framePing:
		_ = s.send(&frame{T: framePong, TS: f.TS})
	case framePong:
		rtt := float64(time.Now().UnixNano()-f.TS) / 1e6
		if rtt >= 0 {
			s.rttMs.Store(math.Float64bits(rtt))
			s.mgr.metrics.CascadeEdgeRTT.WithLabelValues(s.edge.RoomID, s.peerNodeID()).Set(rtt)
		}
	case frameWant:
		if f.Want != nil || f.ScreenWant != nil {
			audio, screen := WantNone(), WantNone()
			if f.Want != nil {
				audio = wantFromWire(*f.Want)
			}
			// 对端未携带 screen_want（旧版本节点）→ 无屏幕需求（安全缺省，不送屏幕轨）
			if f.ScreenWant != nil {
				screen = wantFromWire(*f.ScreenWant)
			}
			s.mgr.onPeerWant(s, audio, screen)
		}
	case frameOffer:
		if f.Dir != s.sendDir() { // 对端 offer 只可能属于对端的发送方向
			s.handleRemoteOffer(f.SDP)
		} else {
			s.log.Warn("offer with unexpected dir ignored", "dir", f.Dir)
		}
	case frameAnswer:
		if f.Dir == s.sendDir() {
			s.handleAnswer(f.SDP)
		} else {
			s.log.Warn("answer with unexpected dir ignored", "dir", f.Dir)
		}
	case frameICE:
		s.handleICE(f)
	default:
		s.log.Debug("unknown cascade frame ignored", "t", f.T)
	}
}

// RTT 返回最近一次探测的 RTT（毫秒，未测得为 0）。
func (s *edgeSession) RTT() float64 { return math.Float64frombits(s.rttMs.Load()) }

// trafficSnapshot 返回本端累计 RTP 字节与选中路径。
func (s *edgeSession) trafficSnapshot() (tx, rx uint64, path owlsfuv1.EdgeStatus_PathType, localIP, remoteIP string) {
	s.refreshSelectedPath()
	s.pathMu.Lock()
	defer s.pathMu.Unlock()
	return s.totalTxBytes.Load(), s.totalRxBytes.Load(), s.pathType, s.localCand, s.remoteCand
}

// refreshSelectedPath 从发送/接收 PC 的 ICE selected pair 推断内网/外网。
func (s *edgeSession) refreshSelectedPath() {
	s.pcMu.Lock()
	pcs := []*webrtc.PeerConnection{s.sendPC, s.recvPC}
	s.pcMu.Unlock()
	for _, pc := range pcs {
		if pc == nil {
			continue
		}
		localIP, remoteIP, ok := selectedPairIPs(pc)
		if !ok {
			continue
		}
		pt := classifyPath(localIP, remoteIP)
		s.pathMu.Lock()
		s.localCand = localIP
		s.remoteCand = remoteIP
		s.pathType = pt
		s.pathMu.Unlock()
		return
	}
	// ICE 尚未选出时：用级联信令 TCP 对端地址做粗判（信令与媒体通常同路径偏好）
	if s.conn != nil {
		if ra, ok := s.conn.RemoteAddr().(*net.TCPAddr); ok && ra.IP != nil {
			remoteIP := ra.IP.String()
			localIP := ""
			if la, ok := s.conn.LocalAddr().(*net.TCPAddr); ok && la.IP != nil {
				localIP = la.IP.String()
			}
			pt := classifyPath(localIP, remoteIP)
			s.pathMu.Lock()
			if s.localCand == "" {
				s.localCand = localIP
			}
			if s.remoteCand == "" {
				s.remoteCand = remoteIP
			}
			if s.pathType == owlsfuv1.EdgeStatus_PATH_TYPE_UNSPECIFIED {
				s.pathType = pt
			}
			s.pathMu.Unlock()
		}
	}
}

// ---- 媒体 PC ----

// ensureSendPC 惰性创建发送方向 PC（我方 offerer）。
func (s *edgeSession) ensureSendPC() (*webrtc.PeerConnection, error) {
	s.pcMu.Lock()
	defer s.pcMu.Unlock()
	if s.sendPC != nil {
		return s.sendPC, nil
	}
	// 级联专用 PC 工厂：独立 ephemeral 端口，不共用客户端 UDPMux
	//（同一对节点多条 PC 在双端单口 mux 下四元组相同会互抢路由，见 engine.go）。
	pc, err := s.mgr.engine.NewCascadePeerConnection()
	if err != nil {
		return nil, err
	}
	dir := s.sendDir()
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || s.closed.Load() {
			return
		}
		init := c.ToJSON()
		_ = s.send(&frame{T: frameICE, Dir: dir, Candidate: init.Candidate,
			SDPMid: init.SDPMid, SDPMLine: init.SDPMLineIndex})
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		if st == webrtc.PeerConnectionStateFailed {
			s.close("send pc failed")
		}
	})
	s.sendPC = pc
	return pc, nil
}

// ensureRecvPC 惰性创建接收方向 PC（对端 offerer）；OnTrack 进入远端 speaker 接入。
func (s *edgeSession) ensureRecvPC() (*webrtc.PeerConnection, error) {
	s.pcMu.Lock()
	defer s.pcMu.Unlock()
	if s.recvPC != nil {
		return s.recvPC, nil
	}
	pc, err := s.mgr.engine.NewCascadePeerConnection()
	if err != nil {
		return nil, err
	}
	peerDir := dirUp
	if !s.isParent {
		peerDir = dirDown
	}
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || s.closed.Load() {
			return
		}
		init := c.ToJSON()
		_ = s.send(&frame{T: frameICE, Dir: peerDir, Candidate: init.Candidate,
			SDPMid: init.SDPMid, SDPMLine: init.SDPMLineIndex})
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		if st == webrtc.PeerConnectionStateFailed {
			s.close("recv pc failed")
		}
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		s.mgr.onRemoteTrack(s, track)
	})
	s.recvPC = pc
	return pc, nil
}

// negotiateSend 发送方向 renegotiation（我方 offerer；在途时置 pending）。
func (s *edgeSession) negotiateSend() {
	if s.closed.Load() {
		return
	}
	s.negMu.Lock()
	if s.negotiating {
		s.negPending = true
		s.negMu.Unlock()
		return
	}
	s.negotiating = true
	s.negMu.Unlock()
	go s.doNegotiateSend()
}

func (s *edgeSession) doNegotiateSend() {
	pc, err := s.ensureSendPC()
	if err != nil {
		s.log.Warn("cascade send pc create failed", "err", err)
		return
	}
	s.pcMu.Lock()
	offer, err := pc.CreateOffer(nil)
	if err == nil {
		err = pc.SetLocalDescription(offer)
	}
	s.pcMu.Unlock()
	if err != nil {
		s.log.Warn("cascade offer failed", "err", err)
		s.negMu.Lock()
		s.negotiating = false
		s.negMu.Unlock()
		return
	}
	if err := s.send(&frame{T: frameOffer, Dir: s.sendDir(), SDP: offer.SDP}); err != nil {
		s.close("send offer: " + err.Error())
	}
}

// handleAnswer 应用对端对我方 offer 的应答；有 pending 则补协商。
func (s *edgeSession) handleAnswer(sdp string) {
	s.pcMu.Lock()
	pc := s.sendPC
	var err error
	if pc != nil {
		err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sdp})
	}
	s.pcMu.Unlock()
	if err != nil {
		s.log.Warn("cascade set answer failed", "err", err)
	}
	s.negMu.Lock()
	s.negotiating = false
	pending := s.negPending
	s.negPending = false
	s.negMu.Unlock()
	if pending {
		s.negotiateSend()
	}
}

// handleRemoteOffer 处理对端发送方向的 offer，回 answer。
func (s *edgeSession) handleRemoteOffer(sdp string) {
	pc, err := s.ensureRecvPC()
	if err != nil {
		s.log.Warn("cascade recv pc create failed", "err", err)
		return
	}
	peerDir := dirUp
	if !s.isParent {
		peerDir = dirDown
	}
	s.pcMu.Lock()
	err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp})
	var answer webrtc.SessionDescription
	if err == nil {
		answer, err = pc.CreateAnswer(nil)
	}
	if err == nil {
		err = pc.SetLocalDescription(answer)
	}
	s.pcMu.Unlock()
	if err != nil {
		s.log.Warn("cascade handle remote offer failed", "err", err)
		return
	}
	if err := s.send(&frame{T: frameAnswer, Dir: peerDir, SDP: answer.SDP}); err != nil {
		s.close("send answer: " + err.Error())
	}
}

// handleICE 按 dir 路由 trickle candidate：dir == 我方发送方向 → sendPC，否则 recvPC。
func (s *edgeSession) handleICE(f *frame) {
	if f.Candidate == "" {
		return
	}
	var pc *webrtc.PeerConnection
	var err error
	if f.Dir == s.sendDir() {
		s.pcMu.Lock()
		pc = s.sendPC
		s.pcMu.Unlock()
	} else {
		// 对端发送方向的 candidate 属于我方 recvPC（可能先于 offer 到达，需先建 PC）
		pc, err = s.ensureRecvPC()
		if err != nil {
			return
		}
	}
	if pc == nil {
		return
	}
	if err := pc.AddICECandidate(webrtc.ICECandidateInit{
		Candidate: f.Candidate, SDPMid: f.SDPMid, SDPMLineIndex: f.SDPMLine,
	}); err != nil {
		s.log.Debug("cascade add ice failed", "err", err)
	}
}

// ---- 出向轨管理（Manager.mu 下调用）----

// addOutTrackLocked 为一条轨建出向轨；local 表示源为本地发布者。
// 返回是否需要 renegotiation。
func (s *edgeSession) addOutTrackLocked(key, uid, kind string, local bool) bool {
	if _, ok := s.outTracks[key]; ok {
		return false
	}
	pc, err := s.ensureSendPC()
	if err != nil {
		s.log.Warn("cascade send pc create failed", "err", err)
		return false
	}
	codec := owlsfu.OpusCodec
	if kind == room.KindScreen {
		codec = owlsfu.VP8Codec
	}
	// track id = trackKey（<sid> / <sid>#screen / <sid>#screen-audio）、
	// streamID = 源 uid：对端经 msid 还原 speaker 身份与 kind。
	track, err := webrtc.NewTrackLocalStaticRTP(codec, key, uid)
	if err != nil {
		s.log.Warn("cascade create out track failed", "err", err)
		return false
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		s.log.Warn("cascade add out track failed", "err", err)
		return false
	}
	if kind == room.KindScreen {
		// 屏幕轨 RTCP 不只排空：对端观看侧的 PLI/FIR 沿级联继续回传到发布节点。
		go s.forwardOutTrackRTCP(sender, key)
	} else {
		go drainRTCP(sender)
	}

	ot := &outTrack{
		key: key, uid: uid, kind: kind, local: local, track: track, sender: sender,
		sink: &countingSink{track: track, active: &s.active, bytes: s.txBytes, packets: s.txPackets, total: &s.totalTxBytes},
	}
	if local {
		detach, err := s.mgr.rooms.AttachCascadeSink(s.edge.RoomID, key, ot.sink)
		if err != nil {
			s.log.Warn("attach cascade sink failed", "key", key, "err", err)
			_ = pc.RemoveTrack(sender)
			return false
		}
		ot.detach = detach
	}
	s.outTracks[key] = ot
	s.log.Info("cascade out track added", "key", key, "uid", uid, "kind", kind, "local", local)
	return true
}

// forwardOutTrackRTCP 读取对端对屏幕出向轨的 RTCP，PLI/FIR 触发向源方向的关键帧回传
// （源为本地发布者 → room 层转发发布客户端；源为另一条边 → 继续沿级联传播）。
func (s *edgeSession) forwardOutTrackRTCP(sender *webrtc.RTPSender, key string) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				s.mgr.onKeyframeRequest(s.edge.RoomID, key)
			}
		}
	}
}

// removeOutTrackLocked 摘除出向轨；返回是否需要 renegotiation。
func (s *edgeSession) removeOutTrackLocked(key string) bool {
	ot, ok := s.outTracks[key]
	if !ok {
		return false
	}
	delete(s.outTracks, key)
	if ot.detach != nil {
		ot.detach()
	}
	s.pcMu.Lock()
	pc := s.sendPC
	s.pcMu.Unlock()
	if pc != nil {
		if err := pc.RemoveTrack(ot.sender); err != nil {
			s.log.Debug("cascade remove out track", "err", err)
		}
	}
	s.log.Info("cascade out track removed", "key", key, "uid", ot.uid, "kind", ot.kind)
	return true
}

// close 幂等关闭边会话并通知 Manager（EdgeDown、清远端 speaker、child 侧重拨）。
func (s *edgeSession) close(reason string) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.active.Store(false)
	s.log.Info("cascade edge closed", "reason", reason)
	_ = s.conn.Close()
	s.pcMu.Lock()
	sendPC, recvPC := s.sendPC, s.recvPC
	s.pcMu.Unlock()
	if sendPC != nil {
		_ = sendPC.Close()
	}
	if recvPC != nil {
		_ = recvPC.Close()
	}
	s.mgr.onEdgeClosed(s)
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
