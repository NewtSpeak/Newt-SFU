package cascade

// 双节点级联屏幕共享端到端验证（本专项验收锚点）：
//  1. A 节点发布者的屏幕轨（VP8，轨 id <sid>#screen）与系统音频伴轨
//     （<sid>#screen-audio）经级联边到达 B 节点观看端；
//  2. want 剪枝区分音频/屏幕：B 节点无 subscribe_audio 听众 → 麦克风轨被剪、
//     屏幕轨照常转发；B 观看端退订 → 屏幕/伴轨一并剪枝，重订阅恢复；
//  3. PLI 关键帧请求跨级联回传：B 观看端就位后，A 节点发布客户端收到 PLI；
//  4. 全程无环路丢包（epoch/租约门控与音频共用同一 countingSink 路径）。

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus/testutil"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
	"github.com/owlspeak/owl-sfu/internal/auth"
	"github.com/owlspeak/owl-sfu/internal/room"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// screenE2EClient 为屏幕共享级联 e2e 客户端：可携带音频/视频/伴轨上行，
// 按下行轨 id 统计收包，统计视频 sender 收到的 PLI，记录轨事件。
type screenE2EClient struct {
	t    *testing.T
	pc   *webrtc.PeerConnection
	part *room.Participant

	audioTrack     *webrtc.TrackLocalStaticRTP
	videoTrack     *webrtc.TrackLocalStaticRTP
	companionTrack *webrtc.TrackLocalStaticRTP

	cntMu    sync.Mutex
	recvByID map[string]*atomic.Int64

	evMu   sync.Mutex
	events []string // "track_published:screen" 等

	pliRecv atomic.Int64

	sigMu            sync.Mutex
	connected        chan struct{}
	connOnce         sync.Once
	companionStarted atomic.Bool
	stop             chan struct{}
	stopOnce         sync.Once
}

func (c *screenE2EClient) idCounter(id string) *atomic.Int64 {
	c.cntMu.Lock()
	defer c.cntMu.Unlock()
	if c.recvByID == nil {
		c.recvByID = make(map[string]*atomic.Int64)
	}
	ctr, ok := c.recvByID[id]
	if !ok {
		ctr = &atomic.Int64{}
		c.recvByID[id] = ctr
	}
	return ctr
}

func (c *screenE2EClient) recvForID(id string) int64 { return c.idCounter(id).Load() }

// startCompanion 开启伴轨发包（主麦克风轨先行，保证到达顺序确定）。
func (c *screenE2EClient) startCompanion() { c.companionStarted.Store(true) }

func (c *screenE2EClient) eventCount(name string) int {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	n := 0
	for _, e := range c.events {
		if e == name {
			n++
		}
	}
	return n
}

// Send 实现 room.Messenger：拦截 SFU → 客户端信令帧。
func (c *screenE2EClient) Send(op string, d any) error {
	m, _ := d.(map[string]any)
	switch op {
	case "offer": // SFU renegotiation
		sdp, _ := m["sdp"].(string)
		go c.answerSFUOffer(sdp)
	case "ice":
		cand, _ := m["candidate"].(string)
		mid, _ := m["sdp_mid"].(*string)
		mline, _ := m["sdp_mline_index"].(*uint16)
		if cand != "" {
			_ = c.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: cand, SDPMid: mid, SDPMLineIndex: mline})
		}
	case "track_published", "track_ended":
		kind, _ := m["kind"].(string)
		c.evMu.Lock()
		c.events = append(c.events, op+":"+kind)
		c.evMu.Unlock()
	}
	return nil
}

func (c *screenE2EClient) CloseWithReason(string, string) {}

func (c *screenE2EClient) answerSFUOffer(sdp string) {
	c.sigMu.Lock()
	defer c.sigMu.Unlock()
	if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sdp}); err != nil {
		c.t.Logf("client set remote offer: %v", err)
		return
	}
	answer, err := c.pc.CreateAnswer(nil)
	if err == nil {
		err = c.pc.SetLocalDescription(answer)
	}
	if err != nil {
		c.t.Logf("client answer: %v", err)
		return
	}
	if err := c.part.HandleAnswer(answer.SDP); err != nil {
		c.t.Logf("sfu handle answer: %v", err)
	}
}

func (c *screenE2EClient) close() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.pc.Close()
}

// joinScreenClient 入房并完成首次协商。publish=true 时携带音频 + 视频（屏幕）+
// 伴轨上行；false 时为纯观看端（recvonly 音视频 transceiver）。
func joinScreenClient(t *testing.T, node *e2eNode, uid, sid, rid string, capList []string, publish bool) *screenE2EClient {
	t.Helper()
	me, err := owlsfu.NewMediaEngine()
	if err != nil {
		t.Fatal(err)
	}
	ir := &interceptor.Registry{}
	if err := webrtc.ConfigureRTCPReports(ir); err != nil {
		t.Fatal(err)
	}
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir)).
		NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	c := &screenE2EClient{t: t, pc: pc, connected: make(chan struct{}), stop: make(chan struct{})}

	drain := func(sender *webrtc.RTPSender) {
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := sender.Read(buf); err != nil {
					return
				}
			}
		}()
	}
	if publish {
		c.audioTrack, err = webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "audio", uid)
		if err != nil {
			t.Fatal(err)
		}
		as, err := pc.AddTrack(c.audioTrack)
		if err != nil {
			t.Fatal(err)
		}
		drain(as)

		c.companionTrack, err = webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "companion", uid)
		if err != nil {
			t.Fatal(err)
		}
		cs, err := pc.AddTrack(c.companionTrack)
		if err != nil {
			t.Fatal(err)
		}
		drain(cs)

		c.videoTrack, err = webrtc.NewTrackLocalStaticRTP(owlsfu.VP8Codec, "screen", uid)
		if err != nil {
			t.Fatal(err)
		}
		vs, err := pc.AddTrack(c.videoTrack)
		if err != nil {
			t.Fatal(err)
		}
		// 视频 sender 的 RTCP：统计 SFU 回传的 PLI（级联关键帧回传验收点）
		go func() {
			for {
				pkts, _, err := vs.ReadRTCP()
				if err != nil {
					return
				}
				for _, pkt := range pkts {
					if _, ok := pkt.(*rtcp.PictureLossIndication); ok {
						c.pliRecv.Add(1)
					}
				}
			}
		}()
	} else {
		for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeAudio, webrtc.RTPCodecTypeVideo} {
			if _, err := pc.AddTransceiverFromKind(kind,
				webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				t.Fatal(err)
			}
		}
	}

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		ctr := c.idCounter(remote.ID())
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := remote.Read(buf); err != nil {
					return
				}
				ctr.Add(1)
			}
		}()
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			c.connOnce.Do(func() { close(c.connected) })
		}
	})

	tok := &auth.Token{
		UID: uid, RID: rid, SID: sid, NID: node.id,
		Caps:      auth.NewCapSet(capList),
		ExpiresAt: time.Now().Add(time.Minute),
	}
	part, _, err := node.mgr.Join(tok, c)
	if err != nil {
		t.Fatal(err)
	}
	c.part = part

	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand == nil {
			return
		}
		init := cand.ToJSON()
		_ = part.AddICECandidate(init.Candidate, init.SDPMid, init.SDPMLineIndex)
	})

	c.sigMu.Lock()
	offer, err := pc.CreateOffer(nil)
	if err == nil {
		err = pc.SetLocalDescription(offer)
	}
	if err != nil {
		c.sigMu.Unlock()
		t.Fatal(err)
	}
	answer, err := part.HandleOffer(offer.SDP)
	if err != nil {
		c.sigMu.Unlock()
		t.Fatal(err)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		c.sigMu.Unlock()
		t.Fatal(err)
	}
	c.sigMu.Unlock()
	return c
}

// sendMediaLoop 连通后发送模拟音频 + VP8；伴轨在 startCompanion 后才发包
// （主麦克风轨先到达，保证「第二条 audio 轨 = 伴轨」的分类确定，BA.4）。
func (c *screenE2EClient) sendMediaLoop() {
	<-c.connected
	seq, ts := uint16(0), uint32(0)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
		}
		_ = c.audioTrack.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts},
			Payload: []byte{0xde, 0xad, 0xbe, 0xef},
		})
		if c.companionStarted.Load() {
			_ = c.companionTrack.WriteRTP(&rtp.Packet{
				Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts},
				Payload: []byte{0xca, 0xfe, 0xba, 0xbe},
			})
		}
		_ = c.videoTrack.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts * 90 / 48, Marker: true},
			Payload: []byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
		})
		seq++
		ts += 960
	}
}

// TestE2ECascadeScreenShare 双节点级联屏幕共享主链路 + 分 kind 剪枝 + PLI 回传。
func TestE2ECascadeScreenShare(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cascade e2e in -short mode")
	}
	const roomID = "lr-screen"
	ca := newTestCA(t)
	nodeA := newE2ENode(t, ca, "node-a") // anchor / parent，发布者所在节点
	nodeB := newE2ENode(t, ca, "node-b") // child，观看端所在节点

	nodeA.mgr.EnsureRoom(roomID)
	nodeB.mgr.EnsureRoom(roomID)

	expire := time.Now().Add(2 * time.Minute).UnixMilli()
	for _, n := range []*e2eNode{nodeA, nodeB} {
		if err := n.casc.SetAnchorLease(roomID, "node-a", 1, expire); err != nil {
			t.Fatal(err)
		}
	}
	edges := []*owlsfuv1.CascadeEdge{{
		ParentNodeId:          "node-a",
		ChildNodeId:           "node-b",
		ParentCascadeEndpoint: nodeA.casc.Addr(),
		CascadeToken:          "tok-screen",
	}}
	if err := nodeA.casc.SetCascadeEdges(roomID, 1, edges); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.casc.SetCascadeEdges(roomID, 1, edges); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "edge up on both nodes", func() bool {
		return testutil.ToFloat64(nodeA.metrics.CascadeEdges) == 1 &&
			testutil.ToFloat64(nodeB.metrics.CascadeEdges) == 1
	})

	// A 节点发布者（音频 + 屏幕 + 伴轨）；B 节点观看端仅持 join
	//（无 subscribe_audio → 音频 want 为空，屏幕 want 全订：剪枝必须分 kind）
	pub := joinScreenClient(t, nodeA, "user-a", "sid-a", roomID,
		[]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen}, true)
	defer pub.close()
	go pub.sendMediaLoop()

	viewer := joinScreenClient(t, nodeB, "user-b", "sid-b", roomID,
		[]string{auth.CapJoin}, false)
	defer viewer.close()

	screenKey := room.TrackKey("sid-a", room.KindScreen)
	companionKey := room.TrackKey("sid-a", room.KindScreenAudio)

	// 主链路：屏幕轨跨节点到达观看端
	waitFor(t, 20*time.Second, "viewer on node-b receives screen RTP", func() bool {
		return viewer.recvForID(screenKey) > 10
	})
	if n := viewer.eventCount("track_published:screen"); n < 1 {
		t.Fatalf("观看端应收到 track_published(kind=screen)，got %d", n)
	}

	// 伴轨（主麦克风轨已先到达，此刻开启伴轨发包）
	pub.startCompanion()
	waitFor(t, 20*time.Second, "viewer receives companion audio RTP", func() bool {
		return viewer.recvForID(companionKey) > 10
	})
	if n := viewer.eventCount("track_published:screen_audio"); n < 1 {
		t.Fatalf("观看端应收到 track_published(kind=screen_audio)，got %d", n)
	}

	// 剪枝分 kind：B 节点无 subscribe_audio 听众 → 麦克风轨不得跨节点转发
	if got := viewer.recvForID("sid-a"); got != 0 {
		t.Fatalf("B 节点无音频听众，麦克风轨不应跨节点转发，got %d packets", got)
	}
	activeGauge := func(n *e2eNode) float64 {
		return testutil.ToFloat64(n.metrics.CascadeOutboundTracks.WithLabelValues("active"))
	}
	waitFor(t, 5*time.Second, "node-a forwards exactly screen+companion", func() bool {
		return activeGauge(nodeA) == 2
	})

	// PLI 跨级联回传：B 观看端就位触发的关键帧请求最终到达 A 发布客户端
	waitFor(t, 10*time.Second, "publisher receives PLI via cascade", func() bool {
		return pub.pliRecv.Load() >= 1
	})

	// 观看端退订 → 屏幕/伴轨一并剪枝
	viewer.part.Unsubscribe("user-a")
	waitFor(t, 10*time.Second, "node-a prunes screen tracks after unsubscribe", func() bool {
		return activeGauge(nodeA) == 0
	})
	settled := viewer.recvForID(screenKey)
	time.Sleep(time.Second)
	if got := viewer.recvForID(screenKey); got != settled {
		t.Fatalf("剪枝后屏幕轨仍在转发：%d → %d", settled, got)
	}

	// 重订阅 → 恢复
	viewer.part.Subscribe("user-a")
	waitFor(t, 10*time.Second, "node-a restores screen tracks after resubscribe", func() bool {
		return activeGauge(nodeA) == 2
	})
	resumeBase := viewer.recvForID(screenKey)
	waitFor(t, 20*time.Second, "screen forwarding resumes", func() bool {
		return viewer.recvForID(screenKey) > resumeBase+10
	})

	// 全程无环路丢包
	if n := testutil.ToFloat64(nodeA.metrics.CascadeLoopDropped) + testutil.ToFloat64(nodeB.metrics.CascadeLoopDropped); n != 0 {
		t.Fatalf("不应有环路丢包，got %v", n)
	}
}
