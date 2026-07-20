package signal

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/owlspeak/owl-sfu/internal/auth"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// signTokenCaps 与 signToken 相同但可指定 caps（屏幕共享用例需要 publish_screen）。
func (e *testEnv) signTokenCaps(t *testing.T, uid, sid, rid string, ttl time.Duration, caps []string) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"v": 1, "uid": uid, "gid": "g1", "cid": "c1",
		"nid": e2eNodeID, "rid": rid, "sid": sid,
		"caps": caps,
		"iat":  now.Unix(), "exp": now.Add(ttl).Unix(), "jti": "jti-" + sid,
	})
	tok.Header["kid"] = "k1"
	raw, err := tok.SignedString(e.priv)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// screenClient 为屏幕共享 e2e 客户端：可选携带 video 轨与系统音频伴轨，
// 分 kind / track id / payload 首字节统计下行 RTP，记录轨事件。
type screenClient struct {
	t    *testing.T
	wsMu sync.Mutex
	ws   *websocket.Conn
	pc   *webrtc.PeerConnection

	audioTrack      *webrtc.TrackLocalStaticRTP
	companionTracks []*webrtc.TrackLocalStaticRTP // 系统音频伴轨（BA.4，默认不发包，见 startCompanion）
	videoTracks     []*webrtc.TrackLocalStaticRTP

	recvAudio atomic.Int64
	recvVideo atomic.Int64

	cntMu       sync.Mutex
	recvByID    map[string]*atomic.Int64 // 下行轨 id → 包数
	recvByFirst map[byte]*atomic.Int64   // 视频 payload 首字节 → 包数（simulcast 层标记）

	evMu   sync.Mutex
	events []string // "track_published:screen" / "track_ended:screen" / ...

	capsUpdated      atomic.Int64
	companionStarted atomic.Bool

	ready     chan struct{}
	connected chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
}

// idCounter 取（或建）某下行轨 id 的计数器。
func (c *screenClient) idCounter(id string) *atomic.Int64 {
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

// recvForID 返回某下行轨 id 已收包数。
func (c *screenClient) recvForID(id string) int64 { return c.idCounter(id).Load() }

// firstByteCounter 取（或建）视频 payload 首字节计数器。
func (c *screenClient) firstByteCounter(b byte) *atomic.Int64 {
	c.cntMu.Lock()
	defer c.cntMu.Unlock()
	if c.recvByFirst == nil {
		c.recvByFirst = make(map[byte]*atomic.Int64)
	}
	ctr, ok := c.recvByFirst[b]
	if !ok {
		ctr = &atomic.Int64{}
		c.recvByFirst[b] = ctr
	}
	return ctr
}

// startCompanion 开始发送系统音频伴轨（主麦克风轨先行发包，保证到达顺序确定）。
func (c *screenClient) startCompanion() { c.companionStarted.Store(true) }

func (c *screenClient) send(op string, d any) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	c.ws.WriteJSON(map[string]any{"op": op, "d": d})
}

func (c *screenClient) close() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.pc.Close()
	c.ws.Close()
}

func (c *screenClient) eventCount(name string) int {
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

// dialScreenClient videoTracks 指定携带的上行 video 轨数（0 = 纯音频客户端）。
func dialScreenClient(t *testing.T, env *testEnv, token string, videoTracks int) *screenClient {
	t.Helper()
	return dialScreenClientOpts(t, env, token, videoTracks, 0)
}

// dialScreenClientOpts 额外指定系统音频伴轨数（同会话第二条 audio 轨，BA.4）。
func dialScreenClientOpts(t *testing.T, env *testEnv, token string, videoTracks, companionTracks int) *screenClient {
	t.Helper()
	ws, _, err := websocket.DefaultDialer.Dial(env.wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
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

	c := &screenClient{
		t: t, ws: ws, pc: pc,
		ready: make(chan struct{}), connected: make(chan struct{}), stop: make(chan struct{}),
	}

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

	c.audioTrack, err = webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "audio", "cli")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := pc.AddTrack(c.audioTrack)
	if err != nil {
		t.Fatal(err)
	}
	drain(sender)

	for i := 0; i < companionTracks; i++ {
		ct, err := webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "companion", "cli")
		if err != nil {
			t.Fatal(err)
		}
		cs, err := pc.AddTrack(ct)
		if err != nil {
			t.Fatal(err)
		}
		drain(cs)
		c.companionTracks = append(c.companionTracks, ct)
	}

	for i := 0; i < videoTracks; i++ {
		vt, err := webrtc.NewTrackLocalStaticRTP(owlsfu.VP8Codec, "screen", "cli")
		if err != nil {
			t.Fatal(err)
		}
		vs, err := pc.AddTrack(vt)
		if err != nil {
			t.Fatal(err)
		}
		drain(vs)
		c.videoTracks = append(c.videoTracks, vt)
	}

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		kind := remote.Kind()
		ctr := c.idCounter(remote.ID())
		go func() {
			for {
				pkt, _, err := remote.ReadRTP()
				if err != nil {
					return
				}
				ctr.Add(1)
				if kind == webrtc.RTPCodecTypeVideo {
					c.recvVideo.Add(1)
					if len(pkt.Payload) > 0 {
						c.firstByteCounter(pkt.Payload[0]).Add(1)
					}
				} else {
					c.recvAudio.Add(1)
				}
			}
		}()
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case <-c.connected:
			default:
				close(c.connected)
			}
		}
	})
	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand == nil {
			return
		}
		init := cand.ToJSON()
		c.send("ice", map[string]any{
			"candidate": init.Candidate, "sdp_mid": init.SDPMid, "sdp_mline_index": init.SDPMLineIndex,
		})
	})

	go c.readLoop()
	c.send("auth", map[string]any{"token": token})
	return c
}

func (c *screenClient) readLoop() {
	for {
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var f struct {
			Op string          `json:"op"`
			D  json.RawMessage `json:"d"`
		}
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		switch f.Op {
		case "ready":
			offer, err := c.pc.CreateOffer(nil)
			if err == nil {
				err = c.pc.SetLocalDescription(offer)
			}
			if err != nil {
				c.t.Logf("screen client offer failed: %v", err)
				return
			}
			c.send("offer", map[string]any{"sdp": offer.SDP})
			close(c.ready)
		case "answer":
			var d struct {
				SDP string `json:"sdp"`
			}
			json.Unmarshal(f.D, &d)
			c.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: d.SDP})
		case "offer":
			var d struct {
				SDP string `json:"sdp"`
			}
			json.Unmarshal(f.D, &d)
			if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: d.SDP}); err != nil {
				c.t.Logf("screen client set remote offer failed: %v", err)
				continue
			}
			answer, err := c.pc.CreateAnswer(nil)
			if err == nil {
				err = c.pc.SetLocalDescription(answer)
			}
			if err != nil {
				c.t.Logf("screen client answer failed: %v", err)
				continue
			}
			c.send("answer", map[string]any{"sdp": answer.SDP})
		case "ice":
			var d struct {
				Candidate     string  `json:"candidate"`
				SDPMid        *string `json:"sdp_mid"`
				SDPMLineIndex *uint16 `json:"sdp_mline_index"`
			}
			json.Unmarshal(f.D, &d)
			if d.Candidate != "" {
				c.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: d.Candidate, SDPMid: d.SDPMid, SDPMLineIndex: d.SDPMLineIndex})
			}
		case "track_published", "track_ended":
			var d struct {
				Kind string `json:"kind"`
			}
			json.Unmarshal(f.D, &d)
			c.evMu.Lock()
			c.events = append(c.events, f.Op+":"+d.Kind)
			c.evMu.Unlock()
		case "caps_updated":
			c.capsUpdated.Add(1)
		}
	}
}

// sendMediaLoop 连通后持续发送模拟 Opus + 假 VP8 RTP（SFU 纯转发不解码，负载合法性无关紧要）。
// 伴轨在 startCompanion 后才发包：保证主麦克风轨先到达（第二条 audio 轨才算伴轨，BA.4）。
func (c *screenClient) sendMediaLoop() {
	<-c.connected
	seq := uint16(0)
	ts := uint32(0)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-tick.C:
		}
		c.audioTrack.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts},
			Payload: []byte{0xde, 0xad, 0xbe, 0xef},
		})
		if c.companionStarted.Load() {
			for _, ct := range c.companionTracks {
				ct.WriteRTP(&rtp.Packet{
					Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts},
					Payload: []byte{0xca, 0xfe, 0xba, 0xbe},
				})
			}
		}
		for _, vt := range c.videoTracks {
			// 首字节 0x10 为最小 VP8 payload descriptor（S=1），其余为填充。
			vt.WriteRTP(&rtp.Packet{
				Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts * 90 / 48, Marker: true},
				Payload: []byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
			})
		}
		seq++
		ts += 960
	}
}

var screenCaps = []string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen}
var audioOnlyCaps = []string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio}

// TestE2EScreenShareForwardAndRevoke 屏幕共享主链路：
// A 持 publish_screen 发布 video 轨 → B 收到 track_published(kind=screen) 与视频 RTP；
// UpdateParticipantCaps 收回 publish_screen → track_ended(kind=screen) 广播、转发停止、计数归零。
func TestE2EScreenShareForwardAndRevoke(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-scr")

	// B（观看端，无 publish_screen）先进房，确保能收到 A 的 track_published 广播
	b := dialScreenClient(t, env, env.signTokenCaps(t, "user-b", "sid-b", "room-scr", time.Minute, audioOnlyCaps), 0)
	defer b.close()
	go b.sendMediaLoop()
	waitFor(t, 5*time.Second, "client B connected", func() bool {
		select {
		case <-b.connected:
			return true
		default:
			return false
		}
	})

	// A（发布端，带 publish_screen + 1 条 video 轨）
	a := dialScreenClient(t, env, env.signTokenCaps(t, "user-a", "sid-a", "room-scr", time.Minute, screenCaps), 1)
	defer a.close()
	go a.sendMediaLoop()

	waitFor(t, 15*time.Second, "B sees track_published kind=screen", func() bool {
		return b.eventCount("track_published:screen") >= 1
	})
	waitFor(t, 15*time.Second, "B receives screen RTP from A", func() bool {
		return b.recvVideo.Load() > 10
	})
	waitFor(t, 5*time.Second, "screen track counted", func() bool {
		return env.mgr.ScreenTrackCount() == 1
	})

	// 收回 publish_screen（模拟 screen/stop / 抱下 / 配额掐最早 → UpdateParticipantCaps）
	if err := env.mgr.UpdateCaps("room-scr", "sid-a", audioOnlyCaps); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "B sees track_ended kind=screen", func() bool {
		return b.eventCount("track_ended:screen") >= 1
	})
	waitFor(t, 5*time.Second, "screen track count back to 0", func() bool {
		return env.mgr.ScreenTrackCount() == 0
	})

	// 转发确已停止：等待在途包排空后计数不再增长
	time.Sleep(300 * time.Millisecond)
	before := b.recvVideo.Load()
	time.Sleep(700 * time.Millisecond)
	if after := b.recvVideo.Load(); after != before {
		t.Fatalf("video forwarding must stop after revoke: %d -> %d", before, after)
	}
	// 音频不受影响
	waitFor(t, 5*time.Second, "audio still flowing", func() bool { return b.recvAudio.Load() > 10 })
}

// TestE2EScreenCapDenied 无 publish_screen 的客户端 offer video 轨 → 剥离不转发、会话保留。
func TestE2EScreenCapDenied(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-scr2")

	b := dialScreenClient(t, env, env.signTokenCaps(t, "user-b", "sid-b2", "room-scr2", time.Minute, audioOnlyCaps), 0)
	defer b.close()
	go b.sendMediaLoop()

	// A 无 publish_screen 却带 video 轨
	a := dialScreenClient(t, env, env.signTokenCaps(t, "user-a", "sid-a2", "room-scr2", time.Minute, audioOnlyCaps), 1)
	defer a.close()
	go a.sendMediaLoop()

	// 音频链路正常（会话未被杀）
	waitFor(t, 15*time.Second, "B receives audio from A", func() bool { return b.recvAudio.Load() > 10 })
	waitFor(t, 15*time.Second, "A receives audio from B", func() bool { return a.recvAudio.Load() > 10 })

	// 视频被剥离：无 screen 发布事件、无视频转发、屏幕轨计数为 0
	time.Sleep(time.Second)
	if n := b.eventCount("track_published:screen"); n != 0 {
		t.Fatalf("expected no screen publish event, got %d", n)
	}
	if v := b.recvVideo.Load(); v != 0 {
		t.Fatalf("expected no video RTP forwarded, got %d", v)
	}
	if n := env.mgr.ScreenTrackCount(); n != 0 {
		t.Fatalf("expected 0 screen tracks, got %d", n)
	}
}

// TestE2EScreenSingleTrackLimit 每用户同时 1 路（AX.4）：第二条 video 轨被剥离。
func TestE2EScreenSingleTrackLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-scr3")

	b := dialScreenClient(t, env, env.signTokenCaps(t, "user-b", "sid-b3", "room-scr3", time.Minute, audioOnlyCaps), 0)
	defer b.close()
	go b.sendMediaLoop()
	waitFor(t, 5*time.Second, "client B connected", func() bool {
		select {
		case <-b.connected:
			return true
		default:
			return false
		}
	})

	// A 带 2 条 video 轨
	a := dialScreenClient(t, env, env.signTokenCaps(t, "user-a", "sid-a3", "room-scr3", time.Minute, screenCaps), 2)
	defer a.close()
	go a.sendMediaLoop()

	waitFor(t, 15*time.Second, "B receives screen RTP", func() bool { return b.recvVideo.Load() > 10 })
	// 只接受 1 路
	waitFor(t, 5*time.Second, "exactly 1 screen track", func() bool { return env.mgr.ScreenTrackCount() == 1 })
	time.Sleep(time.Second)
	if n := env.mgr.ScreenTrackCount(); n != 1 {
		t.Fatalf("expected exactly 1 screen track (AX.4), got %d", n)
	}
	if n := b.eventCount("track_published:screen"); n != 1 {
		t.Fatalf("expected exactly 1 screen publish event, got %d", n)
	}
}
