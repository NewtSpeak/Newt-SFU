package signal

// 屏幕共享 simulcast e2e（docs 14 BA.3 最小版）：发布端按 rid a/b/c 三层上行，
// SFU 依 mid/rid 扩展分流各层；观看端缺省收 high（rid=a）层，经
// {"op":"set_layer","d":{"user_id":...,"layer":"low"}} 切到 low（rid=c）层。
// 各层 RTP payload 首字节作为层标记（0xA1/0xB2/0xC3），观看端据此断言收到的层。

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"

	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
)

// 层标记（VP8 payload descriptor 语义无关：SFU 纯转发不解码）。
const (
	markHigh   = 0xA1
	markMedium = 0xB2
	markLow    = 0xC3
)

// simulcastClient 为 simulcast 发布端：一条 video sender 上挂 rid=a/b/c 三个编码。
type simulcastClient struct {
	t    *testing.T
	wsMu sync.Mutex
	ws   *websocket.Conn
	pc   *webrtc.PeerConnection

	audioTrack *webrtc.TrackLocalStaticRTP
	layers     []*webrtc.TrackLocalStaticRTP // rid a/b/c
	sender     *webrtc.RTPSender

	ready     chan struct{}
	connected chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
}

func (c *simulcastClient) send(op string, d any) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	c.ws.WriteJSON(map[string]any{"op": op, "d": d})
}

func (c *simulcastClient) close() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.pc.Close()
	c.ws.Close()
}

func dialSimulcastClient(t *testing.T, env *testEnv, token string) *simulcastClient {
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
	c := &simulcastClient{
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
	as, err := pc.AddTrack(c.audioTrack)
	if err != nil {
		t.Fatal(err)
	}
	drain(as)

	for _, rid := range []string{"a", "b", "c"} {
		vt, err := webrtc.NewTrackLocalStaticRTP(owlsfu.VP8Codec, "screen", "cli",
			webrtc.WithRTPStreamID(rid))
		if err != nil {
			t.Fatal(err)
		}
		c.layers = append(c.layers, vt)
	}
	c.sender, err = pc.AddTrack(c.layers[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := c.sender.AddEncoding(c.layers[1]); err != nil {
		t.Fatal(err)
	}
	if err := c.sender.AddEncoding(c.layers[2]); err != nil {
		t.Fatal(err)
	}
	drain(c.sender)

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

func (c *simulcastClient) readLoop() {
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
				c.t.Logf("simulcast client offer failed: %v", err)
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
				continue
			}
			answer, err := c.pc.CreateAnswer(nil)
			if err == nil {
				err = c.pc.SetLocalDescription(answer)
			}
			if err != nil {
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
		}
	}
}

// sendSimulcastLoop 连通后三层同发；发布端负责在包上打 mid/rid 头扩展
// （TrackLocalStaticRTP 不自动注入，接收端 SFU 依此分流各 rid 层）。
func (c *simulcastClient) sendSimulcastLoop() {
	<-c.connected
	// 从协商结果取 mid / rid 扩展 ID
	var midID, ridID uint8
	for _, ext := range c.sender.GetParameters().HeaderExtensions {
		switch ext.URI {
		case sdp.SDESMidURI:
			midID = uint8(ext.ID)
		case sdp.SDESRTPStreamIDURI:
			ridID = uint8(ext.ID)
		}
	}
	if midID == 0 || ridID == 0 {
		c.t.Logf("mid/rid extension not negotiated: mid=%d rid=%d", midID, ridID)
		return
	}
	var midVal string
	for _, tcv := range c.pc.GetTransceivers() {
		if tcv.Sender() == c.sender {
			midVal = tcv.Mid()
		}
	}

	marks := []byte{markHigh, markMedium, markLow}
	seq, ts := uint16(0), uint32(0)
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
		for i, vt := range c.layers {
			pkt := &rtp.Packet{
				Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts * 90 / 48, Marker: true},
				Payload: []byte{marks[i], 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
			}
			pkt.Header.SetExtension(midID, []byte(midVal))
			pkt.Header.SetExtension(ridID, []byte(vt.RID()))
			vt.WriteRTP(pkt)
		}
		seq++
		ts += 960
	}
}

// TestE2ESimulcastLayerSelection simulcast 选层主链路：
// 三层上行 → 观看端缺省收 high 层 → set_layer low → 切到 low 层。
func TestE2ESimulcastLayerSelection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-sim")

	// 观看端先进房
	b := dialScreenClient(t, env, env.signTokenCaps(t, "user-b", "sid-sb", "room-sim", time.Minute, audioOnlyCaps), 0)
	defer b.close()
	go b.sendMediaLoop()
	waitFor(t, 5*time.Second, "viewer connected", func() bool {
		select {
		case <-b.connected:
			return true
		default:
			return false
		}
	})

	// simulcast 发布端（rid a/b/c）
	a := dialSimulcastClient(t, env, env.signTokenCaps(t, "user-a", "sid-sa", "room-sim", time.Minute, screenCaps))
	defer a.close()
	go a.sendSimulcastLoop()

	// 三层均被 SFU 接收（同一路屏幕：计数仍为 1）
	waitFor(t, 20*time.Second, "viewer receives video (default high layer)", func() bool {
		return b.firstByteCounter(markHigh).Load() > 10
	})
	if n := env.mgr.ScreenTrackCount(); n != 1 {
		t.Fatalf("simulcast 多 rid 层应算 1 路屏幕，got %d", n)
	}
	if got := b.firstByteCounter(markLow).Load(); got != 0 {
		t.Fatalf("缺省只应转发 high 层，收到 low 层 %d 包", got)
	}

	// 选层切换：high → low
	b.send("set_layer", map[string]any{"user_id": "user-a", "layer": "low"})
	waitFor(t, 20*time.Second, "viewer receives low layer after set_layer", func() bool {
		return b.firstByteCounter(markLow).Load() > 10
	})
	// high 层停止增长（容忍在途包）
	time.Sleep(300 * time.Millisecond)
	highSettled := b.firstByteCounter(markHigh).Load()
	time.Sleep(700 * time.Millisecond)
	if got := b.firstByteCounter(markHigh).Load(); got != highSettled {
		t.Fatalf("set_layer(low) 后 high 层仍在转发：%d → %d", highSettled, got)
	}
	// medium 层全程不应到达观看端
	if got := b.firstByteCounter(markMedium).Load(); got != 0 {
		t.Fatalf("medium 层不应被转发，got %d", got)
	}
}
