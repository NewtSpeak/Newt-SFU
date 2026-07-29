package signal

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/room"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

const e2eNodeID = "test-node"

// testEnv 起一套进程内 SFU：ephemeral UDPMux + 明文 WS 信令。
type testEnv struct {
	verifier *auth.Verifier
	mgr      *room.Manager
	priv     ed25519.PrivateKey
	wsURL    string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	log := slog.Default()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(e2eNodeID, []*owlsfuv1.MediaTokenKey{{Kid: "k1", Ed25519PublicKey: pub}})
	t.Cleanup(verifier.Close)

	engine, err := owlsfu.NewEngine(0, "") // 端口 0 = ephemeral
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })

	metrics := observability.NewMetrics()
	st := stats.NewCollector(100)
	mgr := room.NewManager(log, engine, metrics, st, 100)

	srv := NewServer(log, "127.0.0.1:0", true, "", "", verifier, mgr, metrics)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(func() { l.Close() })

	return &testEnv{
		verifier: verifier,
		mgr:      mgr,
		priv:     priv,
		wsURL:    fmt.Sprintf("ws://%s/ws", l.Addr().String()),
	}
}

func (e *testEnv) signToken(t *testing.T, uid, sid, rid string, ttl time.Duration) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"v": 1, "uid": uid, "gid": "g1", "cid": "c1",
		"nid": e2eNodeID, "rid": rid, "sid": sid,
		"caps": []string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio},
		"iat":  now.Unix(), "exp": now.Add(ttl).Unix(), "jti": "jti-" + sid,
	})
	tok.Header["kid"] = "k1"
	raw, err := tok.SignedString(e.priv)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// testClient 为进程内 headless 客户端（发一条 RTP 轨、统计下行包数）。
type testClient struct {
	t     *testing.T
	wsMu  sync.Mutex
	ws    *websocket.Conn
	pc    *webrtc.PeerConnection
	track *webrtc.TrackLocalStaticRTP

	recv       atomic.Int64
	ready      chan struct{}
	connected  chan struct{}
	closedCode atomic.Value // string
	stop       chan struct{}
	stopOnce   sync.Once
}

func (c *testClient) send(op string, d any) {
	c.wsMu.Lock()
	defer c.wsMu.Unlock()
	c.ws.WriteJSON(map[string]any{"op": op, "d": d})
}

func (c *testClient) close() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.pc.Close()
	c.ws.Close()
}

func dialClient(t *testing.T, env *testEnv, token string) *testClient {
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
	track, err := webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "audio", "client")
	if err != nil {
		t.Fatal(err)
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()

	c := &testClient{
		t: t, ws: ws, pc: pc, track: track,
		ready: make(chan struct{}), connected: make(chan struct{}), stop: make(chan struct{}),
	}
	c.closedCode.Store("")

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := remote.Read(buf); err != nil {
					return
				}
				c.recv.Add(1)
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

func (c *testClient) readLoop() {
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
				c.t.Logf("client offer failed: %v", err)
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
				c.t.Logf("client set remote offer failed: %v", err)
				continue
			}
			answer, err := c.pc.CreateAnswer(nil)
			if err == nil {
				err = c.pc.SetLocalDescription(answer)
			}
			if err != nil {
				c.t.Logf("client answer failed: %v", err)
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
		case "closed":
			var d struct {
				Code string `json:"code"`
			}
			json.Unmarshal(f.D, &d)
			c.closedCode.Store(d.Code)
		}
	}
}

// sendRTPLoop 媒体连通后按 20ms 步进发送模拟 Opus 包。
func (c *testClient) sendRTPLoop() {
	<-c.connected
	seq := uint16(0)
	ts := uint32(0)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			pkt := &rtp.Packet{
				Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts},
				Payload: []byte{0xde, 0xad, 0xbe, 0xef},
			}
			seq++
			ts += 960
			if err := c.track.WriteRTP(pkt); err != nil {
				return
			}
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", desc)
}

// TestE2EAudioForwardAndKick 验证 M1 主链路：
// 双客户端 auth→offer/answer→ICE 连通→互相收到对方 RTP→DisconnectUser 秒级生效。
func TestE2EAudioForwardAndKick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-1")

	a := dialClient(t, env, env.signToken(t, "user-a", "sid-a", "room-1", time.Minute))
	defer a.close()
	waitFor(t, 5*time.Second, "client A ready", func() bool {
		select {
		case <-a.ready:
			return true
		default:
			return false
		}
	})
	go a.sendRTPLoop()

	b := dialClient(t, env, env.signToken(t, "user-b", "sid-b", "room-1", time.Minute))
	defer b.close()
	go b.sendRTPLoop()

	// 双向互转发：A 收到 B 的包，B 收到 A 的包
	waitFor(t, 15*time.Second, "A receives RTP from B", func() bool { return a.recv.Load() > 10 })
	waitFor(t, 15*time.Second, "B receives RTP from A", func() bool { return b.recv.Load() > 10 })

	users, rooms := env.mgr.Counts()
	if users != 2 || rooms != 1 {
		t.Fatalf("expected 2 users / 1 room, got %d/%d", users, rooms)
	}

	// 踢人：P99 < 1s 目标，这里给 2s 上限
	start := time.Now()
	n := env.mgr.DisconnectUser("room-1", "user-b", "", "admin")
	if n != 1 {
		t.Fatalf("expected 1 session disconnected, got %d", n)
	}
	waitFor(t, 2*time.Second, "B receives closed frame", func() bool {
		return b.closedCode.Load().(string) == room.CloseDisconnected
	})
	t.Logf("kick latency: %s", time.Since(start))

	users, _ = env.mgr.Counts()
	if users != 1 {
		t.Fatalf("expected 1 user after kick, got %d", users)
	}
}

// TestE2ERevokeSession 验证吊销：会话立即断开且同 token 无法重入。
func TestE2ERevokeSession(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-2")

	token := env.signToken(t, "user-a", "sid-r", "room-2", time.Minute)
	a := dialClient(t, env, token)
	defer a.close()
	waitFor(t, 5*time.Second, "client ready", func() bool {
		select {
		case <-a.ready:
			return true
		default:
			return false
		}
	})

	env.verifier.Revoke("sid-r")
	env.mgr.CloseSession("sid-r", room.CloseSessionRevoked, "revoked")
	waitFor(t, 2*time.Second, "closed frame", func() bool {
		return a.closedCode.Load().(string) == room.CloseSessionRevoked
	})

	// 同 token 重入必须被拒（吊销表按 sid 立即生效）
	ws, _, err := websocket.DefaultDialer.Dial(env.wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	ws.WriteJSON(map[string]any{"op": "auth", "d": map[string]any{"token": token}})
	var f struct {
		Op string `json:"op"`
		D  struct {
			Code string `json:"code"`
		} `json:"d"`
	}
	ws.SetReadDeadline(time.Now().Add(3 * time.Second))
	if err := ws.ReadJSON(&f); err != nil {
		t.Fatal(err)
	}
	if f.Op != "closed" || f.D.Code != auth.CodeSessionRevoked {
		t.Fatalf("expected closed/SESSION_REVOKED, got %s/%s", f.Op, f.D.Code)
	}
}
