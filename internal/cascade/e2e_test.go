package cascade

// 双节点级联端到端验证（15 BM M3 验收锚点：双节点两用户互听、剪枝生效）：
// 进程内起两个完整 SFU 节点（各自 UDPMux/room.Manager/cascade.Manager + 测试 CA
// 签发的节点证书），手工下发 SetAnchorLease/SetCascadeEdges，验证：
//  1. A 节点用户发音频 → 级联边（mTLS 信令 + 节点间 PC）→ B 节点用户收到
//  2. B 用户退订 → NodeWant 剪枝 → A 停止向该枝转发；重订阅恢复

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/prometheus/client_golang/prometheus/testutil"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/room"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

// ---- 测试 CA：模拟 enrollment 签发的节点证书（CN/SAN = node_id）----

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Cluster CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

// issue 签发节点证书：CN = node_id、DNS SAN = node_id（与 Server CA 规则一致）。
func (ca *testCA) issue(t *testing.T, nodeID string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: nodeID},
		DNSNames:     []string{nodeID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// ---- 进程内 SFU 节点 ----

type e2eNode struct {
	id      string
	mgr     *room.Manager
	casc    *Manager
	metrics *observability.Metrics
}

func newE2ENode(t *testing.T, ca *testCA, nodeID string) *e2eNode {
	t.Helper()
	engine, err := owlsfu.NewEngine(0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })

	metrics := observability.NewMetrics()
	mgr := room.NewManager(slog.Default(), engine, metrics, stats.NewCollector(100), 100)
	casc, err := New(slog.Default(), Config{
		NodeID: nodeID,
		Listen: "127.0.0.1:0",
		Cert:   ca.issue(t, nodeID),
		CAPool: ca.pool,
	}, engine, mgr, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(casc.Close)
	mgr.SetCascade(casc)
	return &e2eNode{id: nodeID, mgr: mgr, casc: casc, metrics: metrics}
}

// ---- 进程内客户端：直接驱动 room.Participant（绕过 WS 层，signal 包不在本专项范围）----

type e2eClient struct {
	t     *testing.T
	pc    *webrtc.PeerConnection
	part  *room.Participant
	track *webrtc.TrackLocalStaticRTP

	sigMu     sync.Mutex // 串行化客户端侧 SDP 操作
	recv      atomic.Int64
	connected chan struct{}
	connOnce  sync.Once
	stop      chan struct{}
	stopOnce  sync.Once
}

// Send 实现 room.Messenger：拦截 SFU → 客户端信令帧。
func (c *e2eClient) Send(op string, d any) error {
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
	}
	return nil
}

func (c *e2eClient) CloseWithReason(string, string) {}

func (c *e2eClient) answerSFUOffer(sdp string) {
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

func (c *e2eClient) close() {
	c.stopOnce.Do(func() { close(c.stop) })
	c.pc.Close()
}

// joinClient 入房并完成首次 offer/answer 协商；publish=true 时携带上行音频轨。
func joinClient(t *testing.T, node *e2eNode, uid, sid, rid string, publish bool) *e2eClient {
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
	c := &e2eClient{t: t, pc: pc, connected: make(chan struct{}), stop: make(chan struct{})}

	if publish {
		track, err := webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "audio", uid)
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
		c.track = track
	} else {
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			t.Fatal(err)
		}
	}

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
			c.connOnce.Do(func() { close(c.connected) })
		}
	})

	tok := &auth.Token{
		UID: uid, RID: rid, SID: sid, NID: node.id,
		Caps:      auth.NewCapSet([]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio}),
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

// sendRTPLoop 媒体连通后按 20ms 步进发送模拟 Opus 包。
func (c *e2eClient) sendRTPLoop() {
	<-c.connected
	seq, ts := uint16(0), uint32(0)
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

// TestE2ECascadeForwardAndPrune 双节点级联主链路 + NodeWant 剪枝。
func TestE2ECascadeForwardAndPrune(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cascade e2e in -short mode")
	}
	const roomID = "lr-1"
	ca := newTestCA(t)
	nodeA := newE2ENode(t, ca, "node-a") // anchor / parent
	nodeB := newE2ENode(t, ca, "node-b") // child

	nodeA.mgr.EnsureRoom(roomID)
	nodeB.mgr.EnsureRoom(roomID)

	// Server 编排（手工模拟）：租约 + 边集（epoch=1，B → A）
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
		CascadeToken:          "tok-epoch1",
	}}
	// 先 parent 后 child（授权先就位；反序也可，child 会退避重拨）
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

	// A 节点 speaker、B 节点 listener
	speaker := joinClient(t, nodeA, "user-a", "sid-a", roomID, true)
	defer speaker.close()
	go speaker.sendRTPLoop()

	listener := joinClient(t, nodeB, "user-b", "sid-b", roomID, false)
	defer listener.close()

	// 主链路：A 用户音频经级联边到达 B 用户
	waitFor(t, 20*time.Second, "listener on node-b receives RTP from node-a speaker", func() bool {
		return listener.recv.Load() > 10
	})

	activeGauge := func(n *e2eNode) float64 {
		return testutil.ToFloat64(n.metrics.CascadeOutboundTracks.WithLabelValues("active"))
	}
	prunedGauge := func(n *e2eNode) float64 {
		return testutil.ToFloat64(n.metrics.CascadeOutboundTracks.WithLabelValues("pruned"))
	}
	if activeGauge(nodeA) != 1 {
		t.Fatalf("node-a 应有 1 条活跃出向轨，got %v", activeGauge(nodeA))
	}

	// 剪枝：B 唯一听众退订 user-a → NodeWant 上报排除 → A 摘除出向轨
	listener.part.Unsubscribe("user-a")
	waitFor(t, 10*time.Second, "node-a prunes outbound track after unsubscribe", func() bool {
		return activeGauge(nodeA) == 0 && prunedGauge(nodeA) == 1
	})
	// 转发确已停止（容忍在途包）
	base := listener.recv.Load()
	time.Sleep(1 * time.Second)
	settled := listener.recv.Load()
	time.Sleep(1 * time.Second)
	if got := listener.recv.Load(); got != settled {
		t.Fatalf("剪枝后仍在转发：%d → %d → %d", base, settled, got)
	}

	// 恢复订阅：级联恢复该 track（08 §5.1）
	listener.part.Subscribe("user-a")
	waitFor(t, 10*time.Second, "node-a restores outbound track after resubscribe", func() bool {
		return activeGauge(nodeA) == 1
	})
	resumeBase := listener.recv.Load()
	waitFor(t, 20*time.Second, "forwarding resumes after resubscribe", func() bool {
		return listener.recv.Load() > resumeBase+10
	})

	// 全程无环路丢包
	if n := testutil.ToFloat64(nodeA.metrics.CascadeLoopDropped) + testutil.ToFloat64(nodeB.metrics.CascadeLoopDropped); n != 0 {
		t.Fatalf("不应有环路丢包，got %v", n)
	}
}

// TestE2ECascadeRejectsUnauthorizedEdge 验证三重校验：未授权边/错误 token 被拒。
func TestE2ECascadeRejectsUnauthorizedEdge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cascade e2e in -short mode")
	}
	const roomID = "lr-sec"
	ca := newTestCA(t)
	nodeA := newE2ENode(t, ca, "node-a")
	nodeB := newE2ENode(t, ca, "node-b")
	nodeA.mgr.EnsureRoom(roomID)
	nodeB.mgr.EnsureRoom(roomID)

	expire := time.Now().Add(time.Minute).UnixMilli()
	_ = nodeA.casc.SetAnchorLease(roomID, "node-a", 1, expire)
	_ = nodeB.casc.SetAnchorLease(roomID, "node-a", 1, expire)

	// parent 侧授权 token = "good"；child 被下发错误 token → 握手必须被拒
	goodEdge := []*owlsfuv1.CascadeEdge{{
		ParentNodeId: "node-a", ChildNodeId: "node-b",
		ParentCascadeEndpoint: nodeA.casc.Addr(), CascadeToken: "good",
	}}
	badEdge := []*owlsfuv1.CascadeEdge{{
		ParentNodeId: "node-a", ChildNodeId: "node-b",
		ParentCascadeEndpoint: nodeA.casc.Addr(), CascadeToken: "bad",
	}}
	if err := nodeA.casc.SetCascadeEdges(roomID, 1, goodEdge); err != nil {
		t.Fatal(err)
	}
	if err := nodeB.casc.SetCascadeEdges(roomID, 1, badEdge); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "token mismatch rejected", func() bool {
		return testutil.ToFloat64(nodeA.metrics.CascadeHandshakeFailed.WithLabelValues("token_mismatch")) >= 1
	})
	if testutil.ToFloat64(nodeA.metrics.CascadeEdges) != 0 {
		t.Fatal("错误 token 不应建立边")
	}
}

// TestE2ECascadeTokenSignatureRejected 验证 VerifyToken（签名校验层）接线：
// 即使等值比较通过（两侧持同一 token），签名校验失败也必须拒绝握手（BG.2）。
func TestE2ECascadeTokenSignatureRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cascade e2e in -short mode")
	}
	const roomID = "lr-sig"
	ca := newTestCA(t)

	engine, err := owlsfu.NewEngine(0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	metrics := observability.NewMetrics()
	mgr := room.NewManager(slog.Default(), engine, metrics, stats.NewCollector(100), 100)
	// parent 节点注入「仅接受 signed-ok」的验签器（模拟 auth.Verifier.VerifyCascade）
	parent, err := New(slog.Default(), Config{
		NodeID: "node-a", Listen: "127.0.0.1:0",
		Cert: ca.issue(t, "node-a"), CAPool: ca.pool,
		VerifyToken: func(token, room string, epoch uint64, p, c string) error {
			if token != "signed-ok" {
				return errAny
			}
			return nil
		},
	}, engine, mgr, metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(parent.Close)
	mgr.SetCascade(parent)

	child := newE2ENode(t, ca, "node-b")
	mgr.EnsureRoom(roomID)
	child.mgr.EnsureRoom(roomID)

	expire := time.Now().Add(time.Minute).UnixMilli()
	_ = parent.SetAnchorLease(roomID, "node-a", 1, expire)
	_ = child.casc.SetAnchorLease(roomID, "node-a", 1, expire)

	// 两侧同一 token（等值比较通过），但签名校验拒绝 → 必须拒绝握手
	edges := []*owlsfuv1.CascadeEdge{{
		ParentNodeId: "node-a", ChildNodeId: "node-b",
		ParentCascadeEndpoint: parent.Addr(), CascadeToken: "forged-but-equal",
	}}
	if err := parent.SetCascadeEdges(roomID, 1, edges); err != nil {
		t.Fatal(err)
	}
	if err := child.casc.SetCascadeEdges(roomID, 1, edges); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "signature verification rejected", func() bool {
		return testutil.ToFloat64(metrics.CascadeHandshakeFailed.WithLabelValues("token_invalid")) >= 1
	})
	if testutil.ToFloat64(metrics.CascadeEdges) != 0 {
		t.Fatal("签名校验失败不应建立边")
	}
}

// errAny 供测试 VerifyToken 返回的固定错误。
var errAny = fmt.Errorf("signature verification failed")
