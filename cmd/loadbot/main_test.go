package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/room"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
	"github.com/newtspeak/newt-sfu/internal/signal"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

// TestBotEndToEnd 起进程内 SFU，双 bot 直连模式互发互收（验证 runBot 全链路）。
func TestBotEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping bot e2e in -short mode")
	}
	log := slog.Default()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("bot-test-node", []*owlsfuv1.MediaTokenKey{{Kid: "k1", Ed25519PublicKey: pub}})
	t.Cleanup(verifier.Close)

	engine, err := owlsfu.NewEngine(0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })

	metrics := observability.NewMetrics()
	mgr := room.NewManager(log, engine, metrics, stats.NewCollector(100), 100)
	mgr.EnsureRoom("room-bot")

	srv := signal.NewServer(log, "127.0.0.1:0", true, "", "", verifier, mgr, metrics)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(func() { l.Close() })
	wsURL := fmt.Sprintf("ws://%s/ws", l.Addr().String())

	sign := signFunc(t, priv, "bot-test-node", "room-bot", []string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio})

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, cred := range []struct{ uid, sid string }{
		{"bot-a", "sid-bot-a"}, {"bot-b", "sid-bot-b"},
	} {
		wg.Add(1)
		go func(idx int, uid, sid string) {
			defer wg.Done()
			errs[idx] = runBot(log.With("bot", uid), flags{
				wsURL:      wsURL,
				token:      sign(uid, sid),
				duration:   8 * time.Second,
				expectRecv: true, // recv==0 时 runBot 返回错误
			})
		}(i, cred.uid, cred.sid)
		time.Sleep(500 * time.Millisecond) // 错开进房
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("bot %d failed: %v", i, err)
		}
	}
}

// signFunc 生成指定房间/caps 的 Media Token 签发闭包。
func signFunc(t *testing.T, priv ed25519.PrivateKey, nodeID, roomID string, caps []string) func(uid, sid string) string {
	return func(uid, sid string) string {
		now := time.Now()
		tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
			"v": 1, "uid": uid, "gid": "g1", "cid": "c1",
			"nid": nodeID, "rid": roomID, "sid": sid,
			"caps": caps,
			"iat":  now.Unix(), "exp": now.Add(time.Minute).Unix(), "jti": "jti-" + sid,
		})
		tok.Header["kid"] = "k1"
		raw, err := tok.SignedString(priv)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
}

// TestBotScreenShare 屏幕共享集成（docs 14 验证锚点）：单节点两 bot，
// A 持 publish_screen 用 --screen 发合成 VP8 屏幕轨，B 收到视频 RTP 包数 > 0。
func TestBotScreenShare(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping bot e2e in -short mode")
	}
	log := slog.Default()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("bot-scr-node", []*owlsfuv1.MediaTokenKey{{Kid: "k1", Ed25519PublicKey: pub}})
	t.Cleanup(verifier.Close)

	engine, err := owlsfu.NewEngine(0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })

	metrics := observability.NewMetrics()
	mgr := room.NewManager(log, engine, metrics, stats.NewCollector(100), 100)
	mgr.EnsureRoom("room-scr")

	srv := signal.NewServer(log, "127.0.0.1:0", true, "", "", verifier, mgr, metrics)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(l)
	t.Cleanup(func() { l.Close() })
	wsURL := fmt.Sprintf("ws://%s/ws", l.Addr().String())

	signScreen := signFunc(t, priv, "bot-scr-node", "room-scr",
		[]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen})
	signViewer := signFunc(t, priv, "bot-scr-node", "room-scr",
		[]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio})

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// bot A：发布屏幕轨
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = runBot(log.With("bot", "bot-a"), flags{
			wsURL:      wsURL,
			token:      signScreen("bot-a", "sid-scr-a"),
			duration:   8 * time.Second,
			expectRecv: true,
			screen:     true,
		})
	}()
	time.Sleep(500 * time.Millisecond)

	// bot B：观看端，要求收到视频 RTP
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = runBot(log.With("bot", "bot-b"), flags{
			wsURL:           wsURL,
			token:           signViewer("bot-b", "sid-scr-b"),
			duration:        8 * time.Second,
			expectRecv:      true,
			expectRecvVideo: true, // recv_video==0 时 runBot 返回错误
		})
	}()
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("bot %d failed: %v", i, err)
		}
	}
}
