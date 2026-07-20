// loadbot：headless 联调/压测客户端。
//
// 直连模式（M1 核心）：--ws-url ws://... --token <jwt>
// 连 WS → auth → ready → 建 PC 发一条模拟 Opus RTP 轨，同时统计收到的下行 RTP。
//
// Server 模式（M4，热迁移 e2e 核心）：--server-url http://... --username --password
// [--signup] --guild --channel：登录（/gapi/v1）→ voice/join → 连 Gateway WS →
// 建 PC 推/收音频；处理 VOICE_MIGRATING / VOICE_SERVER_UPDATE 做双 PC 热切
// （docs 09 M.3），打点静音窗口 mute_gap_ms 并回 ack。
//
// 退出码 0 = 成功收到至少一个远端 track 的 RTP 包（--expect-recv=true 时）。
package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"

	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

type flags struct {
	serverURL string
	username  string
	password  string
	email     string
	signup    bool
	guild     string
	channel   string
	// rttReport 进房前上报的 RTT 样本（"node_id:ms,node_id:ms"），e2e 用于
	// 引导调度落点（docs 10 §7）。
	rttReport string

	wsURL           string
	token           string
	duration        time.Duration
	expectRecv      bool
	screen          bool
	expectRecvVideo bool

	// 剪枝验证（M3，docs 08 D.5：静音 = 退订，跨级联真实停转发）
	unsubscribeUser  string
	unsubscribeAfter time.Duration
	resubscribeAfter time.Duration
}

type frame struct {
	Op string          `json:"op"`
	D  json.RawMessage `json:"d"`
}

// wsClient 串行化写的 WS 封装。
type wsClient struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (w *wsClient) send(op string, d any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteJSON(map[string]any{"op": op, "d": d})
}

type counters struct {
	sent      atomic.Int64
	recv      atomic.Int64
	tracks    atomic.Int64
	sentVideo atomic.Int64
	recvVideo atomic.Int64
}

func main() {
	var f flags
	flag.StringVar(&f.serverURL, "server-url", "", "Owl-Server base URL (server mode), e.g. http://127.0.0.1:8080")
	flag.StringVar(&f.username, "username", "", "server login username")
	flag.StringVar(&f.password, "password", "", "server login password")
	flag.StringVar(&f.email, "email", "", "signup email (default <username>@loadbot.test)")
	flag.BoolVar(&f.signup, "signup", false, "signup before login (fallback to login on conflict)")
	flag.StringVar(&f.guild, "guild", "", "guild id")
	flag.StringVar(&f.channel, "channel", "", "voice channel id")
	flag.StringVar(&f.rttReport, "rtt-report", "", "pre-join RTT samples: node_id:ms,node_id:ms (steers scheduling)")
	flag.StringVar(&f.wsURL, "ws-url", "", "SFU signaling endpoint, e.g. ws://127.0.0.1:8443/ws")
	flag.StringVar(&f.token, "token", "", "media token JWT (direct mode)")
	flag.DurationVar(&f.duration, "duration", 30*time.Second, "run duration before exit")
	flag.BoolVar(&f.expectRecv, "expect-recv", true, "exit non-zero if no downstream RTP received")
	flag.BoolVar(&f.screen, "screen", false, "also publish a synthetic VP8 screen track (token must carry publish_screen)")
	flag.BoolVar(&f.expectRecvVideo, "expect-recv-video", false, "exit non-zero if no downstream video RTP received")
	flag.StringVar(&f.unsubscribeUser, "unsubscribe-user", "", "user_id to unsubscribe from (prune verification, docs 08 D.5)")
	flag.DurationVar(&f.unsubscribeAfter, "unsubscribe-after", 0, "send unsubscribe after this delay (0 = disabled)")
	flag.DurationVar(&f.resubscribeAfter, "resubscribe-after", 0, "send subscribe again after this delay from start (0 = never)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	// Server 模式（M4）：登录 → join → Gateway → 双 PC 热切。
	if f.serverURL != "" {
		if f.username == "" || f.password == "" || f.guild == "" || f.channel == "" {
			log.Error("server mode requires --username --password --guild --channel")
			os.Exit(2)
		}
		if err := runServerMode(log, f); err != nil {
			log.Error("bot failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if f.wsURL == "" || f.token == "" {
		log.Error("missing required flags: --ws-url and --token (or --server-url mode)")
		os.Exit(2)
	}

	if err := runBot(log, f); err != nil {
		log.Error("bot failed", "err", err)
		os.Exit(1)
	}
}

func runBot(log *slog.Logger, f flags) error {
	conn, _, err := websocket.DefaultDialer.Dial(f.wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial ws: %w", err)
	}
	ws := &wsClient{conn: conn}
	defer conn.Close()

	// auth 首帧
	if err := ws.send("auth", map[string]any{"token": f.token}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// 建 PC：与 SFU 相同的 MediaEngine（Opus + audio-level 扩展）
	me, err := owlsfu.NewMediaEngine()
	if err != nil {
		return err
	}
	ir := &interceptor.Registry{}
	if err := webrtc.ConfigureRTCPReports(ir); err != nil {
		return err
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return fmt.Errorf("new pc: %w", err)
	}
	defer pc.Close()

	drainRTCP := func(sender *webrtc.RTPSender) {
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := sender.Read(buf); err != nil {
					return
				}
			}
		}()
	}

	track, err := webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "audio", "bot")
	if err != nil {
		return err
	}
	sender, err := pc.AddTrack(track)
	if err != nil {
		return err
	}
	drainRTCP(sender)

	// --screen：附带一条合成 VP8 屏幕轨。SFU 纯转发不解码（BA.3），故无需真实
	// VP8 编码器：直接以 TrackLocalStaticRTP 发送带最小 VP8 payload descriptor 的
	// 合成 RTP 包即可（接收端 bot 只统计包数、不解码渲染）。
	var videoTrack *webrtc.TrackLocalStaticRTP
	if f.screen {
		videoTrack, err = webrtc.NewTrackLocalStaticRTP(owlsfu.VP8Codec, "screen", "bot")
		if err != nil {
			return err
		}
		videoSender, err := pc.AddTrack(videoTrack)
		if err != nil {
			return err
		}
		drainRTCP(videoSender)
	}

	var ctr counters
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		ctr.tracks.Add(1)
		kind := remote.Kind()
		log.Info("downstream track", "id", remote.ID(), "stream", remote.StreamID(), "kind", kind.String())
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := remote.Read(buf); err != nil {
					ctr.tracks.Add(-1)
					return
				}
				if kind == webrtc.RTPCodecTypeVideo {
					ctr.recvVideo.Add(1)
				} else {
					ctr.recv.Add(1)
				}
			}
		}()
	})

	connected := make(chan struct{})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Info("pc state", "state", s.String())
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}
	})
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		ws.send("ice", map[string]any{
			"candidate":       init.Candidate,
			"sdp_mid":         init.SDPMid,
			"sdp_mline_index": init.SDPMLineIndex,
		})
	})

	done := make(chan error, 1)
	stop := make(chan struct{})
	defer close(stop)
	go readLoop(log, ws, conn, pc, done)

	// 协议时序：ready 帧到达后由 readLoop 发起 offer
	go pingLoop(ws, stop)
	go rtpSendLoop(track, sender, &ctr, connected, stop)
	if videoTrack != nil {
		go videoSendLoop(videoTrack, &ctr, connected, stop)
	}
	go statsLoop(&ctr, stop)
	if f.unsubscribeUser != "" && f.unsubscribeAfter > 0 {
		go pruneLoop(log, ws, f, &ctr, connected, stop)
	}

	select {
	case err := <-done:
		if err != nil {
			return err
		}
	case <-time.After(f.duration):
	}

	stats := map[string]int64{
		"sent": ctr.sent.Load(), "recv": ctr.recv.Load(), "tracks": ctr.tracks.Load(),
		"sent_video": ctr.sentVideo.Load(), "recv_video": ctr.recvVideo.Load(),
	}
	out, _ := json.Marshal(stats)
	fmt.Println(string(out))
	if f.expectRecv && ctr.recv.Load() == 0 {
		return fmt.Errorf("expected downstream RTP but received none")
	}
	if f.expectRecvVideo && ctr.recvVideo.Load() == 0 {
		return fmt.Errorf("expected downstream video RTP but received none")
	}
	return nil
}

// readLoop 处理 SFU → 客户端信令。
func readLoop(log *slog.Logger, ws *wsClient, conn *websocket.Conn, pc *webrtc.PeerConnection, done chan<- error) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			done <- fmt.Errorf("ws read: %w", err)
			return
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		switch f.Op {
		case "ready":
			log.Info("ready", "d", string(f.D))
			offer, err := pc.CreateOffer(nil)
			if err == nil {
				err = pc.SetLocalDescription(offer)
			}
			if err != nil {
				done <- fmt.Errorf("create offer: %w", err)
				return
			}
			ws.send("offer", map[string]any{"sdp": offer.SDP})
		case "answer":
			var d struct {
				SDP string `json:"sdp"`
			}
			json.Unmarshal(f.D, &d)
			if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: d.SDP}); err != nil {
				log.Warn("set remote answer failed", "err", err)
			}
		case "offer":
			// SFU 发起 renegotiation（新增/移除下行轨）
			var d struct {
				SDP string `json:"sdp"`
			}
			json.Unmarshal(f.D, &d)
			if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: d.SDP}); err != nil {
				log.Warn("set remote offer failed", "err", err)
				continue
			}
			answer, err := pc.CreateAnswer(nil)
			if err == nil {
				err = pc.SetLocalDescription(answer)
			}
			if err != nil {
				log.Warn("create answer failed", "err", err)
				continue
			}
			ws.send("answer", map[string]any{"sdp": answer.SDP})
		case "ice":
			var d struct {
				Candidate     string  `json:"candidate"`
				SDPMid        *string `json:"sdp_mid"`
				SDPMLineIndex *uint16 `json:"sdp_mline_index"`
			}
			json.Unmarshal(f.D, &d)
			if d.Candidate != "" {
				pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: d.Candidate, SDPMid: d.SDPMid, SDPMLineIndex: d.SDPMLineIndex})
			}
		case "speaking", "participant_joined", "participant_left", "track_published", "track_ended", "caps_updated", "pong":
			log.Debug("event", "op", f.Op, "d", string(f.D))
		case "closed":
			done <- fmt.Errorf("closed by sfu: %s", string(f.D))
			return
		}
	}
}

// pruneLoop 剪枝验证（docs 08 D.5/§5.1）：媒体连通后按计划发送 unsubscribe /
// subscribe 帧，并在动作前后打点 recv 计数，供脚本比对级联是否真实停/复转发。
func pruneLoop(log *slog.Logger, ws *wsClient, f flags, ctr *counters, connected, stop <-chan struct{}) {
	select {
	case <-connected:
	case <-stop:
		return
	}
	select {
	case <-stop:
		return
	case <-time.After(f.unsubscribeAfter):
	}
	before := ctr.recv.Load()
	if err := ws.send("unsubscribe", map[string]any{"user_id": f.unsubscribeUser}); err != nil {
		log.Error("send unsubscribe failed", "err", err)
		return
	}
	log.Info("unsubscribe sent", "user_id", f.unsubscribeUser, "recv_before", before)
	fmt.Println(mustJSON(map[string]any{"event": "unsubscribe", "user_id": f.unsubscribeUser, "recv": before}))

	if f.resubscribeAfter <= 0 {
		return
	}
	wait := f.resubscribeAfter - f.unsubscribeAfter
	if wait < 0 {
		wait = 0
	}
	select {
	case <-stop:
		return
	case <-time.After(wait):
	}
	atResub := ctr.recv.Load()
	if err := ws.send("subscribe", map[string]any{"user_id": f.unsubscribeUser}); err != nil {
		log.Error("send subscribe failed", "err", err)
		return
	}
	log.Info("resubscribe sent", "user_id", f.unsubscribeUser, "recv_at_resub", atResub)
	fmt.Println(mustJSON(map[string]any{"event": "resubscribe", "user_id": f.unsubscribeUser, "recv": atResub}))
}

func mustJSON(v any) string {
	out, _ := json.Marshal(v)
	return string(out)
}

// pingLoop 每 15s 心跳。
func pingLoop(ws *wsClient, stop <-chan struct{}) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if err := ws.send("ping", map[string]any{}); err != nil {
				return
			}
		}
	}
}

// rtpSendLoop 媒体连通后持续发送模拟 Opus RTP：
// ptime 20ms、时间戳步进 960、随机 20 字节 payload、audio-level 扩展交替 speaking/silent。
func rtpSendLoop(track *webrtc.TrackLocalStaticRTP, sender *webrtc.RTPSender, ctr *counters, connected, stop <-chan struct{}) {
	select {
	case <-connected:
	case <-stop:
		return
	}

	// 协商后的 audio-level 扩展 ID
	var extID uint8
	for _, ext := range sender.GetParameters().HeaderExtensions {
		if ext.URI == sdp.AudioLevelURI {
			extID = uint8(ext.ID)
		}
	}

	payload := make([]byte, 20)
	rand.Read(payload)
	seq := uint16(0)
	ts := uint32(0)
	start := time.Now()

	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				SequenceNumber: seq,
				Timestamp:      ts,
			},
			Payload: payload,
		}
		seq++
		ts += 960
		if extID != 0 {
			// 每 2s 交替 speaking(20 dBov) / silent(90 dBov)
			level := uint8(20)
			if int(time.Since(start).Seconds())%4 >= 2 {
				level = 90
			}
			if raw, err := (&rtp.AudioLevelExtension{Level: level, Voice: level < 45}).Marshal(); err == nil {
				pkt.SetExtension(extID, raw)
			}
		}
		if err := track.WriteRTP(pkt); err != nil {
			return
		}
		ctr.sent.Add(1)
	}
}

// videoSendLoop 媒体连通后按 ~30fps 发送合成 VP8 屏幕包：
// 每包一帧（marker=1）、时间戳步进 3000（90kHz/30fps）、最小 VP8 payload descriptor + 填充负载。
func videoSendLoop(track *webrtc.TrackLocalStaticRTP, ctr *counters, connected, stop <-chan struct{}) {
	select {
	case <-connected:
	case <-stop:
		return
	}
	payload := make([]byte, 1000)
	rand.Read(payload)
	payload[0] = 0x10 // VP8 payload descriptor：S=1（分片起始），无扩展
	seq := uint16(0)
	ts := uint32(0)
	t := time.NewTicker(33 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				SequenceNumber: seq,
				Timestamp:      ts,
				Marker:         true,
			},
			Payload: payload,
		}
		seq++
		ts += 3000
		if err := track.WriteRTP(pkt); err != nil {
			return
		}
		ctr.sentVideo.Add(1)
	}
}

// statsLoop 每 2s 打印一行 JSON 统计。
func statsLoop(ctr *counters, stop <-chan struct{}) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			out, _ := json.Marshal(map[string]int64{
				"sent": ctr.sent.Load(), "recv": ctr.recv.Load(), "tracks": ctr.tracks.Load(),
				"sent_video": ctr.sentVideo.Load(), "recv_video": ctr.recvVideo.Load(),
			})
			fmt.Println(string(out))
		}
	}
}
