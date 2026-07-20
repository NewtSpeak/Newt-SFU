// Server 模式（M4 热迁移 e2e）：经 Owl-Server 完成 登录 → voice/join → Gateway WS，
// 处理 VOICE_MIGRATING / VOICE_SERVER_UPDATE 事件做双 PC 热切（docs 09 M.3）：
// 收到新 endpoint+token 后新建第二个 PC，新 PC 就绪前旧 PC 继续收发；新 PC 连通即
// CUTOVER（发送切到新 PC + 回 ack），新 PC 首包后拆旧 PC；打点静音窗口
// mute_gap_ms（旧 PC 最后一个收包 → 新 PC 第一个收包）。
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
)

// ---------------------------------------------------------------------------
// 媒体会话（一个 SFU 信令 WS + 一个 PC；双 PC 热切期同时存在两个实例）
// ---------------------------------------------------------------------------

type mediaSession struct {
	label string
	log   *slog.Logger
	ws    *wsClient
	conn  *websocket.Conn
	pc    *webrtc.PeerConnection
	track *webrtc.TrackLocalStaticRTP

	// sending 为 true 时本会话是当前发送会话（CUTOVER 语义：同一时刻仅一个
	// 会话在发送，禁止长期双发，docs 09 M.3）。
	sending   atomic.Bool
	connected chan struct{}
	closed    atomic.Bool

	sent          atomic.Int64
	recv          atomic.Int64
	tracks        atomic.Int64
	firstRecvNano atomic.Int64
	lastRecvNano  atomic.Int64

	stop     chan struct{}
	stopOnce sync.Once
}

// newMediaSession 连接 SFU 信令并建 PC；ready→offer→answer→ICE 由内部 readLoop 驱动。
func newMediaSession(log *slog.Logger, label, wsURL, token string) (*mediaSession, error) {
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial sfu ws: %w", err)
	}
	s := &mediaSession{
		label:     label,
		log:       log.With("session", label),
		ws:        &wsClient{conn: conn},
		conn:      conn,
		connected: make(chan struct{}),
		stop:      make(chan struct{}),
	}
	if err := s.ws.send("auth", map[string]any{"token": token}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send auth: %w", err)
	}

	me, err := owlsfu.NewMediaEngine()
	if err != nil {
		conn.Close()
		return nil, err
	}
	ir := &interceptor.Registry{}
	if err := webrtc.ConfigureRTCPReports(ir); err != nil {
		conn.Close()
		return nil, err
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("new pc: %w", err)
	}
	s.pc = pc

	s.track, err = webrtc.NewTrackLocalStaticRTP(owlsfu.OpusCodec, "audio", "bot")
	if err != nil {
		s.close()
		return nil, err
	}
	sender, err := pc.AddTrack(s.track)
	if err != nil {
		s.close()
		return nil, err
	}
	go func() { // drain RTCP
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		s.tracks.Add(1)
		s.log.Info("downstream track", "id", remote.ID(), "stream", remote.StreamID())
		go func() {
			buf := make([]byte, 1500)
			for {
				if _, _, err := remote.Read(buf); err != nil {
					return
				}
				now := time.Now().UnixNano()
				s.firstRecvNano.CompareAndSwap(0, now)
				s.lastRecvNano.Store(now)
				s.recv.Add(1)
			}
		}()
	})
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		s.log.Info("pc state", "state", st.String())
		if st == webrtc.PeerConnectionStateConnected {
			select {
			case <-s.connected:
			default:
				close(s.connected)
			}
		}
	})
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		s.ws.send("ice", map[string]any{
			"candidate": init.Candidate, "sdp_mid": init.SDPMid, "sdp_mline_index": init.SDPMLineIndex,
		})
	})

	go s.readLoop()
	go s.sendLoop()
	go s.pingLoop()
	return s, nil
}

// readLoop SFU → 客户端信令（协议同直连模式）；出错/被关（如源节点 CLEANUP 下发
// closed{MIGRATED} 或节点死亡）只结束本会话，不影响 bot 生命周期。
func (s *mediaSession) readLoop() {
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			s.log.Info("sfu ws closed", "err", err.Error())
			s.close()
			return
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			continue
		}
		switch f.Op {
		case "ready":
			offer, err := s.pc.CreateOffer(nil)
			if err == nil {
				err = s.pc.SetLocalDescription(offer)
			}
			if err != nil {
				s.log.Error("create offer failed", "err", err)
				s.close()
				return
			}
			s.ws.send("offer", map[string]any{"sdp": offer.SDP})
		case "answer":
			var d struct {
				SDP string `json:"sdp"`
			}
			json.Unmarshal(f.D, &d)
			if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: d.SDP}); err != nil {
				s.log.Warn("set remote answer failed", "err", err)
			}
		case "offer": // SFU renegotiation
			var d struct {
				SDP string `json:"sdp"`
			}
			json.Unmarshal(f.D, &d)
			if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: d.SDP}); err != nil {
				s.log.Warn("set remote offer failed", "err", err)
				continue
			}
			answer, err := s.pc.CreateAnswer(nil)
			if err == nil {
				err = s.pc.SetLocalDescription(answer)
			}
			if err != nil {
				s.log.Warn("create answer failed", "err", err)
				continue
			}
			s.ws.send("answer", map[string]any{"sdp": answer.SDP})
		case "ice":
			var d struct {
				Candidate     string  `json:"candidate"`
				SDPMid        *string `json:"sdp_mid"`
				SDPMLineIndex *uint16 `json:"sdp_mline_index"`
			}
			json.Unmarshal(f.D, &d)
			if d.Candidate != "" {
				s.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: d.Candidate, SDPMid: d.SDPMid, SDPMLineIndex: d.SDPMLineIndex})
			}
		case "closed":
			s.log.Info("closed by sfu", "d", string(f.D))
			s.close()
			return
		}
	}
}

// sendLoop 连通后按 20ms 发模拟 Opus RTP；仅当 sending=true 时真正写包
// （CUTOVER 前新会话静默待命，切换后旧会话立即停发，docs 09 M.3）。
func (s *mediaSession) sendLoop() {
	select {
	case <-s.connected:
	case <-s.stop:
		return
	}
	payload := make([]byte, 20)
	rand.Read(payload)
	seq := uint16(0)
	ts := uint32(0)
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
		}
		seq++
		ts += 960
		if !s.sending.Load() {
			continue
		}
		pkt := &rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: ts},
			Payload: payload,
		}
		if err := s.track.WriteRTP(pkt); err != nil {
			return
		}
		s.sent.Add(1)
	}
}

func (s *mediaSession) pingLoop() {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			if err := s.ws.send("ping", map[string]any{}); err != nil {
				return
			}
		}
	}
}

func (s *mediaSession) close() {
	s.stopOnce.Do(func() {
		s.closed.Store(true)
		s.sending.Store(false)
		close(s.stop)
		s.pc.Close()
		s.conn.Close()
	})
}

// waitFirstRecv 等待本会话收到第一个下行 RTP（迁移后音频恢复的判据）。
func (s *mediaSession) waitFirstRecv(timeout time.Duration) (int64, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if v := s.firstRecvNano.Load(); v != 0 {
			return v, true
		}
		select {
		case <-s.stop:
			return 0, false
		case <-time.After(20 * time.Millisecond):
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// Server 模式主体
// ---------------------------------------------------------------------------

type serverBot struct {
	log  *slog.Logger
	f    flags
	http *http.Client
	base string // 规范化的 server URL（无尾斜杠）

	accessToken string
	userID      string

	mu         sync.Mutex
	sessions   []*mediaSession // 全部历史会话（统计聚合用）
	active     *mediaSession   // 当前发送会话
	migrations int
	muteGaps   []int64 // 每次热切的静音窗口 ms
	seq        int     // 会话编号
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// api 发 JSON 请求并解析响应；非 2xx 返回错误。
func (b *serverBot) api(method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, b.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.accessToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.accessToken)
	}
	resp, err := b.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var ae apiError
		json.Unmarshal(raw, &ae)
		return fmt.Errorf("%s %s: HTTP %d %s %s", method, path, resp.StatusCode, ae.Error.Code, ae.Error.Message)
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

type joinResponse struct {
	Token           string `json:"token"`
	NodeID          string `json:"node_id"`
	AdvertiseWssURL string `json:"advertise_wss_url"`
	SessionID       string `json:"session_id"`
}

// runServerMode --server-url 模式入口。
func runServerMode(log *slog.Logger, f flags) error {
	b := &serverBot{
		log:  log,
		f:    f,
		http: &http.Client{Timeout: 10 * time.Second},
		base: strings.TrimRight(f.serverURL, "/"),
	}

	// 1. 登录（可选注册；注册冲突时回退登录，便于脚本幂等重跑）
	if f.signup {
		email := f.email
		if email == "" {
			email = f.username + "@loadbot.test"
		}
		var auth struct {
			AccessToken string `json:"access_token"`
			User        struct {
				ID string `json:"id"`
			} `json:"user"`
		}
		err := b.api("POST", "/gapi/v1/auth/signup", map[string]any{
			"username": f.username, "email": email, "password": f.password,
		}, &auth)
		if err != nil {
			log.Info("signup failed, fallback to login", "err", err.Error())
		} else {
			b.accessToken, b.userID = auth.AccessToken, auth.User.ID
		}
	}
	if b.accessToken == "" {
		var auth struct {
			AccessToken string `json:"access_token"`
			User        struct {
				ID string `json:"id"`
			} `json:"user"`
		}
		if err := b.api("POST", "/gapi/v1/auth/login", map[string]any{
			"identifier": f.username, "password": f.password,
		}, &auth); err != nil {
			return fmt.Errorf("login: %w", err)
		}
		b.accessToken, b.userID = auth.AccessToken, auth.User.ID
	}
	log.Info("logged in", "user_id", b.userID)

	// 2. RTT 上报（可选，引导调度落点，docs 10 §7）
	if f.rttReport != "" {
		samples := []map[string]any{}
		for _, part := range strings.Split(f.rttReport, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
			if len(kv) != 2 {
				continue
			}
			ms, err := strconv.ParseFloat(kv[1], 64)
			if err != nil {
				continue
			}
			samples = append(samples, map[string]any{"node_id": kv[0], "rtt_ms": ms})
		}
		if len(samples) > 0 {
			if err := b.api("POST", "/gapi/v1/voice/rtt", map[string]any{"samples": samples}, nil); err != nil {
				return fmt.Errorf("rtt report: %w", err)
			}
		}
	}

	// 3. voice/join → 首个媒体会话
	var joined joinResponse
	if err := b.api("POST", "/gapi/v1/voice/join", map[string]any{
		"guild_id": f.guild, "channel_id": f.channel,
	}, &joined); err != nil {
		return fmt.Errorf("voice join: %w", err)
	}
	log.Info("voice joined", "node_id", joined.NodeID, "wss", joined.AdvertiseWssURL)
	first, err := b.startSession(joined.AdvertiseWssURL, joined.Token)
	if err != nil {
		return fmt.Errorf("media session: %w", err)
	}
	first.sending.Store(true)
	b.mu.Lock()
	b.active = first
	b.mu.Unlock()

	// 4. Gateway WS（用户端平面）：迁移事件驱动双 PC 热切
	gwDone := make(chan error, 1)
	stop := make(chan struct{})
	go func() { gwDone <- b.gatewayLoop(stop) }()
	go b.statsLoop(stop)

	select {
	case err := <-gwDone:
		if err != nil {
			log.Error("gateway loop ended", "err", err)
		}
	case <-time.After(f.duration):
	}
	close(stop)

	// 5. 退出统计
	sent, recv, tracks := b.totals()
	stats := map[string]any{
		"sent": sent, "recv": recv, "tracks": tracks,
		"migrations": b.migrations, "mute_gaps_ms": b.muteGaps,
	}
	if n := len(b.muteGaps); n > 0 {
		stats["mute_gap_ms"] = b.muteGaps[n-1]
	}
	fmt.Println(mustJSON(stats))
	if f.expectRecv && recv == 0 {
		return fmt.Errorf("expected downstream RTP but received none")
	}
	return nil
}

func (b *serverBot) startSession(wssURL, token string) (*mediaSession, error) {
	b.mu.Lock()
	b.seq++
	label := fmt.Sprintf("pc%d", b.seq)
	b.mu.Unlock()
	s, err := newMediaSession(b.log, label, wssURL, token)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.sessions = append(b.sessions, s)
	b.mu.Unlock()
	return s, nil
}

func (b *serverBot) totals() (sent, recv, tracks int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.sessions {
		sent += s.sent.Load()
		recv += s.recv.Load()
		tracks += s.tracks.Load()
	}
	return
}

// statsLoop 每 2s 打印一行 JSON 统计（e2e 脚本采样 recv 增长用）。
func (b *serverBot) statsLoop(stop <-chan struct{}) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			sent, recv, tracks := b.totals()
			fmt.Println(mustJSON(map[string]any{"sent": sent, "recv": recv, "tracks": tracks, "migrations": b.migrations}))
		}
	}
}

// ---------------------------------------------------------------------------
// Gateway（用户端 WS 平面）
// ---------------------------------------------------------------------------

type gatewayFrame struct {
	Op string          `json:"op"`
	T  string          `json:"t,omitempty"`
	S  int64           `json:"s,omitempty"`
	D  json.RawMessage `json:"d,omitempty"`
}

// gatewayLoop HELLO → IDENTIFY → READY → 心跳 + DISPATCH 事件处理。
func (b *serverBot) gatewayLoop(stop <-chan struct{}) error {
	gwURL, err := url.Parse(b.base)
	if err != nil {
		return err
	}
	scheme := "ws"
	if gwURL.Scheme == "https" {
		scheme = "wss"
	}
	wsURL := fmt.Sprintf("%s://%s/gapi/v1/gateway", scheme, gwURL.Host)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial gateway: %w", err)
	}
	defer conn.Close()
	ws := &wsClient{conn: conn}

	send := func(op string, d any) error {
		return ws.send(op, d)
	}
	// wsClient.send 使用 {"op","d"} 结构，与 Gateway 帧兼容。

	// HELLO
	var hello gatewayFrame
	if err := conn.ReadJSON(&hello); err != nil || hello.Op != "HELLO" {
		return fmt.Errorf("expect HELLO, got %v (err=%v)", hello.Op, err)
	}
	var helloD struct {
		HeartbeatIntervalMS int64 `json:"heartbeat_interval_ms"`
	}
	json.Unmarshal(hello.D, &helloD)
	if helloD.HeartbeatIntervalMS <= 0 {
		helloD.HeartbeatIntervalMS = 30000
	}

	// IDENTIFY → READY
	if err := send("IDENTIFY", map[string]any{"token": b.accessToken}); err != nil {
		return err
	}
	var ready gatewayFrame
	if err := conn.ReadJSON(&ready); err != nil || ready.Op != "READY" {
		return fmt.Errorf("expect READY, got %v (err=%v)", ready.Op, err)
	}
	b.log.Info("gateway ready")

	// 心跳
	hbStop := make(chan struct{})
	defer close(hbStop)
	go func() {
		t := time.NewTicker(time.Duration(helloD.HeartbeatIntervalMS) * time.Millisecond / 2)
		defer t.Stop()
		for {
			select {
			case <-hbStop:
				return
			case <-stop:
				return
			case <-t.C:
				if err := send("HEARTBEAT", map[string]any{}); err != nil {
					return
				}
			}
		}
	}()

	// DISPATCH 事件
	for {
		select {
		case <-stop:
			return nil
		default:
		}
		var f gatewayFrame
		if err := conn.ReadJSON(&f); err != nil {
			return fmt.Errorf("gateway read: %w", err)
		}
		if f.Op != "DISPATCH" {
			continue
		}
		switch f.T {
		case "VOICE_MIGRATING":
			b.log.Info("VOICE_MIGRATING", "d", string(f.D))
			fmt.Println(mustJSON(map[string]any{"event": "voice_migrating", "at_ms": time.Now().UnixMilli()}))
		case "VOICE_SERVER_UPDATE":
			var d struct {
				GuildID     string `json:"guild_id"`
				ChannelID   string `json:"channel_id"`
				NodeID      string `json:"node_id"`
				SFUEndpoint string `json:"sfu_endpoint"`
				Token       string `json:"token"`
				MigrationID string `json:"migration_id"`
			}
			if err := json.Unmarshal(f.D, &d); err != nil {
				b.log.Warn("bad VOICE_SERVER_UPDATE", "err", err)
				continue
			}
			b.log.Info("VOICE_SERVER_UPDATE", "node_id", d.NodeID, "migration_id", d.MigrationID)
			go b.hotSwitch(d.SFUEndpoint, d.Token, d.MigrationID, d.NodeID)
		case "VOICE_MIGRATED":
			b.log.Info("VOICE_MIGRATED", "d", string(f.D))
		}
	}
}

// hotSwitch 双 PC 热切（docs 09 M.3 / 08 G.3）：
//  1. 新建第二个 PC 连新节点（旧 PC 继续收发）；
//  2. 新 PC 连通 → CUTOVER：发送切到新 PC，回 ack；
//  3. 新 PC 首个下行包 → 记录静音窗口（旧 PC 最后收包 → 新 PC 首包）→ 拆旧 PC。
func (b *serverBot) hotSwitch(wssURL, token, migrationID, nodeID string) {
	start := time.Now()
	next, err := b.startSession(wssURL, token)
	if err != nil {
		b.log.Error("hot switch: new session failed", "err", err)
		return
	}
	select {
	case <-next.connected:
	case <-next.stop:
		b.log.Error("hot switch: new session closed before connected")
		return
	case <-time.After(15 * time.Second):
		b.log.Error("hot switch: new pc connect timeout")
		next.close()
		return
	}

	// CUTOVER：发送从旧 PC 原子切到新 PC（禁止双发，docs 09 M.3）。
	b.mu.Lock()
	old := b.active
	b.active = next
	b.migrations++
	b.mu.Unlock()
	if old != nil {
		old.sending.Store(false)
	}
	next.sending.Store(true)
	b.log.Info("cutover: sending switched", "to_node", nodeID, "connect_ms", time.Since(start).Milliseconds())

	// 迁移完成确认（docs 09 §7：驱动 Server 状态机 CONNECT → CUTOVER）。
	if migrationID != "" {
		if err := b.api("POST", "/gapi/v1/voice/migrations/"+migrationID+"/ack", map[string]any{}, nil); err != nil {
			b.log.Warn("migration ack failed（可能已超时自动推进）", "err", err.Error())
		}
	}

	// 静音窗口：旧 PC 最后一个收包 → 新 PC 第一个收包。
	firstRecv, ok := next.waitFirstRecv(30 * time.Second)
	gap := int64(-1)
	if ok {
		gap = 0
		if old != nil {
			if oldLast := old.lastRecvNano.Load(); oldLast != 0 && firstRecv > oldLast {
				gap = (firstRecv - oldLast) / int64(time.Millisecond)
			}
		}
	}
	b.mu.Lock()
	b.muteGaps = append(b.muteGaps, gap)
	b.mu.Unlock()
	fmt.Println(mustJSON(map[string]any{
		"event": "migrated", "migration_id": migrationID, "to_node": nodeID,
		"mute_gap_ms": gap, "at_ms": time.Now().UnixMilli(),
	}))

	// CLEANUP：拆旧 PC（源节点侧同时会经 MigrateParticipants EXECUTE 摘会话）。
	if old != nil {
		old.close()
	}
}
