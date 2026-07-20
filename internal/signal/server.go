// Package signal 实现客户端 WSS 信令端点（docs/协议/README.md §2）与 /rtt 探测。
package signal

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/owlspeak/owl-sfu/internal/auth"
	"github.com/owlspeak/owl-sfu/internal/observability"
	"github.com/owlspeak/owl-sfu/internal/room"
)

const (
	authTimeout  = 5 * time.Second
	readIdleTime = 60 * time.Second // ping 建议 15s，4 倍容忍
	rttPerSecond = 10
)

// Server 为信令 HTTP/WS 服务。
type Server struct {
	log      *slog.Logger
	verifier *auth.Verifier
	mgr      *room.Manager
	metrics  *observability.Metrics

	srv      *http.Server
	upgrader websocket.Upgrader
	rttLimit *ipRateLimiter

	noTLS    bool
	certFile string
	keyFile  string
}

// NewServer 构建信令服务（同端口承载 /ws /rtt /healthz /metrics /debug/pprof）。
func NewServer(log *slog.Logger, listen string, noTLS bool, certFile, keyFile string,
	verifier *auth.Verifier, mgr *room.Manager, metrics *observability.Metrics) *Server {
	s := &Server{
		log:      log,
		verifier: verifier,
		mgr:      mgr,
		metrics:  metrics,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// M1：信令由 token 鉴权，不做 Origin 白名单
			CheckOrigin: func(*http.Request) bool { return true },
		},
		rttLimit: newIPRateLimiter(rttPerSecond),
		noTLS:    noTLS,
		certFile: certFile,
		keyFile:  keyFile,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /rtt", s.handleRTT)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	observability.Register(mux, metrics)
	s.srv = &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s
}

// Start 后台启动监听（no_tls=true 时纯 HTTP/WS）。
func (s *Server) Start() {
	go func() {
		var err error
		if s.noTLS {
			s.log.Warn("signal server running WITHOUT TLS (no_tls=true)", "addr", s.srv.Addr)
			err = s.srv.ListenAndServe()
		} else {
			s.log.Info("signal server listening (wss)", "addr", s.srv.Addr)
			err = s.srv.ListenAndServeTLS(s.certFile, s.keyFile)
		}
		if err != nil && err != http.ErrServerClosed {
			s.log.Error("signal server exited", "err", err)
		}
	}()
}

// Serve 在给定 listener 上服务（无 TLS；测试/嵌入用）。
func (s *Server) Serve(l net.Listener) error {
	return s.srv.Serve(l)
}

// Shutdown 优雅关闭监听。
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// handleHealthz 为存活探针。
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// handleRTT 为无鉴权 RTT 探测，每 IP 每秒 10 次限速。
func (s *Server) handleRTT(w http.ResponseWriter, r *http.Request) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	if !s.rttLimit.allow(ip) {
		w.WriteHeader(http.StatusTooManyRequests)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// 各 op 的 d 载荷。
type authD struct {
	Token string `json:"token"`
}

type sdpD struct {
	SDP string `json:"sdp"`
}

type iceD struct {
	Candidate     string  `json:"candidate"`
	SDPMid        *string `json:"sdp_mid"`
	SDPMLineIndex *uint16 `json:"sdp_mline_index"`
}

type subD struct {
	UserID string `json:"user_id"`
}

// setLayerD 为 set_layer 上行帧载荷（屏幕 simulcast 选层，docs 14 BA.3）：
// layer ∈ high|medium|low，缺省 high；发布端未开 simulcast 时忽略选层。
type setLayerD struct {
	UserID string `json:"user_id"`
	Layer  string `json:"layer"`
}

type inFrame struct {
	Op string          `json:"op"`
	D  json.RawMessage `json:"d"`
}

// handleWS 升级连接并进入会话循环。
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.log.Debug("ws upgrade failed", "err", err)
		return
	}
	s.metrics.SignalConnects.Inc()
	go s.session(newWSConn(conn), conn)
}

// readFrame 读取一帧并解析。
func readFrame(conn *websocket.Conn) (*inFrame, error) {
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var f inFrame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// session 执行单连接生命周期：auth → ready → 消息循环。
func (s *Server) session(wc *wsConn, conn *websocket.Conn) {
	// 5s 内必须收到 auth 首帧
	conn.SetReadDeadline(time.Now().Add(authTimeout))
	f, err := readFrame(conn)
	if err != nil || f.Op != "auth" {
		s.metrics.SignalAuthFailures.WithLabelValues(room.CloseAuthTimeout).Inc()
		wc.CloseWithReason(room.CloseAuthTimeout, "auth frame not received in time")
		return
	}
	var ad authD
	if err := json.Unmarshal(f.D, &ad); err != nil || ad.Token == "" {
		s.metrics.SignalAuthFailures.WithLabelValues(auth.CodeTokenInvalid).Inc()
		wc.CloseWithReason(auth.CodeTokenInvalid, "malformed auth frame")
		return
	}
	tok, err := s.verifier.Verify(ad.Token)
	if err != nil {
		code := auth.ErrCode(err)
		s.metrics.SignalAuthFailures.WithLabelValues(code).Inc()
		s.log.Info("auth rejected", "code", code, "err", err)
		wc.CloseWithReason(code, "token verification failed")
		return
	}

	p, snapshot, err := s.mgr.Join(tok, wc)
	if err != nil {
		code := room.CloseNodeDraining
		msg := err.Error()
		if je, ok := err.(*room.JoinError); ok {
			code, msg = je.Code, je.Message
		}
		s.metrics.SignalAuthFailures.WithLabelValues(code).Inc()
		wc.CloseWithReason(code, msg)
		return
	}

	if err := wc.Send("ready", map[string]any{
		"session_id":   p.SID(),
		"room_id":      p.RoomID(),
		"participants": snapshot,
	}); err != nil {
		p.Close(room.CloseDisconnected, "failed to deliver ready")
		return
	}

	s.loop(wc, conn, p)
}

// loop 处理 auth 后的信令消息，退出即关闭会话。
func (s *Server) loop(wc *wsConn, conn *websocket.Conn, p *room.Participant) {
	defer p.Close(room.CloseDisconnected, "websocket closed")
	for {
		conn.SetReadDeadline(time.Now().Add(readIdleTime))
		f, err := readFrame(conn)
		if err != nil {
			return
		}
		switch f.Op {
		case "auth":
			// 重复 auth = token 刷新
			var ad authD
			if err := json.Unmarshal(f.D, &ad); err != nil {
				continue
			}
			tok, err := s.verifier.Verify(ad.Token)
			if err != nil {
				code := auth.ErrCode(err)
				wc.CloseWithReason(code, "refresh token verification failed")
				return
			}
			if err := p.RefreshToken(tok); err != nil {
				wc.CloseWithReason(room.CloseRoomMismatch, err.Error())
				return
			}
		case "offer":
			var d sdpD
			if err := json.Unmarshal(f.D, &d); err != nil {
				continue
			}
			answer, err := p.HandleOffer(d.SDP)
			if err != nil {
				s.log.Warn("handle offer failed", "sid", p.SID(), "err", err)
				continue
			}
			wc.Send("answer", map[string]any{"sdp": answer})
		case "answer":
			var d sdpD
			if err := json.Unmarshal(f.D, &d); err != nil {
				continue
			}
			if err := p.HandleAnswer(d.SDP); err != nil {
				s.log.Warn("handle answer failed", "sid", p.SID(), "err", err)
			}
		case "ice":
			var d iceD
			if err := json.Unmarshal(f.D, &d); err != nil || d.Candidate == "" {
				continue
			}
			if err := p.AddICECandidate(d.Candidate, d.SDPMid, d.SDPMLineIndex); err != nil {
				s.log.Debug("add ice candidate failed", "sid", p.SID(), "err", err)
			}
		case "subscribe":
			var d subD
			if err := json.Unmarshal(f.D, &d); err != nil || d.UserID == "" {
				continue
			}
			p.Subscribe(d.UserID)
		case "unsubscribe":
			var d subD
			if err := json.Unmarshal(f.D, &d); err != nil || d.UserID == "" {
				continue
			}
			p.Unsubscribe(d.UserID)
		case "set_layer":
			var d setLayerD
			if err := json.Unmarshal(f.D, &d); err != nil || d.UserID == "" || d.Layer == "" {
				continue
			}
			p.SetScreenLayer(d.UserID, d.Layer)
		case "ping":
			wc.Send("pong", map[string]any{})
		default:
			s.log.Debug("unknown op ignored", "op", f.Op)
		}
	}
}
