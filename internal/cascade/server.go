package cascade

import (
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/room"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
)

const (
	handshakeTimeout = 10 * time.Second
	dialBackoffMax   = 30 * time.Second
)

// New 创建级联管理器并启动 mTLS 信令监听（15 BG.2/BH：级联端口默认 tcp/8843）。
func New(log *slog.Logger, cfg Config, engine *owlsfu.Engine, rooms *room.Manager,
	metrics *observability.Metrics) (*Manager, error) {
	if cfg.GetCert == nil {
		staticCert := cfg.Cert
		cfg.GetCert = func() *tls.Certificate { return &staticCert }
	}
	m := &Manager{
		log:     log.With("mod", "cascade"),
		cfg:     cfg,
		engine:  engine,
		rooms:   rooms,
		metrics: metrics,
		states:  make(map[string]*roomState),
		dialing: make(map[string]bool),
		stopCh:  make(chan struct{}),
	}
	// mTLS：双向节点证书校验（复用 enrollment 证书/CA，禁止裸互信 08 E.4）；
	// 证书经 GetCertificate 回调取用，续期热更新后新握手即用新证书。
	ln, err := tls.Listen("tcp", cfg.Listen, &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return cfg.GetCert(), nil
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  cfg.CAPool,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		return nil, fmt.Errorf("cascade listen %s: %w", cfg.Listen, err)
	}
	m.ln = ln
	m.log.Info("cascade signaling listening (mTLS)", "addr", ln.Addr().String())
	go m.acceptLoop()
	go m.watchdog()
	return m, nil
}

// Addr 返回实际监听地址（测试用 ephemeral 端口）。
func (m *Manager) Addr() string { return m.ln.Addr().String() }

// Close 停止监听与全部边会话。
func (m *Manager) Close() {
	if !m.closed.CompareAndSwap(false, true) {
		return
	}
	m.stopOnce.Do(func() { close(m.stopCh) })
	_ = m.ln.Close()

	m.mu.Lock()
	var sessions []*edgeSession
	for _, rs := range m.states {
		for _, s := range rs.sessions {
			sessions = append(sessions, s)
		}
	}
	m.mu.Unlock()
	for _, s := range sessions {
		s.close("cascade manager shutting down")
	}
}

// acceptLoop 接受 child 侧入站连接。
func (m *Manager) acceptLoop() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			if m.closed.Load() {
				return
			}
			m.log.Warn("cascade accept failed", "err", err)
			continue
		}
		go m.handleInbound(conn)
	}
}

// handleInbound parent 侧握手（BG.2 三重校验：mTLS 证书 + 边授权 + cascade token）。
func (m *Manager) handleInbound(conn net.Conn) {
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		conn.Close()
		return
	}
	if err := tlsConn.Handshake(); err != nil {
		m.metrics.CascadeHandshakeFailed.WithLabelValues("tls").Inc()
		m.log.Warn("cascade tls handshake failed", "remote", conn.RemoteAddr().String(), "err", err)
		conn.Close()
		return
	}
	certs := tlsConn.ConnectionState().PeerCertificates
	if len(certs) == 0 {
		m.metrics.CascadeHandshakeFailed.WithLabelValues("no_client_cert").Inc()
		conn.Close()
		return
	}
	// 节点证书 CN = node_id（03 §4 签发规则）
	peerNodeID := certs[0].Subject.CommonName

	dec := json.NewDecoder(conn)
	var hello frame
	if err := dec.Decode(&hello); err != nil || hello.T != frameHello {
		m.metrics.CascadeHandshakeFailed.WithLabelValues("bad_hello").Inc()
		conn.Close()
		return
	}

	edge, reason := m.authorizeInbound(&hello, peerNodeID)
	enc := json.NewEncoder(conn)
	if reason != "" {
		m.metrics.CascadeHandshakeFailed.WithLabelValues(reason).Inc()
		m.log.Warn("cascade inbound rejected", "remote", conn.RemoteAddr().String(),
			"peer_cn", peerNodeID, "room", hello.RoomID, "epoch", hello.Epoch, "reason", reason)
		_ = enc.Encode(&frame{T: frameHelloAck, OK: false, Reason: reason})
		conn.Close()
		return
	}
	if err := enc.Encode(&frame{T: frameHelloAck, OK: true}); err != nil {
		conn.Close()
		return
	}
	_ = conn.SetDeadline(time.Time{})

	s := newEdgeSession(m, edge, true, conn)
	if !m.edgeEstablished(s) {
		s.close("edge no longer desired")
		return
	}
	s.runWith(dec)
}

// authorizeInbound 校验 hello：边授权（当前 epoch 边集）+ 证书身份 + cascade token。
func (m *Manager) authorizeInbound(hello *frame, peerNodeID string) (Edge, string) {
	if hello.Child != peerNodeID {
		return Edge{}, "cert_mismatch" // hello 声称的 child 与证书身份不符
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rs := m.states[hello.RoomID]
	if rs == nil {
		return Edge{}, "unknown_room"
	}
	if hello.Epoch != rs.epoch {
		return Edge{}, "epoch_mismatch"
	}
	edge, ok := rs.edges[m.cfg.NodeID+"|"+hello.Child]
	if !ok {
		return Edge{}, "edge_not_authorized" // 未经 SetCascadeEdges 授权 = 私自互联，拒绝（08 C.3）
	}
	// cascade token 三重校验（BG.2）：
	//  1) 等值比较：Server 在 SetCascadeEdges 中对边两端下发同一 token，
	//     child 出示的必须与本节点持有的副本一致（恒时比较防侧信道）；
	//  2) 验签（VerifyToken 已注入时）：EdDSA 签名 + 短 TTL + room/epoch/edge 绑定，
	//     防持证节点跨房/跨边滥用凭证拉流（08 E.4）。
	if edge.Token != "" {
		if subtle.ConstantTimeCompare([]byte(edge.Token), []byte(hello.Token)) != 1 {
			return Edge{}, "token_mismatch"
		}
	} else if hello.Token == "" {
		m.log.Warn("cascade edge without token (mTLS + edge authorization only)",
			"room", hello.RoomID, "child", hello.Child)
	}
	if m.cfg.VerifyToken != nil && hello.Token != "" {
		if err := m.cfg.VerifyToken(hello.Token, hello.RoomID, hello.Epoch,
			m.cfg.NodeID, hello.Child); err != nil {
			m.log.Warn("cascade token signature verification failed",
				"room", hello.RoomID, "child", hello.Child, "err", err)
			return Edge{}, "token_invalid"
		}
	}
	return edge, ""
}

// ---- child 侧拨号 ----

// childDialLoop 维护一条 child 边：拨号→握手→会话循环，断线退避重连，
// 边被撤销（epoch 替换/边集移除）后退出。
func (m *Manager) childDialLoop(roomID, key string) {
	defer func() {
		m.mu.Lock()
		delete(m.dialing, roomID+"|"+key)
		m.mu.Unlock()
	}()
	backoff := time.Second
	for {
		if m.closed.Load() {
			return
		}
		m.mu.Lock()
		rs := m.states[roomID]
		var edge Edge
		desired := false
		if rs != nil {
			if e, ok := rs.edges[key]; ok && e.ChildNodeID == m.cfg.NodeID {
				edge, desired = e, true
			}
		}
		m.mu.Unlock()
		if !desired {
			return
		}

		s, err := m.dialEdge(edge)
		if err != nil {
			m.metrics.CascadeHandshakeFailed.WithLabelValues("dial").Inc()
			m.log.Warn("cascade dial failed", "room", roomID, "edge", key,
				"endpoint", edge.ParentEndpoint, "err", err, "backoff", backoff.String())
			select {
			case <-m.stopCh:
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > dialBackoffMax {
				backoff = dialBackoffMax
			}
			continue
		}
		backoff = time.Second
		if !m.edgeEstablished(s) {
			s.close("edge no longer desired")
			continue
		}
		s.runWith(s.handshakeDec) // 阻塞至边断开，然后回到循环重拨
	}
}

// dialEdge 拨通 parent 级联端口并完成 hello 握手。
func (m *Manager) dialEdge(edge Edge) (*edgeSession, error) {
	dialer := &net.Dialer{Timeout: handshakeTimeout}
	// ServerName = parent node_id：节点证书 SAN 含 node_id（03 §4），
	// 由 TLS 层完成「连的确实是该 parent 节点」的身份校验。
	conn, err := tls.DialWithDialer(dialer, "tcp", edge.ParentEndpoint, &tls.Config{
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return m.cfg.GetCert(), nil
		},
		RootCAs:    m.cfg.CAPool,
		ServerName: edge.ParentNodeID,
		MinVersion: tls.VersionTLS12,
	})
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	enc := json.NewEncoder(conn)
	if err := enc.Encode(&frame{T: frameHello, RoomID: edge.RoomID, Epoch: edge.Epoch,
		Child: m.cfg.NodeID, Token: edge.Token}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send hello: %w", err)
	}
	dec := json.NewDecoder(conn)
	var ack frame
	if err := dec.Decode(&ack); err != nil {
		conn.Close()
		return nil, fmt.Errorf("recv hello_ack: %w", err)
	}
	if ack.T != frameHelloAck || !ack.OK {
		conn.Close()
		return nil, fmt.Errorf("hello rejected: %s", ack.Reason)
	}
	_ = conn.SetDeadline(time.Time{})

	s := newEdgeSession(m, edge, false, conn)
	s.handshakeDec = dec // 复用握手期 decoder（可能已缓冲后续帧）
	return s, nil
}
