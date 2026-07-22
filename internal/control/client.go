// Package control 实现 mTLS gRPC 控制通道：注册、心跳、指令处理与 RoomEvent 上报。
package control

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
	"github.com/owlspeak/owl-sfu/internal/auth"
	"github.com/owlspeak/owl-sfu/internal/observability"
	"github.com/owlspeak/owl-sfu/internal/stats"
	"github.com/owlspeak/owl-sfu/internal/update"
)

const (
	defaultHeartbeatMs = 5000
	backoffInitial     = time.Second
	backoffMax         = 30 * time.Second
	ackWindow          = 1024
	eventQueueSize     = 1024
)

// Backend 为指令执行后端（由 room.Manager + auth.Verifier + cascade/migrate 适配）。
type Backend interface {
	EnsureRoom(roomID string)
	CloseRoom(roomID string)
	// DisconnectUser 返回实际断开的会话数。
	DisconnectUser(roomID, userID, sessionID, reason string) int
	// RevokeSession 吊销并断开会话，返回是否有在线会话被断开。
	RevokeSession(sessionID, jti string) bool
	UpdateCaps(roomID, sessionID string, caps []string) error
	SetDraining(v bool)

	// ---- 级联（M3，docs 08）----
	SetAnchorLease(roomID, anchorNodeID string, epoch uint64, expireUnixMs int64) error
	SetCascadeEdges(roomID string, epoch uint64, edges []*owlsfuv1.CascadeEdge) error

	// ---- 热迁移（M4，docs 09）----
	// MigrateParticipantsMark MARK 阶段：标记 sid 迁出中（CUTOVER 前继续服务），
	// 返回实际标记数（不在线的 sid 幂等跳过）。
	MigrateParticipantsMark(migrationID, roomID string, sessionIDs []string) int
	// MigrateParticipants EXECUTE 阶段：迁出指定会话，返回实际断开数
	//（不在线的 sid 幂等跳过）。
	MigrateParticipants(migrationID, roomID string, sessionIDs []string, toNodeID string) int
}

// Client 为控制通道客户端（SFU 主动外连，断线指数退避重连）。
type Client struct {
	log         *slog.Logger
	addr        string
	nodeVersion string
	creds       credentials.TransportCredentials
	backend     Backend
	verifier    *auth.Verifier
	stats       *stats.Collector
	metrics     *observability.Metrics
	advertise   *owlsfuv1.NodeAdvertise

	acks *ackCache
	// outbox 承载 RoomEvent / EdgeStatus / DrainRequest 等异步上行消息，
	// 发送失败回灌等重连补发。
	outbox chan *owlsfuv1.NodeMessage
	// onRegisterAck 每次 RegisterAck 成功后回调（用于应用 Server 下发的审计上传配置等）。
	onRegisterAck func(ack *owlsfuv1.RegisterAck)
}

// SetOnRegisterAck 注册 RegisterAck 回调（可在 NewClient 之后、Run 之前调用）。
func (c *Client) SetOnRegisterAck(fn func(ack *owlsfuv1.RegisterAck)) {
	c.onRegisterAck = fn
}

// NewClient 构建控制通道客户端。getCert 以回调形式取节点证书（证书续期热更新：
// 重连/新握手即用新证书），可用 enroll.CertSource.Get 或静态闭包。
func NewClient(log *slog.Logger, addr, nodeVersion string, getCert func() *tls.Certificate, caPool *x509.CertPool,
	advertise *owlsfuv1.NodeAdvertise, backend Backend, verifier *auth.Verifier,
	st *stats.Collector, metrics *observability.Metrics) *Client {
	return &Client{
		log:         log,
		addr:        addr,
		nodeVersion: nodeVersion,
		creds: credentials.NewTLS(&tls.Config{
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return getCert(), nil
			},
			RootCAs:    caPool,
			MinVersion: tls.VersionTLS12,
		}),
		backend:   backend,
		verifier:  verifier,
		stats:     st,
		metrics:   metrics,
		advertise: advertise,
		acks:      newAckCache(ackWindow),
		outbox:    make(chan *owlsfuv1.NodeMessage, eventQueueSize),
	}
}

// ---- room.Events 实现：媒体连通/离开经此上报 Server ----

// ParticipantJoined 上报媒体连通。
func (c *Client) ParticipantJoined(roomID, sid, uid string) {
	c.enqueueEvent(&owlsfuv1.RoomEvent{
		RoomId:    roomID,
		Type:      owlsfuv1.RoomEvent_EVENT_TYPE_PARTICIPANT_JOINED,
		SessionId: sid,
		UserId:    uid,
		UnixMs:    time.Now().UnixMilli(),
	})
}

// ParticipantLeft 上报会话结束（幂等，Server 侧按 sid 去重）。
func (c *Client) ParticipantLeft(roomID, sid, uid, detail string) {
	c.enqueueEvent(&owlsfuv1.RoomEvent{
		RoomId:    roomID,
		Type:      owlsfuv1.RoomEvent_EVENT_TYPE_PARTICIPANT_LEFT,
		SessionId: sid,
		UserId:    uid,
		Detail:    detail,
		UnixMs:    time.Now().UnixMilli(),
	})
}

// ScreenTrackActive 上报屏幕轨发布生效（docs 14 BC.1 步骤 5：Server 据此 RESERVED→ACTIVE）。
func (c *Client) ScreenTrackActive(roomID, sid, uid string) {
	c.enqueueEvent(&owlsfuv1.RoomEvent{
		RoomId:    roomID,
		Type:      owlsfuv1.RoomEvent_EVENT_TYPE_SCREEN_TRACK_ACTIVE,
		SessionId: sid,
		UserId:    uid,
		UnixMs:    time.Now().UnixMilli(),
	})
}

// ScreenTrackEnded 上报屏幕轨结束（客户端停轨 / caps 收回；Server 据此释放屏幕坑）。
func (c *Client) ScreenTrackEnded(roomID, sid, uid string) {
	c.enqueueEvent(&owlsfuv1.RoomEvent{
		RoomId:    roomID,
		Type:      owlsfuv1.RoomEvent_EVENT_TYPE_SCREEN_TRACK_ENDED,
		SessionId: sid,
		UserId:    uid,
		UnixMs:    time.Now().UnixMilli(),
	})
}

// ReportEdgeStatus 上报级联边状态（cascade.Reporter 实现，08 §6.1）。
func (c *Client) ReportEdgeStatus(es *owlsfuv1.EdgeStatus) {
	c.enqueue(&owlsfuv1.NodeMessage{Payload: &owlsfuv1.NodeMessage_EdgeStatus{EdgeStatus: es}})
}

// RequestDrain 主动请求排空（SIGTERM 生命周期调用；编排权威在 Server）。
func (c *Client) RequestDrain(reason string) {
	c.enqueue(&owlsfuv1.NodeMessage{Payload: &owlsfuv1.NodeMessage_DrainRequest{
		DrainRequest: &owlsfuv1.DrainRequest{Reason: reason},
	}})
}

func (c *Client) enqueueEvent(ev *owlsfuv1.RoomEvent) {
	c.enqueue(&owlsfuv1.NodeMessage{Payload: &owlsfuv1.NodeMessage_RoomEvent{RoomEvent: ev}})
}

func (c *Client) enqueue(msg *owlsfuv1.NodeMessage) {
	select {
	case c.outbox <- msg:
	default:
		c.log.Warn("control outbox full, dropping message")
	}
}

// Run 维持控制通道直到 ctx 取消：断线 1s 起指数退避重连（上限 30s），重连后重新 Register。
func (c *Client) Run(ctx context.Context) {
	backoff := backoffInitial
	for {
		err := c.runOnce(ctx, &backoff)
		if ctx.Err() != nil {
			return
		}
		c.log.Warn("control channel disconnected, will reconnect", "err", err, "backoff", backoff.String())
		c.metrics.ControlReconnects.Inc()
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > backoffMax {
			backoff = backoffMax
		}
	}
}

// runOnce 完成一次「连接→注册→心跳/事件/指令」生命周期，返回断开原因。
func (c *Client) runOnce(ctx context.Context, backoff *time.Duration) error {
	conn, err := grpc.NewClient(c.addr, grpc.WithTransportCredentials(c.creds))
	if err != nil {
		return fmt.Errorf("grpc client: %w", err)
	}
	defer conn.Close()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := owlsfuv1.NewControlServiceClient(conn).Channel(streamCtx)
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	var sendMu sync.Mutex
	send := func(msg *owlsfuv1.NodeMessage) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(msg)
	}

	// 首帧 Register
	if err := send(&owlsfuv1.NodeMessage{Payload: &owlsfuv1.NodeMessage_Register{Register: &owlsfuv1.Register{
		NodeVersion: c.nodeVersion,
		Advertise:   c.advertise,
		Capacity:    c.stats.Capacity(),
	}}}); err != nil {
		return fmt.Errorf("send register: %w", err)
	}

	// 等 RegisterAck
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv register ack: %w", err)
	}
	ack := first.GetRegisterAck()
	if ack == nil {
		return errors.New("first server message is not RegisterAck")
	}
	heartbeatMs := ack.GetHeartbeatIntervalMs()
	if heartbeatMs == 0 {
		heartbeatMs = defaultHeartbeatMs
	}
	c.verifier.UpdateKeys(ack.GetMediaTokenKeys())
	*backoff = backoffInitial // 注册成功即重置退避
	c.log.Info("control channel registered", "node_id", ack.GetNodeId(), "heartbeat_ms", heartbeatMs)
	if c.onRegisterAck != nil {
		c.onRegisterAck(ack)
	}

	// 心跳
	go func() {
		t := time.NewTicker(time.Duration(heartbeatMs) * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-t.C:
				if err := send(&owlsfuv1.NodeMessage{Payload: &owlsfuv1.NodeMessage_Heartbeat{Heartbeat: &owlsfuv1.Heartbeat{
					Capacity:    c.stats.Capacity(),
					UnixMs:      time.Now().UnixMilli(),
					NodeVersion: c.nodeVersion,
				}}}); err != nil {
					return
				}
			}
		}
	}()

	// 异步上行消息（RoomEvent/EdgeStatus/DrainRequest）：发送失败回灌队列，等重连后补发
	go func() {
		for {
			select {
			case <-streamCtx.Done():
				return
			case msg := <-c.outbox:
				if err := send(msg); err != nil {
					c.enqueue(msg)
					return
				}
			}
		}
	}()

	// 指令接收循环
	for {
		msg, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("recv: %w", err)
		}
		cmd := msg.GetCommand()
		if cmd == nil {
			continue
		}
		ack := c.handleCommand(cmd)
		if err := send(&owlsfuv1.NodeMessage{Payload: &owlsfuv1.NodeMessage_CommandAck{CommandAck: ack}}); err != nil {
			return fmt.Errorf("send ack: %w", err)
		}
	}
}

// handleCommand 幂等执行指令：命中去重窗口直接重发上次 Ack。
func (c *Client) handleCommand(cmd *owlsfuv1.Command) *owlsfuv1.CommandAck {
	if cached, ok := c.acks.get(cmd.GetCommandId()); ok {
		c.log.Info("duplicate command, replaying ack", "command_id", cmd.GetCommandId())
		return cached
	}
	start := time.Now()
	ack := c.execute(cmd)
	c.acks.put(ack)

	result := "ok"
	if !ack.GetOk() {
		result = ack.GetErrorCode()
	}
	c.metrics.ControlCommands.WithLabelValues(commandType(cmd), result).Inc()
	c.metrics.CommandLatency.WithLabelValues(commandType(cmd)).Observe(time.Since(start).Seconds())
	return ack
}

// execute 分发执行各类指令。
func (c *Client) execute(cmd *owlsfuv1.Command) *owlsfuv1.CommandAck {
	ok := func() *owlsfuv1.CommandAck {
		return &owlsfuv1.CommandAck{CommandId: cmd.GetCommandId(), Ok: true}
	}
	fail := func(code, msg string) *owlsfuv1.CommandAck {
		return &owlsfuv1.CommandAck{CommandId: cmd.GetCommandId(), Ok: false, ErrorCode: code, ErrorMessage: msg}
	}

	switch payload := cmd.GetPayload().(type) {
	case *owlsfuv1.Command_EnsureLogicalRoom:
		c.backend.EnsureRoom(payload.EnsureLogicalRoom.GetRoomId())
		return ok()
	case *owlsfuv1.Command_CloseLogicalRoom:
		c.backend.CloseRoom(payload.CloseLogicalRoom.GetRoomId())
		return ok()
	case *owlsfuv1.Command_DisconnectUser:
		start := time.Now()
		d := payload.DisconnectUser
		n := c.backend.DisconnectUser(d.GetRoomId(), d.GetUserId(), d.GetSessionId(), disconnectReason(d.GetReason()))
		c.metrics.DisconnectCmdLatency.Observe(time.Since(start).Seconds())
		c.log.Info("disconnect user executed", "room", d.GetRoomId(), "uid", d.GetUserId(), "closed", n)
		return ok()
	case *owlsfuv1.Command_RevokeSession:
		start := time.Now()
		r := payload.RevokeSession
		hit := c.backend.RevokeSession(r.GetSessionId(), r.GetJti())
		c.metrics.DisconnectCmdLatency.Observe(time.Since(start).Seconds())
		c.log.Info("revoke session executed", "sid", r.GetSessionId(), "online", hit)
		return ok()
	case *owlsfuv1.Command_UpdateParticipantCaps:
		u := payload.UpdateParticipantCaps
		caps := make([]string, 0, len(u.GetCaps()))
		for _, cp := range u.GetCaps() {
			if s := capToString(cp); s != "" {
				caps = append(caps, s)
			}
		}
		if err := c.backend.UpdateCaps(u.GetRoomId(), u.GetSessionId(), caps); err != nil {
			return fail("NOT_FOUND", err.Error())
		}
		return ok()
	case *owlsfuv1.Command_Drain:
		if payload.Drain.GetCancel() {
			c.backend.SetDraining(false)
			c.log.Info("node undrained: accepting new sessions")
			return ok()
		}
		c.backend.SetDraining(true)
		c.log.Warn("node draining: rejecting new sessions", "deadline_unix_ms", payload.Drain.GetDeadlineUnixMs())
		return ok()
	case *owlsfuv1.Command_SetAnchorLease:
		l := payload.SetAnchorLease
		if err := c.backend.SetAnchorLease(l.GetRoomId(), l.GetAnchorNodeId(),
			l.GetEpoch(), l.GetLeaseExpireUnixMs()); err != nil {
			return fail("STALE_EPOCH", err.Error())
		}
		return ok()
	case *owlsfuv1.Command_SetCascadeEdges:
		e := payload.SetCascadeEdges
		if err := c.backend.SetCascadeEdges(e.GetRoomId(), e.GetEpoch(), e.GetEdges()); err != nil {
			return fail("STALE_EPOCH", err.Error())
		}
		return ok()
	case *owlsfuv1.Command_MigrateParticipants:
		mp := payload.MigrateParticipants
		// MARK = CONNECT 阶段标记（继续服务）；EXECUTE / 未指定 = CLEANUP 摘会话
		//（未指定按 EXECUTE 兼容旧版指令）。
		if mp.GetPhase() == owlsfuv1.MigrateParticipants_PHASE_MARK {
			n := c.backend.MigrateParticipantsMark(mp.GetMigrationId(), mp.GetRoomId(), mp.GetSessionIds())
			c.log.Info("migrate participants marked", "migration_id", mp.GetMigrationId(),
				"room", mp.GetRoomId(), "requested", len(mp.GetSessionIds()), "marked", n)
			return ok()
		}
		n := c.backend.MigrateParticipants(mp.GetMigrationId(), mp.GetRoomId(),
			mp.GetSessionIds(), mp.GetToNodeId())
		c.log.Info("migrate participants executed", "migration_id", mp.GetMigrationId(),
			"room", mp.GetRoomId(), "requested", len(mp.GetSessionIds()), "closed", n)
		return ok()
	case *owlsfuv1.Command_UpdateBinary:
		ub := payload.UpdateBinary
		// 下载可能较久：单独超时上下文，避免无限挂起。
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
		defer cancel()
		result, err := update.Apply(ctx, c.log, update.Options{
			TargetVersion:  ub.GetTargetVersion(),
			DownloadURL:    ub.GetDownloadUrl(),
			SHA256Hex:      ub.GetSha256Hex(),
			Force:          ub.GetForce(),
			CurrentVersion: c.nodeVersion,
		})
		if err != nil {
			c.log.Error("update binary failed", "err", err, "target", ub.GetTargetVersion())
			return fail("UPDATE_FAILED", err.Error())
		}
		if result.Skipped {
			c.log.Info("update binary skipped", "version", ub.GetTargetVersion(), "msg", result.Message)
			return ok()
		}
		c.log.Info("update binary accepted", "target", ub.GetTargetVersion(), "msg", result.Message)
		return ok()
	default:
		return fail("BAD_COMMAND", "unknown or empty command payload")
	}
}

// commandType 返回指令类型名（指标标签用）。
func commandType(cmd *owlsfuv1.Command) string {
	switch cmd.GetPayload().(type) {
	case *owlsfuv1.Command_EnsureLogicalRoom:
		return "ensure_logical_room"
	case *owlsfuv1.Command_CloseLogicalRoom:
		return "close_logical_room"
	case *owlsfuv1.Command_DisconnectUser:
		return "disconnect_user"
	case *owlsfuv1.Command_RevokeSession:
		return "revoke_session"
	case *owlsfuv1.Command_UpdateParticipantCaps:
		return "update_participant_caps"
	case *owlsfuv1.Command_Drain:
		return "drain"
	case *owlsfuv1.Command_SetAnchorLease:
		return "set_anchor_lease"
	case *owlsfuv1.Command_SetCascadeEdges:
		return "set_cascade_edges"
	case *owlsfuv1.Command_MigrateParticipants:
		return "migrate_participants"
	case *owlsfuv1.Command_UpdateBinary:
		return "update_binary"
	default:
		return "unknown"
	}
}

// capToString 映射 proto Cap 枚举到 caps 字符串（协议 §1）。
func capToString(c owlsfuv1.Cap) string {
	switch c {
	case owlsfuv1.Cap_CAP_JOIN:
		return auth.CapJoin
	case owlsfuv1.Cap_CAP_PUBLISH_AUDIO:
		return auth.CapPublishAudio
	case owlsfuv1.Cap_CAP_SUBSCRIBE_AUDIO:
		return auth.CapSubscribeAudio
	case owlsfuv1.Cap_CAP_PUBLISH_SCREEN:
		return auth.CapPublishScreen
	default:
		return ""
	}
}

// disconnectReason 映射断开原因枚举到可读串。
func disconnectReason(r owlsfuv1.DisconnectUser_Reason) string {
	switch r {
	case owlsfuv1.DisconnectUser_REASON_ADMIN:
		return "admin"
	case owlsfuv1.DisconnectUser_REASON_PERMISSION:
		return "permission"
	case owlsfuv1.DisconnectUser_REASON_LEAVE:
		return "leave"
	case owlsfuv1.DisconnectUser_REASON_MIGRATION:
		return "migration"
	case owlsfuv1.DisconnectUser_REASON_RESTRICTION:
		return "restriction"
	default:
		return "unspecified"
	}
}
