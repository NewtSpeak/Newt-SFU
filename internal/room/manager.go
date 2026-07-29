// Package room 管理逻辑房间与参与者（键=sid），执行 caps 与订阅图，驱动 RTP 转发。
package room

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

// Events 为房间事件上报接口（由控制通道客户端实现）。
type Events interface {
	ParticipantJoined(roomID, sid, uid string)
	ParticipantLeft(roomID, sid, uid, detail string)
	// ScreenTrackActive / ScreenTrackEnded 屏幕轨发布生效/结束（docs 14 BC.1 步骤 5，
	// Server 据此 RESERVED→ACTIVE / 释放配额）。
	ScreenTrackActive(roomID, sid, uid string)
	ScreenTrackEnded(roomID, sid, uid string)
}

// NoopEvents 空实现（控制通道未接入时使用）。
type NoopEvents struct{}

func (NoopEvents) ParticipantJoined(_, _, _ string) {}

func (NoopEvents) ParticipantLeft(_, _, _, _ string) {}

func (NoopEvents) ScreenTrackActive(_, _, _ string) {}

func (NoopEvents) ScreenTrackEnded(_, _, _ string) {}

// JoinError 携带协议关闭码的入房失败。
type JoinError struct {
	Code    string
	Message string
}

func (e *JoinError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// ParticipantInfo 为 ready 帧中的房间成员快照项。
type ParticipantInfo struct {
	UserID     string `json:"user_id"`
	SessionID  string `json:"session_id"`
	Publishing bool   `json:"publishing"`
	// PublishingScreen 该成员是否正在共享屏幕（新入房者据此立即订阅屏幕轨）。
	PublishingScreen bool `json:"publishing_screen"`
	// IsBot 机器人参与者独立标记（Media Token bot claim，bot 专项）。
	IsBot bool `json:"is_bot"`
}

// AuditUploader 由控制/信令层注入：把一段录制完成的审计音频上传到主节点服务器。
// 为 nil 时录音仅落本地磁盘（不上传）。
type AuditUploader interface {
	UploadAudit(meta AuditMeta, oggPath string)
}

// AuditMeta 一段审计录音的元数据。
type AuditMeta struct {
	GuildID   string
	ChannelID string
	UserID    string
	SessionID string
	NodeID    string
	StartedAt time.Time
	EndedAt   time.Time
}

// Manager 持有本节点全部房间与会话索引。
type Manager struct {
	log     *slog.Logger
	engine  *owlsfu.Engine
	metrics *observability.Metrics
	stats   *stats.Collector
	events  Events
	// cascade 级联钩子（可为 nil；经 SetCascade 注入，见 cascade_api.go）
	cascade CascadeHooks

	maxUsers int
	draining atomic.Bool

	// nodeID / auditDir / auditUploader 音频审计（adminpresence 专项）：
	// 录音落 auditDir，完成后经 auditUploader 上传主节点；未注入时仅落盘。
	nodeID        string
	auditDir      string
	auditUploader AuditUploader

	// screenTracks 当前活跃屏幕轨数（心跳容量上报 screen_tracks，docs 14 BD.3）。
	screenTracks atomic.Int64

	mu    sync.RWMutex
	rooms map[string]*Room
	bySID map[string]*Participant
}

// NewManager 创建房间管理器。
func NewManager(log *slog.Logger, engine *owlsfu.Engine, metrics *observability.Metrics, st *stats.Collector, maxUsers int) *Manager {
	m := &Manager{
		log:      log,
		engine:   engine,
		metrics:  metrics,
		stats:    st,
		events:   NoopEvents{},
		maxUsers: maxUsers,
		rooms:    make(map[string]*Room),
		bySID:    make(map[string]*Participant),
	}
	st.SetCountsFunc(m.Counts)
	st.SetScreenTracksFunc(m.ScreenTrackCount)
	return m
}

// ScreenTrackCount 返回当前活跃屏幕轨数（心跳 screen_tracks 字段来源）。
func (m *Manager) ScreenTrackCount() int { return int(m.screenTracks.Load()) }

// SetEvents 注入控制通道事件上报器。
func (m *Manager) SetEvents(ev Events) {
	if ev != nil {
		m.events = ev
	}
}

// SetAudit 注入音频审计参数（节点 ID、录音目录、上传器）。审计录音落 dir，
// 完成后经 uploader 上传主节点；uploader 为 nil 时仅落盘。
func (m *Manager) SetAudit(nodeID, dir string, uploader AuditUploader) {
	m.nodeID = nodeID
	m.auditDir = dir
	m.auditUploader = uploader
}

// SetDraining 标记节点排空：拒绝新会话，存量不动。
func (m *Manager) SetDraining(v bool) { m.draining.Store(v) }

// Draining 返回排空状态。
func (m *Manager) Draining() bool { return m.draining.Load() }

// Counts 返回当前 (users, rooms)。
func (m *Manager) Counts() (int, int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.bySID), len(m.rooms)
}

// Join 用验签通过的 token 创建参与者；返回参与者与房间成员快照。
func (m *Manager) Join(tok *auth.Token, msgr Messenger) (*Participant, []ParticipantInfo, error) {
	if m.draining.Load() {
		return nil, nil, &JoinError{Code: CloseNodeDraining, Message: "node is draining"}
	}

	// 同 sid 重连：旧连接让位（网络闪断后客户端持同一 token 重入）
	m.mu.RLock()
	old := m.bySID[tok.SID]
	m.mu.RUnlock()
	if old != nil {
		old.log.Info("session superseded by new connection", "sid", tok.SID)
		old.Close(CloseSessionRevoked, "superseded by new connection")
	}

	pc, err := m.engine.NewPeerConnection()
	if err != nil {
		return nil, nil, fmt.Errorf("new peer connection: %w", err)
	}

	m.mu.Lock()
	if m.maxUsers > 0 && len(m.bySID) >= m.maxUsers {
		m.mu.Unlock()
		pc.Close()
		m.log.Warn("local max_users cap hit, rejecting session", "max_users", m.maxUsers)
		return nil, nil, &JoinError{Code: CloseNodeDraining, Message: "node at capacity"}
	}
	r, ok := m.rooms[tok.RID]
	if !ok {
		// EnsureLogicalRoom 未提前到达：允许隐式建房但告警
		m.log.Warn("implicit room creation on auth (EnsureLogicalRoom not received)", "room", tok.RID)
		r = newRoom(tok.RID, m, false)
		m.rooms[tok.RID] = r
		m.metrics.Rooms.Inc()
	}

	p := &Participant{
		log:          m.log.With("room", tok.RID, "sid", tok.SID, "uid", tok.UID),
		room:         r,
		sid:          tok.SID,
		uid:          tok.UID,
		gid:          tok.GID,
		isBot:        tok.Bot,
		hidden:       tok.Hidden,
		audit:        tok.Audit,
		msgr:         msgr,
		pc:           pc,
		downTracks:   make(map[string]*downTrack),
		screenDown:   make(map[string]*downTrack),
		unsubscribed: make(map[string]unsubKinds),
		stopExpiry:   make(chan struct{}),
		joinedAt:     time.Now(),
	}
	p.setCaps(tok.Caps)
	p.expiryMs.Store(tok.ExpiresAt.UnixMilli())

	// 成员快照（含自己之前的在房成员）：隐身临场成员对他人不可见，从快照剔除。
	// Publishing 语义 = 对外可听的发布（挂起接纳的无 cap 音频轨不算，docs 11 AD.4）。
	snapshot := make([]ParticipantInfo, 0, len(r.parts))
	for _, other := range r.parts {
		if other.hidden {
			continue
		}
		snapshot = append(snapshot, ParticipantInfo{
			UserID: other.uid, SessionID: other.sid,
			Publishing:       other.Publishing() && other.Caps().Has(auth.CapPublishAudio),
			PublishingScreen: other.ScreenPublishing(),
			IsBot:            other.isBot,
		})
	}
	r.parts[tok.SID] = p
	m.bySID[tok.SID] = p
	m.mu.Unlock()

	m.metrics.Participants.Inc()
	m.wirePC(p)
	go p.expiryWatchdog()

	// 隐身临场：不向房内其他成员广播其加入（SFU 侧隐身，与 Server 侧列表/事件抑制配合）。
	if !p.hidden {
		r.broadcast(p, "participant_joined", map[string]any{"user_id": p.uid, "session_id": p.sid, "is_bot": p.isBot})
	}
	p.log.Info("participant joined", "publishing_snapshot", len(snapshot))
	// 新听众默认全订：本地订阅需求变化，通知级联重算 NodeWant
	m.cascadeDemand(r.id)
	return p, snapshot, nil
}

// wirePC 挂接 PC 回调：trickle ICE 下发、上行轨接入、连接状态跟踪。
func (m *Manager) wirePC(p *Participant) {
	p.pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || p.closed.Load() {
			return
		}
		init := c.ToJSON()
		if err := p.msgr.Send("ice", map[string]any{
			"candidate":       init.Candidate,
			"sdp_mid":         init.SDPMid,
			"sdp_mline_index": init.SDPMLineIndex,
		}); err != nil {
			p.log.Debug("send ice failed", "err", err)
		}
	})
	p.pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		// caps 校验按 kind 分别执行：音频要求 publish_audio（onAudioTrack），
		// 视频要求 publish_screen（onScreenTrack，无 cap 经 renegotiation 剥离）。
		p.room.onTrack(p, track, receiver)
	})
	p.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			// 媒体连通：经控制通道上报 PARTICIPANT_JOINED（05 步骤 12–13）
			if p.joined.CompareAndSwap(false, true) {
				m.metrics.JoinDuration.Observe(time.Since(p.joinedAt).Seconds())
				m.events.ParticipantJoined(p.room.id, p.sid, p.uid)
				// 补挂房内既有发布者的下行轨（默认全订）
				p.room.attachPublishersToSubscriber(p)
			}
		case webrtc.PeerConnectionStateFailed:
			p.Close(ClosePCFailed, "peer connection failed")
		case webrtc.PeerConnectionStateClosed:
			p.Close(ClosePCFailed, "peer connection closed")
		}
	})
}

// KindVideo 为 subscribe/unsubscribe 帧 kinds 字段的视频维度取值（协议 §2.1）：
// 覆盖屏幕轨与系统音频伴轨（伴轨跟随屏幕会话，docs 14 BA.4）。
const KindVideo = "video"

// subscriptionMask 解析 kinds 列表为维度掩码：缺省（空列表）= 全部维度（旧客户端
// 行为不变）；显式给出时按取值选择维度（"video" 同时覆盖 screen/screen_audio；
// 亦宽容接受轨事件 kind 取值 "screen"/"screen_audio"）。全部取值未知时返回零掩码
// （调用侧忽略该帧）。
func subscriptionMask(kinds []string) unsubKinds {
	if len(kinds) == 0 {
		return unsubKinds{audio: true, video: true}
	}
	var m unsubKinds
	for _, k := range kinds {
		switch k {
		case KindAudio:
			m.audio = true
		case KindVideo, KindScreen, KindScreenAudio:
			m.video = true
		}
	}
	return m
}

// Subscribe 恢复订阅某发布者的指定轨类型（kinds 为空 = 全部轨类型）。
func (p *Participant) Subscribe(pubUID string, kinds ...string) {
	p.room.setSubscription(p, pubUID, subscriptionMask(kinds), true)
}

// Unsubscribe 退订某发布者的指定轨类型（kinds 为空 = 全部；静音=退订 audio，
// 不观看=退订 video，真实停转发）。
func (p *Participant) Unsubscribe(pubUID string, kinds ...string) {
	p.room.setSubscription(p, pubUID, subscriptionMask(kinds), false)
}

// ---- 控制通道指令入口 ----

// EnsureRoom 幂等建房。
func (m *Manager) EnsureRoom(roomID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r, ok := m.rooms[roomID]; ok {
		r.ensured = true
		return
	}
	m.rooms[roomID] = newRoom(roomID, m, true)
	m.metrics.Rooms.Inc()
}

// CloseRoom 关房：断开全部成员并删除房间（幂等）。
func (m *Manager) CloseRoom(roomID string) {
	m.mu.Lock()
	r, ok := m.rooms[roomID]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.rooms, roomID)
	r.removed = true
	parts := make([]*Participant, 0, len(r.parts))
	for _, p := range r.parts {
		parts = append(parts, p)
	}
	m.mu.Unlock()

	m.metrics.Rooms.Dec()
	r.stopLoop()
	for _, p := range parts {
		p.Close(CloseRoomClosed, "room closed by server")
	}
}

// DisconnectUser 断开房内某用户（sid 为空则断其全部会话），返回断开数。
func (m *Manager) DisconnectUser(roomID, userID, sessionID, reason string) int {
	m.mu.RLock()
	r, ok := m.rooms[roomID]
	var targets []*Participant
	if ok {
		for _, p := range r.parts {
			if p.uid == userID && (sessionID == "" || p.sid == sessionID) {
				targets = append(targets, p)
			}
		}
	}
	m.mu.RUnlock()
	for _, p := range targets {
		p.Close(CloseDisconnected, "disconnected by server: "+reason)
	}
	return len(targets)
}

// MarkMigrating 按 sid 标记会话迁出中（MigrateParticipants MARK，09 §5.1：
// CUTOVER 前继续服务），返回是否命中。幂等。
func (m *Manager) MarkMigrating(sessionID string) bool {
	m.mu.RLock()
	p := m.bySID[sessionID]
	m.mu.RUnlock()
	if p == nil {
		return false
	}
	p.SetMigrating(true)
	return true
}

// CloseSession 按 sid 关闭会话（RevokeSession 用），返回是否命中。
func (m *Manager) CloseSession(sessionID, code, message string) bool {
	m.mu.RLock()
	p := m.bySID[sessionID]
	m.mu.RUnlock()
	if p == nil {
		return false
	}
	p.Close(code, message)
	return true
}

// UpdateCaps 全量替换参与者 caps 并实时生效。
func (m *Manager) UpdateCaps(roomID, sessionID string, caps []string) error {
	m.mu.RLock()
	r, ok := m.rooms[roomID]
	var p *Participant
	if ok {
		p = r.parts[sessionID]
	}
	m.mu.RUnlock()
	if p == nil {
		return fmt.Errorf("session %s not found in room %s", sessionID, roomID)
	}
	r.applyCaps(p, auth.NewCapSet(caps))
	return nil
}

// CloseAll 优雅退出：断开全部会话。
func (m *Manager) CloseAll(code, message string) {
	m.mu.RLock()
	parts := make([]*Participant, 0, len(m.bySID))
	for _, p := range m.bySID {
		parts = append(parts, p)
	}
	m.mu.RUnlock()
	for _, p := range parts {
		p.Close(code, message)
	}
}
