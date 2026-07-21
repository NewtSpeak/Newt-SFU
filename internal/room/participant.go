package room

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/owlspeak/owl-sfu/internal/auth"
)

// SFU 主动关闭会话时 closed 帧使用的原因码。
// 前 8 个对齐协议 §2.4；其余为 SFU 侧扩展（协议未定义踢人/房间关闭码）。
const (
	CloseNodeDraining   = "NODE_DRAINING"
	CloseAuthTimeout    = "AUTH_TIMEOUT"
	CloseSessionRevoked = "SESSION_REVOKED"
	CloseTokenExpired   = "TOKEN_EXPIRED"
	CloseRoomMismatch   = "ROOM_MISMATCH"
	CloseDisconnected   = "DISCONNECTED" // DisconnectUser 指令（扩展码）
	CloseRoomClosed     = "ROOM_CLOSED"  // CloseLogicalRoom 指令（扩展码）
	ClosePCFailed       = "PC_FAILED"    // ICE/DTLS 失败（扩展码）
	CloseMigrated       = "MIGRATED"     // MigrateParticipants 迁出（扩展码，09 §5.1 CLEANUP）
)

// Messenger 由 signal 层实现：向客户端发帧、带原因关闭连接。
type Messenger interface {
	Send(op string, d any) error
	// CloseWithReason 发送 closed{code,message} 后关闭底层连接；须幂等。
	CloseWithReason(code, message string)
}

// RTPWriter 为下行轨写入抽象：本地订阅者下行轨为 *webrtc.TrackLocalStaticRTP，
// 级联旁路 sink 可用带计数/门控的包装实现（见 internal/cascade）。
type RTPWriter interface {
	WriteRTP(p *rtp.Packet) error
}

// unsubKinds 为对某发布者的按轨类型退订标记（协议 §2.1 subscribe/unsubscribe 的
// kinds 维度）：audio 作用于音频轨；video 作用于屏幕轨与系统音频伴轨（伴轨跟随
// 屏幕会话，docs 14 BA.4）。两维均为 false 时等价于「未退订」（从位图删除）。
type unsubKinds struct {
	audio bool
	video bool
}

// none 返回是否两维均未退订。
func (u unsubKinds) none() bool { return !u.audio && !u.video }

// kindIsVideo 把轨 kind 归并到订阅维度：screen / screen_audio 属 video 维度
// （伴轨跟随屏幕，BA.4），其余（audio）属 audio 维度。
func kindIsVideo(kind string) bool {
	return kind == KindScreen || kind == KindScreenAudio
}

// downTrack 为一条「发布者→订阅者」下行轨。
type downTrack struct {
	pubUID string
	track  RTPWriter
	// sender 本地订阅者下行轨的 RTPSender（级联旁路 sink 为 nil，摘除时无需 RemoveTrack）
	sender *webrtc.RTPSender
	// active=false 表示订阅者已退订（轨保留、转发跳过，重订阅零协商恢复）
	active bool
	// owner 远端轨（级联）下行轨的所属 RemotePublisher（本地发布者为 nil）：
	// 同 key 快速重挂时，旧 rp.Close 只允许清理属于自己的下行轨（防误删新注册）。
	owner *RemotePublisher
}

// Participant 为一个媒体会话（键 = sid）。
type Participant struct {
	log  *slog.Logger
	room *Room
	sid  string
	uid  string
	gid  string
	// isBot 机器人会话标记（Media Token bot claim）：参与者信令带 is_bot 独立标记。
	isBot bool
	// hidden 系统管理员隐身临场（Media Token hidden claim）：抑制其 joined/left
	// 广播并从 ready 快照剔除。
	hidden bool
	// audit 音频审计（Media Token audit claim）：录制其上行音频上传主节点。
	audit bool
	// rec 审计录音器（audit=true 且上行音频到达时创建；转发热路径无锁读）。
	rec  atomic.Pointer[auditRecorder]
	msgr Messenger
	pc   *webrtc.PeerConnection

	caps     atomic.Pointer[auth.CapSet]
	expiryMs atomic.Int64 // token 过期时间 unix ms（auth 刷新时更新）

	// mu 保护 downTracks（发布者 sid → 音频下行轨）、screenDown（trackKey →
	// 屏幕/伴轨下行轨）、unsubscribed（发布者 uid → 按轨类型退订标记）、
	// screenTcv/screenAudioTcv、screenLayers 与 screenLayerSel
	mu           sync.Mutex
	downTracks   map[string]*downTrack
	screenDown   map[string]*downTrack
	unsubscribed map[string]unsubKinds

	// fanout：本参与者作为发布者时的下行轨快照（转发热路径无锁读）
	fanout     atomic.Pointer[[]*downTrack]
	publishing atomic.Bool

	// ---- 屏幕共享发布状态（docs 14；实现见 screen.go）----
	screenPublishing atomic.Bool // 每用户同时 1 路（AX.4；simulcast 多 rid 层算 1 路）
	// screenLayers 屏幕上行编码层（quality → 层；非 simulcast 单层 = layerHigh；mu 保护）
	screenLayers map[string]*screenLayer
	// screenLayerSel 本参与者作为观看端的选层请求（发布者 uid → quality；缺省 high；mu 保护）
	screenLayerSel   map[string]string
	screenTcv        *webrtc.RTPTransceiver // 屏幕上行 transceiver（mu 保护；停收上行用）
	screenKeyReqAtMs atomic.Int64           // 上次向发布者转发关键帧请求的时间（节流）

	// ---- 系统音频伴轨发布状态（docs 14 BA.4；实现见 screen.go）----
	screenAudioPublishing atomic.Bool
	screenAudioFanout     atomic.Pointer[[]*downTrack]
	screenAudioTcv        *webrtc.RTPTransceiver // 伴轨上行 transceiver（mu 保护）

	// speaking 检测状态（forwardLoop 写，speakingLoop 读）
	lastSpeaking  atomic.Bool
	lastLevelAtMs atomic.Int64

	// 协商串行化：negotiating 表示有一个 SFU offer 在途，negPending 表示需再协商
	sigMu       sync.Mutex // 保护全部 SDP 操作
	negMu       sync.Mutex
	negotiating bool
	negPending  bool

	closed atomic.Bool
	joined atomic.Bool // 已上报 PARTICIPANT_JOINED
	// migrating 迁出标记（MigrateParticipants MARK，09 §5.1）：CONNECT 阶段由
	// Server 置位，CUTOVER 前继续正常服务；仅用于观测与关闭语义（EXECUTE /
	// 客户端拆旧 PC 均属预期收尾，非异常）。
	migrating atomic.Bool

	// joinedAt 为 auth 通过时刻（进房耗时直方图基准）
	joinedAt time.Time

	stopExpiry chan struct{}
}

// SID 返回会话 ID。
func (p *Participant) SID() string { return p.sid }

// UID 返回用户 ID。
func (p *Participant) UID() string { return p.uid }

// IsBot 返回是否为机器人会话（Media Token bot claim）。
func (p *Participant) IsBot() bool { return p.isBot }

// RoomID 返回所在逻辑房间 ID。
func (p *Participant) RoomID() string { return p.room.id }

// Caps 返回当前 caps 集合快照。
func (p *Participant) Caps() auth.CapSet { return *p.caps.Load() }

func (p *Participant) setCaps(c auth.CapSet) { p.caps.Store(&c) }

// Publishing 返回是否有活跃上行音频轨。
func (p *Participant) Publishing() bool { return p.publishing.Load() }

// SetMigrating 置迁出标记（MigrateParticipants MARK）。
func (p *Participant) SetMigrating(v bool) { p.migrating.Store(v) }

// Migrating 返回是否处于迁出标记状态。
func (p *Participant) Migrating() bool { return p.migrating.Load() }

// ScreenPublishing 返回是否有活跃上行屏幕（视频）轨。
func (p *Participant) ScreenPublishing() bool { return p.screenPublishing.Load() }

// ScreenAudioPublishing 返回是否有活跃上行系统音频伴轨（docs 14 BA.4）。
func (p *Participant) ScreenAudioPublishing() bool { return p.screenAudioPublishing.Load() }

// RefreshToken 处理重复 auth 帧（token 刷新）：sid 必须一致；更新过期时间，并
// 同步 audit/hidden claim（管理员中途开关审计/隐身后，客户端在位重发 auth 即可生效，
// 无需整房重连）。
func (p *Participant) RefreshToken(tok *auth.Token) error {
	if tok.SID != p.sid {
		return fmt.Errorf("refresh token sid mismatch")
	}
	if tok.RID != p.room.id {
		return fmt.Errorf("refresh token rid mismatch")
	}
	p.expiryMs.Store(tok.ExpiresAt.UnixMilli())

	// hidden：中途开关仅影响后续 joined/left 广播；已在房成员不回放历史事件。
	p.hidden = tok.Hidden

	// audit：中途开启 → 若已在发布上行则立即开录；中途关闭 → 收尾并上传当前段。
	if tok.Audit && !p.audit {
		p.audit = true
		if p.publishing.Load() && p.Caps().Has(auth.CapPublishAudio) {
			p.startAudit()
		}
	} else if !tok.Audit && p.audit {
		p.audit = false
		p.finishAudit()
	}
	return nil
}

// HandleOffer 处理客户端 offer，返回 answer SDP。
func (p *Participant) HandleOffer(sdp string) (string, error) {
	p.sigMu.Lock()
	defer p.sigMu.Unlock()
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: sdp,
	}); err != nil {
		return "", fmt.Errorf("set remote offer: %w", err)
	}
	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("create answer: %w", err)
	}
	if err := p.pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("set local answer: %w", err)
	}
	// 客户端 offer 处理完毕（回稳）：补做被挂起的 SFU renegotiation
	p.retryPendingNegotiation()
	return answer.SDP, nil
}

// retryPendingNegotiation 在信令回稳后补做挂起的协商。
func (p *Participant) retryPendingNegotiation() {
	p.negMu.Lock()
	pending := p.negPending && !p.negotiating
	if pending {
		p.negPending = false
	}
	p.negMu.Unlock()
	if pending {
		p.negotiate()
	}
}

// HandleAnswer 处理客户端对 SFU renegotiation offer 的应答。
func (p *Participant) HandleAnswer(sdp string) error {
	p.sigMu.Lock()
	err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: sdp,
	})
	p.sigMu.Unlock()

	p.negMu.Lock()
	p.negotiating = false
	pending := p.negPending
	p.negPending = false
	p.negMu.Unlock()
	if pending {
		p.negotiate()
	}
	return err
}

// AddICECandidate 注入客户端 trickle ICE candidate。
func (p *Participant) AddICECandidate(candidate string, sdpMid *string, sdpMLineIndex *uint16) error {
	return p.pc.AddICECandidate(webrtc.ICECandidateInit{
		Candidate:     candidate,
		SDPMid:        sdpMid,
		SDPMLineIndex: sdpMLineIndex,
	})
}

// negotiate 请求一次 SFU 发起的 renegotiation；在途时置 pending，answer 到达后补做。
func (p *Participant) negotiate() {
	if p.closed.Load() {
		return
	}
	p.negMu.Lock()
	if p.negotiating {
		p.negPending = true
		p.negMu.Unlock()
		return
	}
	p.negotiating = true
	p.negMu.Unlock()
	go p.doNegotiate()
}

func (p *Participant) doNegotiate() {
	p.sigMu.Lock()
	if p.closed.Load() || p.pc.SignalingState() != webrtc.SignalingStateStable {
		// 客户端 offer 处理中：挂起，等状态回稳后由 answer/offer 路径重试
		p.sigMu.Unlock()
		p.negMu.Lock()
		p.negotiating = false
		p.negPending = true
		p.negMu.Unlock()
		return
	}
	offer, err := p.pc.CreateOffer(nil)
	if err == nil {
		err = p.pc.SetLocalDescription(offer)
	}
	p.sigMu.Unlock()
	if err != nil {
		p.log.Warn("renegotiation offer failed", "err", err)
		p.negMu.Lock()
		p.negotiating = false
		p.negMu.Unlock()
		return
	}
	if err := p.msgr.Send("offer", map[string]any{"sdp": offer.SDP}); err != nil {
		p.log.Debug("send renegotiation offer failed", "err", err)
	}
}

// getDownTrack 返回来自指定发布者的下行轨。
func (p *Participant) getDownTrack(pubSID string) *downTrack {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.downTracks[pubSID]
}

// isUnsubscribed 判断对某发布者某轨类型是否已退订（kind ∈ audio/screen/screen_audio，
// 屏幕与伴轨共用 video 维度）。
func (p *Participant) isUnsubscribed(pubUID, kind string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hasUnsubscribedLocked(pubUID, kind)
}

// expiryWatchdog 周期检查 token 过期；刷新帧会推迟过期时间。
func (p *Participant) expiryWatchdog() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.stopExpiry:
			return
		case now := <-t.C:
			exp := p.expiryMs.Load()
			if exp > 0 && now.UnixMilli() > exp {
				p.log.Info("media token expired without refresh", "sid", p.sid)
				p.Close(CloseTokenExpired, "media token expired")
				return
			}
		}
	}
}

// Close 统一关闭路径（幂等）：WS 断/PC failed/踢人/吊销/过期均走此处。
func (p *Participant) Close(code, message string) {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.stopExpiry)
	p.msgr.CloseWithReason(code, message)
	if err := p.pc.Close(); err != nil {
		p.log.Debug("pc close", "err", err)
	}
	p.room.removeParticipant(p, code)
}
