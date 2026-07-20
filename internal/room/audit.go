package room

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4/pkg/media/oggwriter"
)

// 音频审计录制（adminpresence 专项）：token 带 audit claim 的会话，其上行音频
// 被录制为 Ogg/Opus 落本地盘，会话结束后经 AuditUploader 上传主节点服务器。
// 纯旁路：复用 forwardLoop 解出的 RTP 包，不影响转发热路径。

// auditRecorder 单个说话者一段上行音频的录制器（Opus 48kHz 双声道）。
type auditRecorder struct {
	mu      sync.Mutex
	writer  *oggwriter.OggWriter
	path    string
	started time.Time
	closed  bool
}

// newAuditRecorder 在 dir 下创建一个以 sid 命名的 ogg 录音文件。
func newAuditRecorder(dir, sid string) *auditRecorder {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil
	}
	path := filepath.Join(dir, sid+"-"+time.Now().UTC().Format("20060102T150405")+".ogg")
	// Opus：48000 采样率、双声道（与 SFU MediaEngine 一致）。
	writer, err := oggwriter.New(path, 48000, 2)
	if err != nil {
		return nil
	}
	return &auditRecorder{writer: writer, path: path, started: time.Now().UTC()}
}

// write 写入一个 RTP 包（forwardLoop 旁路调用）。
func (a *auditRecorder) write(pkt *rtp.Packet) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.writer == nil {
		return
	}
	_ = a.writer.WriteRTP(pkt)
}

// finish 关闭写入器，返回文件路径与起止时间（幂等）。
func (a *auditRecorder) finish() (path string, started, ended time.Time, ok bool) {
	if a == nil {
		return "", time.Time{}, time.Time{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return "", time.Time{}, time.Time{}, false
	}
	a.closed = true
	if a.writer != nil {
		_ = a.writer.Close()
	}
	return a.path, a.started, time.Now().UTC(), true
}

// startAudit 为参与者创建审计录音器（onAudioTrack 中，audit=true 时调用）。
func (p *Participant) startAudit() {
	if !p.audit || p.room.mgr.auditDir == "" {
		return
	}
	if p.rec.Load() != nil {
		return
	}
	if rec := newAuditRecorder(p.room.mgr.auditDir, p.sid); rec != nil {
		p.rec.Store(rec)
	}
}

// finishAudit 收尾录音并上传主节点（幂等；participant 离开或轨结束时调用）。
func (p *Participant) finishAudit() {
	rec := p.rec.Swap(nil)
	if rec == nil {
		return
	}
	path, started, ended, ok := rec.finish()
	if !ok {
		return
	}
	uploader := p.room.mgr.auditUploader
	if uploader == nil {
		p.log.Info("audit recording saved locally (no uploader)", "path", path)
		return
	}
	uploader.UploadAudit(AuditMeta{
		GuildID: p.gid, ChannelID: p.room.id, UserID: p.uid, SessionID: p.sid,
		NodeID: p.room.mgr.nodeID, StartedAt: started, EndedAt: ended,
	}, path)
}
