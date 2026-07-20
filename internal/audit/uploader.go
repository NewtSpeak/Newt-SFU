// Package audit 把审计录音上传到主节点服务器（adminpresence 专项）。
package audit

import (
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/owlspeak/owl-sfu/internal/room"
)

// Uploader 实现 room.AuditUploader：会话结束后把录音 POST 到主节点 /audit-api/records。
// 认证走共享密钥（Bearer），与 Owl-Server 的 AUDIT_INGEST_TOKEN 对齐。
type Uploader struct {
	log       *slog.Logger
	ingestURL string // 形如 https://server/audit-api/records
	token     string
	client    *http.Client
	// keepLocal=false 时上传成功后删除本地录音文件（默认删除，节省节点磁盘）。
	keepLocal bool
}

var _ room.AuditUploader = (*Uploader)(nil)

// NewUploader 构建上传器；ingestURL 或 token 为空时返回 nil（审计上传关闭）。
func NewUploader(log *slog.Logger, ingestURL, token string, keepLocal bool) *Uploader {
	if ingestURL == "" || token == "" {
		return nil
	}
	return &Uploader{
		log:       log,
		ingestURL: ingestURL,
		token:     token,
		client:    &http.Client{Timeout: 60 * time.Second},
		keepLocal: keepLocal,
	}
}

// UploadAudit 上传一段录音（异步，不阻塞会话收尾）。
func (u *Uploader) UploadAudit(meta room.AuditMeta, oggPath string) {
	go u.upload(meta, oggPath)
}

func (u *Uploader) upload(meta room.AuditMeta, oggPath string) {
	file, err := os.Open(oggPath)
	if err != nil {
		u.log.Warn("audit upload: open file failed", "path", oggPath, "err", err)
		return
	}
	defer file.Close()

	params := url.Values{}
	params.Set("guild_id", meta.GuildID)
	params.Set("channel_id", meta.ChannelID)
	params.Set("user_id", meta.UserID)
	params.Set("session_id", meta.SessionID)
	params.Set("node_id", meta.NodeID)
	params.Set("started", strconv.FormatInt(meta.StartedAt.Unix(), 10))
	params.Set("ended", strconv.FormatInt(meta.EndedAt.Unix(), 10))

	req, err := http.NewRequest(http.MethodPost, u.ingestURL+"?"+params.Encode(), file)
	if err != nil {
		u.log.Warn("audit upload: build request failed", "err", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+u.token)
	req.Header.Set("Content-Type", "audio/ogg")
	if info, statErr := file.Stat(); statErr == nil {
		req.ContentLength = info.Size()
	}
	resp, err := u.client.Do(req)
	if err != nil {
		u.log.Warn("audit upload: request failed", "err", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		u.log.Warn("audit upload: rejected by server", "status", resp.StatusCode)
		return
	}
	u.log.Info("audit recording uploaded", "user_id", meta.UserID, "channel_id", meta.ChannelID)
	if !u.keepLocal {
		_ = os.Remove(oggPath)
	}
}
