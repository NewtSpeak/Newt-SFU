// Package migrate 实现热迁移的节点侧职责（docs 09 / 15 BJ）：
//   - 迁出 MARK（CONNECT 阶段）：按 sid 标记会话迁出中，CUTOVER 前继续服务；
//   - 迁出 EXECUTE（CLEANUP 阶段）：按 sid 摘除会话（RemoveParticipant，closed 码 MIGRATED）；
//   - 迁入 pre-warm：无需额外动作——SFU 会话键 = sid（BJ.2），同一 uid 的新会话
//     （新 token 新 sid）与旧会话天然并存互不冲突，由 room.Manager 保证
//   - Drain 请求：SIGTERM 时经控制通道向 Server 请求排空（编排权威在 Server）
package migrate

import (
	"log/slog"

	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/room"
)

// Handler 处理迁移相关指令。
type Handler struct {
	log     *slog.Logger
	mgr     *room.Manager
	metrics *observability.Metrics
}

// NewHandler 创建迁移处理器。
func NewHandler(log *slog.Logger, mgr *room.Manager, metrics *observability.Metrics) *Handler {
	return &Handler{log: log.With("mod", "migrate"), mgr: mgr, metrics: metrics}
}

// Mark 迁出 MARK 阶段（09 §5.1 CONNECT：源节点标记 sid 迁出中，继续服务到
// CUTOVER）。幂等：sid 不在线视为跳过。返回实际标记数。
func (h *Handler) Mark(migrationID, roomID string, sessionIDs []string) int {
	marked := 0
	for _, sid := range sessionIDs {
		if h.mgr.MarkMigrating(sid) {
			marked++
		}
	}
	h.log.Info("migrate participants marked",
		"migration_id", migrationID, "room", roomID,
		"requested", len(sessionIDs), "marked", marked)
	return marked
}

// MigrateOut 执行迁出（09 §5.1 CLEANUP：源 SFU 删参与者）：
// 按 sid 逐个关闭会话，客户端收到 closed{code=MIGRATED}（专用码，
// 区别于 SESSION_REVOKED——客户端不应清除重连意愿，双 PC 热切由新节点会话接管）。
// 幂等：sid 不在线（已断/已迁）视为成功跳过。返回实际断开数。
func (h *Handler) MigrateOut(migrationID, roomID string, sessionIDs []string, toNodeID string) int {
	closed := 0
	for _, sid := range sessionIDs {
		if h.mgr.CloseSession(sid, room.CloseMigrated, "migrated to node "+toNodeID) {
			closed++
			h.metrics.MigratedSessions.WithLabelValues("ok").Inc()
		} else {
			h.metrics.MigratedSessions.WithLabelValues("not_found").Inc()
		}
	}
	h.log.Info("migrate participants executed",
		"migration_id", migrationID, "room", roomID, "to_node", toNodeID,
		"requested", len(sessionIDs), "closed", closed)
	return closed
}
