// Package auth 负责 Media Token（Ed25519 JWT）验签、caps 解析与会话吊销表。
package auth

import "time"

// caps 字符串常量（与 proto Cap 枚举一一映射，见 docs/协议/README.md §1）。
const (
	CapJoin           = "join"
	CapPublishAudio   = "publish_audio"
	CapSubscribeAudio = "subscribe_audio"
	CapPublishScreen  = "publish_screen"
)

// CapSet 为不可变约定的 caps 集合（构造后只读）。
type CapSet map[string]struct{}

// NewCapSet 从字符串列表构造集合。
func NewCapSet(caps []string) CapSet {
	s := make(CapSet, len(caps))
	for _, c := range caps {
		s[c] = struct{}{}
	}
	return s
}

// Has 判断是否包含指定 cap。
func (s CapSet) Has(cap string) bool {
	_, ok := s[cap]
	return ok
}

// Slice 返回排序无关的 caps 列表副本。
func (s CapSet) Slice() []string {
	out := make([]string, 0, len(s))
	for c := range s {
		out = append(out, c)
	}
	return out
}

// Token 为验签通过后的结构化 Media Token claims。
type Token struct {
	UID string
	GID string
	CID string
	NID string
	RID string
	SID string
	// Bot 机器人会话标记（bot 专项）：参与者信令（ready 快照 / participant_joined）
	// 据此携带 is_bot，使机器人在音频流中拥有独立的用户标记。
	Bot bool
	// Hidden 系统管理员隐身临场（adminpresence 专项）：抑制 participant_joined/left
	// 广播并从 ready 快照剔除。
	Hidden bool
	// Audit 音频审计（adminpresence 专项）：录制该会话上行音频并上传主节点。
	Audit     bool
	Caps      CapSet
	JTI       string
	ExpiresAt time.Time
}
