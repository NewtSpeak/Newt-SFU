// 轨道 kind 与下行轨 id 约定（协议 §2.3 track_published.kind / docs 14 BA.4）：
//
//	kind=audio        → 下行轨 id = <sid>            （主麦克风轨）
//	kind=screen       → 下行轨 id = <sid>#screen      （屏幕共享视频轨）
//	kind=screen_audio → 下行轨 id = <sid>#screen-audio（屏幕共享系统音频伴轨）
//
// 该 id 同时用作级联出向轨的 track id（对端经 msid 还原 speaker 身份与 kind），
// 因此 TrackKey/SplitTrackKey 是客户端下行与节点间级联共用的编码约定。
package room

import "strings"

// 轨道 kind（track_published / track_ended 事件的 kind 字段取值）。
const (
	KindAudio       = "audio"
	KindScreen      = "screen"
	KindScreenAudio = "screen_audio"
)

const (
	screenTrackSuffix      = "#screen"
	screenAudioTrackSuffix = "#screen-audio"
)

// TrackKey 返回某发布者某 kind 轨的全局唯一键（同时是下行轨/级联轨的 track id）。
func TrackKey(sid, kind string) string {
	switch kind {
	case KindScreen:
		return sid + screenTrackSuffix
	case KindScreenAudio:
		return sid + screenAudioTrackSuffix
	default:
		return sid
	}
}

// SplitTrackKey 解析 TrackKey，返回源 sid 与 kind。
func SplitTrackKey(key string) (sid, kind string) {
	if s, ok := strings.CutSuffix(key, screenAudioTrackSuffix); ok {
		return s, KindScreenAudio
	}
	if s, ok := strings.CutSuffix(key, screenTrackSuffix); ok {
		return s, KindScreen
	}
	return key, KindAudio
}
