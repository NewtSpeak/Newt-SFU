// 屏幕共享视频路径（docs 14 BA / docs 15 §7.1）：VP8 编解码注册与参数。
package sfu

import (
	"fmt"

	"github.com/pion/webrtc/v4"
)

// VP8Codec 为屏幕共享视频轨的统一编解码参数（选路 RTP 转发、不解码不转码，docs 14 BA.3）。
//
// 编解码协商策略：首期只注册 VP8 作为基线——全端（含浏览器）必支持、无专利授权负担、
// 打包格式简单。客户端 offer 中的其他视频编解码（H.264/VP9/AV1）不在本节点注册表内，
// 协商自然回落到 VP8；完全不支持 VP8 的客户端该 m-line 协商失败，由客户端自行回退。
// 多编解码透传 / simulcast 选层（BA.3 降质路径）留二期。
var VP8Codec = webrtc.RTPCodecCapability{
	MimeType:  webrtc.MimeTypeVP8,
	ClockRate: 90000,
	// RTCP 反馈只声明关键帧请求（nack pli / ccm fir），由 SFU 在观看端→发布者方向转发；
	// 不声明 generic NACK：SFU 无重传缓冲（纯转发，与音频原则一致，docs 15 §7.1）；
	// 不声明 goog-remb / transport-cc：interceptor 链保持精简（取舍见 interceptors.go）。
	RTCPFeedback: []webrtc.RTCPFeedback{
		{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"},
		{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"},
	},
}

// registerVideo 在 MediaEngine 上注册屏幕共享用 VP8（PayloadType 96，惯例值）。
func registerVideo(me *webrtc.MediaEngine) error {
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: VP8Codec,
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("register vp8: %w", err)
	}
	return nil
}
