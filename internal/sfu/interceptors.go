package sfu

import (
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

// minimalInterceptors 保留 RTCP SR/RR + receiver 侧 TWCC 反馈：
// 音频不开 NACK、不做重排/缓冲，抗丢包依赖端侧 Opus in-band FEC（15 §7.1）。
//
// 屏幕共享视频（docs 14）取舍：
//   - PLI/FIR 关键帧请求不走 interceptor，由 room 层从观看端 sender 读出后
//     手工转发给发布者（见 room/screen.go），保持转发路径可控；
//   - TWCC（transport-cc）：注册 receiver 侧反馈生成器（ConfigureTWCCSender：
//     SFU 作为接收方回发 transport-cc report），为将来发布端带宽自适应/自动降层
//     提供反馈通道（BA.3 二期）；本期 SFU 不做带宽估计与自动切层；
//   - REMB / 发送侧 TWCC 序号注入保持未注册：SFU 下行不做拥塞控制。
func minimalInterceptors(me *webrtc.MediaEngine) (*interceptor.Registry, error) {
	ir := &interceptor.Registry{}
	if err := webrtc.ConfigureRTCPReports(ir); err != nil {
		return nil, err
	}
	if err := webrtc.ConfigureTWCCSender(me, ir); err != nil {
		return nil, err
	}
	return ir, nil
}
