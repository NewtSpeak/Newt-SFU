// Package sfu 封装 Pion：共享 UDPMux、MediaEngine（Opus + audio-level 扩展）、PC 工厂。
package sfu

import (
	"fmt"
	"net"

	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
)

// OpusCodec 为全节点统一的音频编解码参数（纯转发，不解码）。
var OpusCodec = webrtc.RTPCodecCapability{
	MimeType:    webrtc.MimeTypeOpus,
	ClockRate:   48000,
	Channels:    2,
	// stereo=1 / sprop-stereo=1：协商允许双声道 Opus（端侧按采集声道实际编码；纯转发）
	SDPFmtpLine: "minptime=10;useinbandfec=1;stereo=1;sprop-stereo=1",
}

// Engine 持有共享 UDPMux 与 webrtc.API，负责创建 PeerConnection。
type Engine struct {
	api *webrtc.API
	// cascadeAPI 级联专用 API：不走 UDPMux（每 PC 独立 ephemeral UDP 端口）。
	// 原因：级联在同一对节点间存在多条 PC（上/下行方向 × 房间数），若双端都
	// 复用单端口 mux，这些 PC 的 UDP 四元组完全相同，而 pion UDPMux 对非 STUN
	// 包按「对端地址」路由，同一地址对只能承载一个 DTLS 会话，多 PC 会互相
	// 抢占路由导致媒体断续。故偏离 15 BH.1「级联 PC 共用单口」：级联媒体改用
	// 动态端口直连（BG.4 host candidate 语义不变）；单口复用留待二期
	// 「节点对单 PC + slot 复用」（BG.3 二期项）一并解决。
	cascadeAPI *webrtc.API
	udpConn    *net.UDPConn
}

// NewEngine 监听单一媒体 UDP 端口并构建 API。
// publicIP 非空时以 NAT1To1 形式对外通告 host candidate。
func NewEngine(mediaUDPPort int, publicIP string) (*Engine, error) {
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: mediaUDPPort})
	if err != nil {
		return nil, fmt.Errorf("listen media udp :%d: %w", mediaUDPPort, err)
	}

	se := webrtc.SettingEngine{}
	se.SetICEUDPMux(webrtc.NewICEUDPMux(nil, udpConn))
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	if publicIP != "" {
		se.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)
	}

	me, err := NewMediaEngine()
	if err != nil {
		udpConn.Close()
		return nil, err
	}

	// 精简 interceptor 链：仅 RTCP report（无音频 NACK、无 jitter buffer，纯转发）。
	ir, err := minimalInterceptors(me)
	if err != nil {
		udpConn.Close()
		return nil, err
	}

	api := webrtc.NewAPI(
		webrtc.WithSettingEngine(se),
		webrtc.WithMediaEngine(me),
		webrtc.WithInterceptorRegistry(ir),
	)

	// 级联 API：与客户端 API 同 MediaEngine 配置，仅去掉 UDPMux（见字段注释）。
	cse := webrtc.SettingEngine{}
	cse.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	if publicIP != "" {
		// 与客户端 API 不同，这里用 srflx 模式：保留接口 host candidate（节点间
		// 内网/同机直连），public_ip 作为附加映射候选（1:1 NAT 场景）。
		// Host 替换模式会让 candidate 宣告 IP 与 socket 绑定接口不一致，
		// 非 mux 的 ephemeral 端口场景下将直接握手失败。
		cse.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeSrflx)
	}
	cme, err := NewMediaEngine()
	if err != nil {
		udpConn.Close()
		return nil, err
	}
	cir, err := minimalInterceptors(cme)
	if err != nil {
		udpConn.Close()
		return nil, err
	}
	cascadeAPI := webrtc.NewAPI(
		webrtc.WithSettingEngine(cse),
		webrtc.WithMediaEngine(cme),
		webrtc.WithInterceptorRegistry(cir),
	)
	return &Engine{api: api, cascadeAPI: cascadeAPI, udpConn: udpConn}, nil
}

// NewMediaEngine 注册 Opus + ssrc-audio-level 头扩展 + 屏幕共享 VP8（bot 与 SFU 共用），
// 以及 simulcast 所需的 mid/rid/rrid 头扩展（发布端未开 simulcast 时协商无副作用）。
func NewMediaEngine() (*webrtc.MediaEngine, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: OpusCodec,
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register opus: %w", err)
	}
	if err := me.RegisterHeaderExtension(
		webrtc.RTPHeaderExtensionCapability{URI: sdp.AudioLevelURI},
		webrtc.RTPCodecTypeAudio,
	); err != nil {
		return nil, fmt.Errorf("register audio-level extension: %w", err)
	}
	// 屏幕共享视频轨（docs 14）：VP8 基线，策略详见 video.go。
	if err := registerVideo(me); err != nil {
		return nil, err
	}
	// 屏幕 simulcast（BA.3）：发布端按 rid 多层上行，SFU 依 mid/rid 扩展分流各层。
	if err := webrtc.ConfigureSimulcastExtensionHeaders(me); err != nil {
		return nil, fmt.Errorf("register simulcast extensions: %w", err)
	}
	return me, nil
}

// NewPeerConnection 创建媒体 PC（无 STUN/TURN：SFU 侧 host candidate 即可）。
func (e *Engine) NewPeerConnection() (*webrtc.PeerConnection, error) {
	return e.api.NewPeerConnection(webrtc.Configuration{})
}

// NewCascadePeerConnection 创建级联专用 PC（独立 ephemeral UDP 端口，
// 不共用 UDPMux；原因见 Engine.cascadeAPI 注释）。
func (e *Engine) NewCascadePeerConnection() (*webrtc.PeerConnection, error) {
	return e.cascadeAPI.NewPeerConnection(webrtc.Configuration{})
}

// Close 关闭共享 UDP 监听。
func (e *Engine) Close() error {
	return e.udpConn.Close()
}
