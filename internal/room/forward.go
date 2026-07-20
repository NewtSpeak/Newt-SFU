package room

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/pion/rtp"

	"github.com/owlspeak/owl-sfu/internal/auth"
)

// speakingLevelThreshold：RFC 6464 audio level（0=最大声，127=静音），
// level < 45 dBov 视为 speaking。
const speakingLevelThreshold = 45

// speakingStaleAfter：超过该时长没有上行包则不再视为 speaking。
const speakingStaleAfter = 500 * time.Millisecond

// ShouldForward 为纯转发决策函数（单测锚点）：
// 发布者需持 publish_audio、订阅者需持 subscribe_audio、且订阅者未退订该发布者。
func ShouldForward(pubCaps, subCaps auth.CapSet, subUnsubscribed bool) bool {
	return pubCaps.Has(auth.CapPublishAudio) &&
		subCaps.Has(auth.CapSubscribeAudio) &&
		!subUnsubscribed
}

// rtpBufPool：转发路径包缓冲复用，避免每包分配。
var rtpBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 1500)
		return &b
	},
}

// forwardLoop 读取发布者上行 RTP 并写入 fanout 快照中的全部下行轨。
// 不解码不转码；SSRC/PT 重写由 TrackLocalStaticRTP 绑定处理。
func (r *Room) forwardLoop(p *Participant, read func([]byte) (int, error), audioLevelExtID uint8) {
	bufp := rtpBufPool.Get().(*[]byte)
	defer rtpBufPool.Put(bufp)
	buf := *bufp

	var pkt rtp.Packet
	for {
		n, err := read(buf)
		if err != nil {
			if !errors.Is(err, io.EOF) && !p.closed.Load() {
				p.log.Debug("publisher track read ended", "err", err)
			}
			return
		}
		// 复用 pkt：先清空上一包的扩展，防止 Unmarshal 残留
		pkt.Header.Extensions = pkt.Header.Extensions[:0]
		pkt.Header.Extension = false
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}

		// caps 门控（热更 + 挂起接纳，docs 11 AD.4）：无 publish_audio 时不转发、
		// 不计 speaking、不录审计——轨保持静默挂起，抱上麦授予 cap 后立即生效；
		// 发布权被收回时同理立即停止（继续读以排空缓冲）。
		if !p.Caps().Has(auth.CapPublishAudio) {
			continue
		}

		// speaking 检测：解析 ssrc-audio-level 扩展
		if audioLevelExtID != 0 {
			if raw := pkt.GetExtension(audioLevelExtID); raw != nil {
				var lvl rtp.AudioLevelExtension
				if err := lvl.Unmarshal(raw); err == nil {
					p.lastLevelAtMs.Store(time.Now().UnixMilli())
					p.lastSpeaking.Store(lvl.Level < speakingLevelThreshold)
				}
			}
		}

		// 音频审计（adminpresence）：旁路录制上行 RTP（不影响转发热路径）。
		if rec := p.rec.Load(); rec != nil {
			rec.write(&pkt)
		}

		fanout := p.fanout.Load()
		if fanout == nil {
			continue
		}
		for _, dt := range *fanout {
			// 单个订阅者写失败（如 PC 正在关闭）不影响其余订阅者
			_ = dt.track.WriteRTP(&pkt)
		}
		if cnt := len(*fanout); cnt > 0 {
			r.mgr.metrics.RTPForwardedPackets.Add(float64(cnt))
			r.mgr.metrics.RTPForwardedBytes.Add(float64(n * cnt))
			r.mgr.stats.AddForwardedBytes(n * cnt)
		}
	}
}
