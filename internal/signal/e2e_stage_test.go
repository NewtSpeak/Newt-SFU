package signal

// 舞台 caps 执行 e2e（进程内，docs 11 AD.4 / 15 BM M5）：
// AUDIENCE（无 publish_audio）上行音频轨被挂起接纳——对端收不到包；
// 抱上麦（UpdateCaps 授予 publish_audio）后对端秒内开始收包（<1s 生效）；
// 抱下（收回 cap）后对端秒内停止收包。

import (
	"testing"
	"time"

	"github.com/newtspeak/newt-sfu/internal/auth"
)

// TestE2EStageBringUpBringDown 挂起接纳 → 抱上即发声 → 抱下即静默。
func TestE2EStageBringUpBringDown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-stage")

	listenCaps := []string{auth.CapJoin, auth.CapSubscribeAudio}
	// 听众 A（对端观察者）：可订阅，正常发布权（其发包与断言无关）。
	a := dialClient(t, env, env.signTokenCaps(t, "user-a", "sid-sta", "room-stage", time.Minute,
		[]string{auth.CapJoin, auth.CapSubscribeAudio, auth.CapPublishAudio}))
	defer a.close()
	waitFor(t, 5*time.Second, "client A ready", func() bool {
		select {
		case <-a.ready:
			return true
		default:
			return false
		}
	})

	// AUDIENCE B：无 publish_audio，但客户端照常推流（loadbot 语义）。
	b := dialClient(t, env, env.signTokenCaps(t, "user-b", "sid-stb", "room-stage", time.Minute, listenCaps))
	defer b.close()
	go b.sendRTPLoop()
	waitFor(t, 5*time.Second, "client B connected", func() bool {
		select {
		case <-b.connected:
			return true
		default:
			return false
		}
	})

	// 阶段 1：AUDIENCE 发包但对端必须收不到（挂起接纳 + 包级门控）。
	time.Sleep(2 * time.Second)
	if got := a.recv.Load(); got != 0 {
		t.Fatalf("AUDIENCE 无 publish_audio 时对端不应收到包，got %d", got)
	}

	// 阶段 2：bring-up（Server 下发 UpdateParticipantCaps 授予 publish_audio）
	// → <1s 对端开始收包（15 BM M5 验收锚点）。
	bringUpAt := time.Now()
	if err := env.mgr.UpdateCaps("room-stage", "sid-stb",
		[]string{auth.CapJoin, auth.CapSubscribeAudio, auth.CapPublishAudio}); err != nil {
		t.Fatalf("UpdateCaps 授予失败: %v", err)
	}
	waitFor(t, 3*time.Second, "A receives RTP after bring-up", func() bool { return a.recv.Load() > 5 })
	elapsed := time.Since(bringUpAt)
	t.Logf("bring-up → 对端首批收包耗时 %s", elapsed)
	if elapsed > time.Second {
		t.Fatalf("bring-up 后应 <1s 可发声，实测 %s", elapsed)
	}

	// 阶段 3：bring-down（收回 publish_audio）→ <1s 停止转发。
	if err := env.mgr.UpdateCaps("room-stage", "sid-stb", listenCaps); err != nil {
		t.Fatalf("UpdateCaps 收回失败: %v", err)
	}
	// 给在途包 1s 排空后采样：3 个 100ms 窗口内不得再有新包。
	time.Sleep(time.Second)
	base := a.recv.Load()
	time.Sleep(500 * time.Millisecond)
	if got := a.recv.Load(); got != base {
		t.Fatalf("bring-down 后仍在转发（recv %d → %d）", base, got)
	}
}
