package signal

// 系统音频伴轨 e2e（docs 14 BA.4）：同会话第二条 audio 轨 = 屏幕共享系统音频伴轨，
// 下行轨 id = <sid>#screen-audio，事件 kind = screen_audio，订阅/退订跟随屏幕，
// 不占屏幕路数配额；无 publish_screen cap 的第二条 audio 轨被剥离。

import (
	"testing"
	"time"

	"github.com/owlspeak/owl-sfu/internal/room"
)

// TestE2EScreenAudioCompanion 伴轨主链路：发布 → 事件/转发 → caps 收回一并结束。
func TestE2EScreenAudioCompanion(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-comp")

	b := dialScreenClient(t, env, env.signTokenCaps(t, "user-b", "sid-cb", "room-comp", time.Minute, audioOnlyCaps), 0)
	defer b.close()
	go b.sendMediaLoop()
	waitFor(t, 5*time.Second, "client B connected", func() bool {
		select {
		case <-b.connected:
			return true
		default:
			return false
		}
	})

	// A：麦克风 + 屏幕 + 伴轨；伴轨在麦克风轨到达后才发包（第二条 audio 轨 = 伴轨）
	a := dialScreenClientOpts(t, env, env.signTokenCaps(t, "user-a", "sid-ca", "room-comp", time.Minute, screenCaps), 1, 1)
	defer a.close()
	go a.sendMediaLoop()

	micKey := "sid-ca"
	companionKey := room.TrackKey("sid-ca", room.KindScreenAudio)

	waitFor(t, 15*time.Second, "B receives mic audio from A", func() bool {
		return b.recvForID(micKey) > 10
	})
	a.startCompanion()
	waitFor(t, 15*time.Second, "B sees track_published kind=screen_audio", func() bool {
		return b.eventCount("track_published:screen_audio") >= 1
	})
	waitFor(t, 15*time.Second, "B receives companion audio on <sid>#screen-audio", func() bool {
		return b.recvForID(companionKey) > 10
	})
	// 伴轨不占屏幕路数配额（BA.4）：屏幕轨计数仍为 1
	if n := env.mgr.ScreenTrackCount(); n != 1 {
		t.Fatalf("companion must not consume screen quota, screen tracks = %d", n)
	}

	// 收回 publish_screen：屏幕与伴轨一并结束（同一共享会话），麦克风不受影响
	if err := env.mgr.UpdateCaps("room-comp", "sid-ca", audioOnlyCaps); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "B sees track_ended kind=screen_audio", func() bool {
		return b.eventCount("track_ended:screen_audio") >= 1
	})
	waitFor(t, 5*time.Second, "B sees track_ended kind=screen", func() bool {
		return b.eventCount("track_ended:screen") >= 1
	})
	time.Sleep(300 * time.Millisecond)
	before := b.recvForID(companionKey)
	time.Sleep(700 * time.Millisecond)
	if after := b.recvForID(companionKey); after != before {
		t.Fatalf("companion forwarding must stop after revoke: %d -> %d", before, after)
	}
	micBase := b.recvForID(micKey)
	waitFor(t, 5*time.Second, "mic audio still flowing", func() bool {
		return b.recvForID(micKey) > micBase+10
	})
}

// TestE2EScreenAudioWithoutCapStripped 无 publish_screen 的第二条 audio 轨被剥离。
func TestE2EScreenAudioWithoutCapStripped(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping e2e in -short mode")
	}
	env := newTestEnv(t)
	env.mgr.EnsureRoom("room-comp2")

	b := dialScreenClient(t, env, env.signTokenCaps(t, "user-b", "sid-cb2", "room-comp2", time.Minute, audioOnlyCaps), 0)
	defer b.close()
	go b.sendMediaLoop()

	// A 无 publish_screen 却带第二条 audio 轨
	a := dialScreenClientOpts(t, env, env.signTokenCaps(t, "user-a", "sid-ca2", "room-comp2", time.Minute, audioOnlyCaps), 0, 1)
	defer a.close()
	go a.sendMediaLoop()

	waitFor(t, 15*time.Second, "B receives mic audio from A", func() bool {
		return b.recvForID("sid-ca2") > 10
	})
	a.startCompanion()

	// 伴轨被剥离：无 screen_audio 事件、无对应下行转发
	time.Sleep(time.Second)
	if n := b.eventCount("track_published:screen_audio"); n != 0 {
		t.Fatalf("expected no screen_audio publish event, got %d", n)
	}
	if got := b.recvForID(room.TrackKey("sid-ca2", room.KindScreenAudio)); got != 0 {
		t.Fatalf("expected no companion RTP forwarded, got %d", got)
	}
}
