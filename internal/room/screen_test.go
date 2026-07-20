package room

import (
	"testing"

	"github.com/owlspeak/owl-sfu/internal/auth"
)

// TestShouldForwardScreen 屏幕轨转发决策：发布者须持 publish_screen，
// 观看侧仅要求 join（无独立 subscribe_screen cap），退订位图生效。
func TestShouldForwardScreen(t *testing.T) {
	pubFull := caps(auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	pubNoScreen := caps(auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)
	subJoin := caps(auth.CapJoin, auth.CapSubscribeAudio)
	subNoJoin := caps(auth.CapSubscribeAudio) // 理论上不可能（join 恒给），防御性用例

	cases := []struct {
		name         string
		pub, sub     auth.CapSet
		unsubscribed bool
		want         bool
	}{
		{"发布者持 publish_screen 且观看端在房", pubFull, subJoin, false, true},
		{"发布者无 publish_screen 拒绝", pubNoScreen, subJoin, false, false},
		{"观看端已退订该发布者", pubFull, subJoin, true, false},
		{"观看端无 join（防御）", pubFull, subNoJoin, false, false},
		{"publish_audio 不代表 publish_screen", pubNoScreen, subJoin, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldForwardScreen(tc.pub, tc.sub, tc.unsubscribed); got != tc.want {
				t.Fatalf("ShouldForwardScreen(%v,%v,unsub=%v) = %v, want %v",
					tc.pub.Slice(), tc.sub.Slice(), tc.unsubscribed, got, tc.want)
			}
		})
	}
}

// setupFakeScreenLayers 模拟屏幕上行编码层已建立（单层，非 simulcast；
// 不置 screenPublishing 位：这些用例无真实 PC，避免走 detach 的 RemoveTrack 路径）。
func setupFakeScreenLayers(pub *Participant) {
	pub.mu.Lock()
	pub.screenLayers = map[string]*screenLayer{LayerHigh: {quality: LayerHigh}}
	pub.mu.Unlock()
}

// TestScreenSubscriptionPruning 退订/重订阅对屏幕 fanout 的剪枝（订阅同音频语义）。
func TestScreenSubscriptionPruning(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	// 模拟屏幕上行已建立、观看端已持有屏幕下行轨
	setupFakeScreenLayers(pub)
	key := TrackKey(pub.sid, KindScreen)
	sub.screenDown[key] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildScreenFanout(pub)
	if got := screenFanoutLen(pub); got != 1 {
		t.Fatalf("expected screen fanout=1, got %d", got)
	}

	// 退订：音频与屏幕轨同时失活
	r.setSubscription(sub, pub.uid, false)
	if sub.screenDown[key].active {
		t.Fatal("screen down track should be inactive after unsubscribe")
	}
	if got := screenFanoutLen(pub); got != 0 {
		t.Fatalf("expected screen fanout=0 after unsubscribe, got %d", got)
	}

	// 重订阅：屏幕转发恢复
	r.setSubscription(sub, pub.uid, true)
	if got := screenFanoutLen(pub); got != 1 {
		t.Fatalf("expected screen fanout=1 after resubscribe, got %d", got)
	}
}

// TestApplyCapsRevokesScreen UpdateParticipantCaps 去掉 publish_screen → 屏幕 fanout 立即清空。
func TestApplyCapsRevokesScreen(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	setupFakeScreenLayers(pub)
	sub.screenDown[TrackKey(pub.sid, KindScreen)] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildScreenFanout(pub)
	if got := screenFanoutLen(pub); got != 1 {
		t.Fatalf("baseline screen fanout=1, got %d", got)
	}

	// 全量替换收回 publish_screen（保留音频权限）
	r.applyCaps(pub, auth.NewCapSet([]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio}))
	if got := screenFanoutLen(pub); got != 0 {
		t.Fatalf("expected screen fanout=0 after publish_screen revoked, got %d", got)
	}
	if !pub.msgr.(*fakeMsgr).sent("caps_updated") {
		t.Fatal("caps_updated should be sent to the participant")
	}
	if pub.Caps().Has(auth.CapPublishScreen) {
		t.Fatal("caps should be replaced")
	}
}

// TestFinishScreenPublish 屏幕发布收尾幂等：只广播一次 track_ended，计数只减一次。
func TestFinishScreenPublish(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapSubscribeAudio)

	pub.screenPublishing.Store(true)
	mgr.screenTracks.Add(1)
	if mgr.ScreenTrackCount() != 1 {
		t.Fatalf("expected screen track count 1, got %d", mgr.ScreenTrackCount())
	}

	r.finishScreenPublish(pub)
	r.finishScreenPublish(pub) // 第二次必须为 no-op
	if mgr.ScreenTrackCount() != 0 {
		t.Fatalf("expected screen track count 0, got %d", mgr.ScreenTrackCount())
	}
	if pub.ScreenPublishing() {
		t.Fatal("screen publishing flag should be cleared")
	}
	if !sub.msgr.(*fakeMsgr).sent("track_ended") {
		t.Fatal("subscriber should receive track_ended broadcast")
	}
	if n := sub.msgr.(*fakeMsgr).count("track_ended"); n != 1 {
		t.Fatalf("track_ended must be broadcast exactly once, got %d", n)
	}
}

// screenFanoutLen 统计全部编码层 fanout 中的下行轨总数。
func screenFanoutLen(p *Participant) int {
	p.mu.Lock()
	layers := make([]*screenLayer, 0, len(p.screenLayers))
	for _, l := range p.screenLayers {
		layers = append(layers, l)
	}
	p.mu.Unlock()
	n := 0
	for _, l := range layers {
		if f := l.fanout.Load(); f != nil {
			n += len(*f)
		}
	}
	return n
}
