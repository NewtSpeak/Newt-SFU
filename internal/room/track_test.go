package room

import (
	"testing"

	"github.com/newtspeak/newt-sfu/internal/auth"
)

// TestTrackKeyRoundtrip 轨 id 约定编解码往返（客户端下行与级联共用）。
func TestTrackKeyRoundtrip(t *testing.T) {
	cases := []struct {
		sid, kind, key string
	}{
		{"sid-1", KindAudio, "sid-1"},
		{"sid-1", KindScreen, "sid-1#screen"},
		{"sid-1", KindScreenAudio, "sid-1#screen-audio"},
	}
	for _, tc := range cases {
		if got := TrackKey(tc.sid, tc.kind); got != tc.key {
			t.Fatalf("TrackKey(%s,%s) = %s, want %s", tc.sid, tc.kind, got, tc.key)
		}
		sid, kind := SplitTrackKey(tc.key)
		if sid != tc.sid || kind != tc.kind {
			t.Fatalf("SplitTrackKey(%s) = (%s,%s), want (%s,%s)", tc.key, sid, kind, tc.sid, tc.kind)
		}
	}
	// "#screen-audio" 必须先于 "#screen" 匹配（后者是前者的前缀）
	if _, kind := SplitTrackKey("x#screen-audio"); kind != KindScreenAudio {
		t.Fatal("#screen-audio 后缀应解析为 screen_audio")
	}
}

// TestLocalScreenDemand 屏幕轨 NodeWant 本地分量：观看侧仅要求 join
// （与音频的 subscribe_audio 区分），退订位图共用。
func TestLocalScreenDemand(t *testing.T) {
	m := newDemandTestManager(t)
	m.EnsureRoom("r1")

	// 空房：无需求
	if want, _ := m.LocalScreenDemand("r1"); want {
		t.Fatal("空房不应有屏幕需求")
	}

	// 只有 join（无 subscribe_audio）的成员：音频无需求、屏幕有需求
	watcher := demandJoin(t, m, "uw", "sw", []string{auth.CapJoin})
	if want, _ := m.LocalDemand("r1"); want {
		t.Fatal("无 subscribe_audio 听众时音频不应有需求")
	}
	want, except := m.LocalScreenDemand("r1")
	if !want || len(except) != 0 {
		t.Fatalf("join 成员即构成屏幕需求: want=%v except=%v", want, except)
	}

	// 唯一观看端退订 ux → ux 的屏幕轨进入排除集（未订阅不得跨节点拉流）
	watcher.Unsubscribe("ux")
	if _, except = m.LocalScreenDemand("r1"); len(except) != 1 || except[0] != "ux" {
		t.Fatalf("全体退订应剪枝 ux 的屏幕轨: %v", except)
	}
	// 第二名观看端进房（默认全订）→ ux 恢复被需要
	demandJoin(t, m, "u2", "s2", []string{auth.CapJoin})
	if _, except = m.LocalScreenDemand("r1"); len(except) != 0 {
		t.Fatalf("仍有观看端需要 ux: %v", except)
	}
}

// TestScreenAudioSubscriptionFollowsScreen 伴轨订阅/退订跟随屏幕（BA.4）：
// unsubscribe(user) 同时使伴轨下行失活，重订阅恢复。
func TestScreenAudioSubscriptionFollowsScreen(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	key := TrackKey(pub.sid, KindScreenAudio)
	sub.screenDown[key] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildScreenAudioFanout(pub)
	if got := screenAudioFanoutLen(pub); got != 1 {
		t.Fatalf("expected screen-audio fanout=1, got %d", got)
	}

	sub.Unsubscribe(pub.uid)
	if sub.screenDown[key].active {
		t.Fatal("companion down track should be inactive after unsubscribe")
	}
	if got := screenAudioFanoutLen(pub); got != 0 {
		t.Fatalf("expected screen-audio fanout=0 after unsubscribe, got %d", got)
	}

	sub.Subscribe(pub.uid)
	if got := screenAudioFanoutLen(pub); got != 1 {
		t.Fatalf("expected screen-audio fanout=1 after resubscribe, got %d", got)
	}
}

// TestScreenAudioCapsRevoke 收回 publish_screen → 伴轨 fanout 同步清空（跟随屏幕会话）。
func TestScreenAudioCapsRevoke(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	sub.screenDown[TrackKey(pub.sid, KindScreenAudio)] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildScreenAudioFanout(pub)
	if got := screenAudioFanoutLen(pub); got != 1 {
		t.Fatalf("baseline screen-audio fanout=1, got %d", got)
	}

	r.applyCaps(pub, auth.NewCapSet([]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio}))
	if got := screenAudioFanoutLen(pub); got != 0 {
		t.Fatalf("expected screen-audio fanout=0 after publish_screen revoked, got %d", got)
	}
}

func screenAudioFanoutLen(p *Participant) int {
	f := p.screenAudioFanout.Load()
	if f == nil {
		return 0
	}
	return len(*f)
}

// TestPickScreenLayer 选层回退：请求档存在则用之，否则回退最高可用档。
func TestPickScreenLayer(t *testing.T) {
	h, m, l := &screenLayer{quality: LayerHigh}, &screenLayer{quality: LayerMedium}, &screenLayer{quality: LayerLow}
	all := map[string]*screenLayer{LayerHigh: h, LayerMedium: m, LayerLow: l}
	if pickScreenLayer(all, LayerMedium) != m {
		t.Fatal("请求档存在应直取")
	}
	if pickScreenLayer(all, "") != h {
		t.Fatal("未选层缺省最高档")
	}
	onlyLow := map[string]*screenLayer{LayerLow: l}
	if pickScreenLayer(onlyLow, LayerHigh) != l {
		t.Fatal("请求档缺失应回退可用档")
	}
	if pickScreenLayer(map[string]*screenLayer{}, LayerHigh) != nil {
		t.Fatal("无层应返回 nil")
	}
}

// TestRIDQuality rid → 质量档映射（a/b/c 与 h/m/l 两套约定）。
func TestRIDQuality(t *testing.T) {
	cases := map[string]string{
		"": LayerHigh, "a": LayerHigh, "h": LayerHigh,
		"b": LayerMedium, "m": LayerMedium,
		"c": LayerLow, "l": LayerLow,
		"x": "",
	}
	for rid, want := range cases {
		if got := ridQuality(rid); got != want {
			t.Fatalf("ridQuality(%q) = %q, want %q", rid, got, want)
		}
	}
}
