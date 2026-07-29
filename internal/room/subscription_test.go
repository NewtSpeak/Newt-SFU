package room

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

// fakeMsgr 记录发出的信令帧。
type fakeMsgr struct {
	mu  sync.Mutex
	ops []string
}

func (f *fakeMsgr) Send(op string, _ any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
	return nil
}

func (f *fakeMsgr) CloseWithReason(_, _ string) {}

func (f *fakeMsgr) sent(op string) bool {
	return f.count(op) > 0
}

func (f *fakeMsgr) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, o := range f.ops {
		if o == op {
			n++
		}
	}
	return n
}

// newTestRoom 构造不含真实 PC 的房间与参与者（只测订阅图/fanout 决策）。
func newTestRoom(t *testing.T) (*Manager, *Room) {
	t.Helper()
	log := slog.Default()
	mgr := NewManager(log, nil, observability.NewMetrics(), stats.NewCollector(100), 100)
	mgr.EnsureRoom("room-1")
	mgr.mu.RLock()
	r := mgr.rooms["room-1"]
	mgr.mu.RUnlock()
	t.Cleanup(r.stopLoop)
	return mgr, r
}

func addTestParticipant(mgr *Manager, r *Room, sid, uid string, capList ...string) *Participant {
	p := &Participant{
		log:          slog.Default(),
		room:         r,
		sid:          sid,
		uid:          uid,
		msgr:         &fakeMsgr{},
		downTracks:   make(map[string]*downTrack),
		screenDown:   make(map[string]*downTrack),
		unsubscribed: make(map[string]unsubKinds),
		stopExpiry:   make(chan struct{}),
	}
	p.setCaps(auth.NewCapSet(capList))
	mgr.mu.Lock()
	r.parts[sid] = p
	mgr.bySID[sid] = p
	mgr.mu.Unlock()
	return p
}

func fanoutLen(p *Participant) int {
	f := p.fanout.Load()
	if f == nil {
		return 0
	}
	return len(*f)
}

func TestSubscriptionBitmap(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub", auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub", auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	// 模拟发布者上行轨已建立、订阅者持有其下行轨
	pub.publishing.Store(true)
	sub.downTracks[pub.sid] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildFanout(pub)
	if fanoutLen(pub) != 1 {
		t.Fatalf("expected fanout=1, got %d", fanoutLen(pub))
	}

	// 退订（缺省 kinds = 全部维度）：位图记录 + 下行轨失活 + fanout 清空
	sub.Unsubscribe(pub.uid)
	if !sub.isUnsubscribed(pub.uid, KindAudio) || !sub.isUnsubscribed(pub.uid, KindScreen) {
		t.Fatal("unsubscribed bitmap should mark both dimensions")
	}
	if sub.downTracks[pub.sid].active {
		t.Fatal("down track should be inactive after unsubscribe")
	}
	if fanoutLen(pub) != 0 {
		t.Fatalf("expected fanout=0 after unsubscribe, got %d", fanoutLen(pub))
	}

	// 重订阅：恢复转发
	sub.Subscribe(pub.uid)
	if sub.isUnsubscribed(pub.uid, KindAudio) || sub.isUnsubscribed(pub.uid, KindScreen) {
		t.Fatal("unsubscribed bitmap should be cleared after subscribe")
	}
	if fanoutLen(pub) != 1 {
		t.Fatalf("expected fanout=1 after resubscribe, got %d", fanoutLen(pub))
	}
}

// TestSubscriptionKinds 按轨类型订阅粒度（协议 §2.1 kinds 字段）：
// audio / video 两维独立：退订 video 不影响音频转发，反之亦然；
// 叠加退订后按维度分别恢复。
func TestSubscriptionKinds(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	// 发布者同时在播音频 + 屏幕 + 伴轨
	pub.publishing.Store(true)
	screenKey := TrackKey(pub.sid, KindScreen)
	screenAudioKey := TrackKey(pub.sid, KindScreenAudio)
	sub.downTracks[pub.sid] = &downTrack{pubUID: pub.uid, active: true}
	sub.screenDown[screenKey] = &downTrack{pubUID: pub.uid, active: true}
	sub.screenDown[screenAudioKey] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildFanout(pub)
	r.rebuildScreenAudioFanout(pub)

	// 只退订 video：屏幕/伴轨下行失活，音频不受影响
	sub.Unsubscribe(pub.uid, KindVideo)
	if sub.isUnsubscribed(pub.uid, KindAudio) {
		t.Fatal("audio dimension should be untouched by video unsubscribe")
	}
	if !sub.isUnsubscribed(pub.uid, KindScreen) || !sub.isUnsubscribed(pub.uid, KindScreenAudio) {
		t.Fatal("video dimension should mark screen and companion tracks")
	}
	if !sub.downTracks[pub.sid].active {
		t.Fatal("audio down track should stay active")
	}
	if sub.screenDown[screenKey].active || sub.screenDown[screenAudioKey].active {
		t.Fatal("screen/companion down tracks should be inactive")
	}
	if fanoutLen(pub) != 1 {
		t.Fatalf("audio fanout should stay 1, got %d", fanoutLen(pub))
	}
	if got := screenAudioFanoutLen(pub); got != 0 {
		t.Fatalf("companion fanout should be 0 after video unsubscribe, got %d", got)
	}

	// 叠加退订 audio（本地静音）：两维均退订
	sub.Unsubscribe(pub.uid, KindAudio)
	if !sub.isUnsubscribed(pub.uid, KindAudio) {
		t.Fatal("audio dimension should be marked")
	}
	if fanoutLen(pub) != 0 {
		t.Fatalf("audio fanout should be 0, got %d", fanoutLen(pub))
	}

	// 只恢复 video（被本地静音的用户开播，点观看只订视频）：audio 维持退订
	sub.Subscribe(pub.uid, KindVideo)
	if sub.isUnsubscribed(pub.uid, KindScreen) {
		t.Fatal("video dimension should be cleared")
	}
	if !sub.isUnsubscribed(pub.uid, KindAudio) {
		t.Fatal("audio dimension should stay unsubscribed")
	}
	if !sub.screenDown[screenKey].active || !sub.screenDown[screenAudioKey].active {
		t.Fatal("screen/companion down tracks should be reactivated")
	}
	if fanoutLen(pub) != 0 {
		t.Fatalf("audio fanout should stay 0, got %d", fanoutLen(pub))
	}

	// 恢复 audio：位图清空
	sub.Subscribe(pub.uid, KindAudio)
	if sub.isUnsubscribed(pub.uid, KindAudio) || sub.isUnsubscribed(pub.uid, KindScreen) {
		t.Fatal("bitmap should be fully cleared")
	}
	sub.mu.Lock()
	_, present := sub.unsubscribed[pub.uid]
	sub.mu.Unlock()
	if present {
		t.Fatal("fully subscribed publisher should be removed from bitmap")
	}
	if fanoutLen(pub) != 1 {
		t.Fatalf("audio fanout should recover to 1, got %d", fanoutLen(pub))
	}
}

// TestSubscriptionKindsPersistBeforePublish 时序：先 unsubscribe video、发布者
// 后发布屏幕轨——退订状态持久在会话上，ensureScreenDownTrack 按维度拒绝挂轨。
func TestSubscriptionKindsPersistBeforePublish(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio, auth.CapPublishScreen)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub",
		auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	// 发布前退订 video
	sub.Unsubscribe(pub.uid, KindVideo)

	// 发布者屏幕轨上线：转发决策必须为 false（不为其挂屏幕下行轨）
	pub.screenPublishing.Store(true)
	if ShouldForwardScreen(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid, KindScreen)) {
		t.Fatal("screen forwarding should stay off for pre-unsubscribed viewer")
	}
	// 音频维度不受影响
	if !ShouldForward(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid, KindAudio)) {
		t.Fatal("audio forwarding should be unaffected")
	}
}

func TestApplyCapsStopsForwarding(t *testing.T) {
	mgr, r := newTestRoom(t)
	pub := addTestParticipant(mgr, r, "sid-pub", "u-pub", auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)
	sub := addTestParticipant(mgr, r, "sid-sub", "u-sub", auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)

	sub.downTracks[pub.sid] = &downTrack{pubUID: pub.uid, active: true}
	r.rebuildFanout(pub)
	if fanoutLen(pub) != 1 {
		t.Fatalf("baseline fanout=1, got %d", fanoutLen(pub))
	}

	// 收回发布权（全量替换）：fanout 立即清空，caps_updated 已推送
	r.applyCaps(pub, auth.NewCapSet([]string{auth.CapJoin, auth.CapSubscribeAudio}))
	if fanoutLen(pub) != 0 {
		t.Fatalf("expected fanout=0 after publish_audio revoked, got %d", fanoutLen(pub))
	}
	if !pub.msgr.(*fakeMsgr).sent("caps_updated") {
		t.Fatal("caps_updated should be sent to the participant")
	}
	if pub.Caps().Has(auth.CapPublishAudio) {
		t.Fatal("caps should be replaced")
	}

	// 收回订阅权：订阅者侧决策为 false
	// （先清掉模拟下行轨：真实路径中 detach 会经 PC RemoveTrack 完成，这里无真 PC）
	sub.mu.Lock()
	delete(sub.downTracks, pub.sid)
	sub.mu.Unlock()
	r.applyCaps(sub, auth.NewCapSet([]string{auth.CapJoin}))
	if ShouldForward(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid, KindAudio)) {
		t.Fatal("should not forward to subscriber without subscribe_audio")
	}
}
