package room

import (
	"log/slog"
	"sync"
	"testing"

	"github.com/owlspeak/owl-sfu/internal/auth"
	"github.com/owlspeak/owl-sfu/internal/observability"
	"github.com/owlspeak/owl-sfu/internal/stats"
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
		unsubscribed: make(map[string]struct{}),
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

	// 退订：位图记录 + 下行轨失活 + fanout 清空
	r.setSubscription(sub, pub.uid, false)
	if !sub.isUnsubscribed(pub.uid) {
		t.Fatal("unsubscribed bitmap should contain publisher uid")
	}
	if sub.downTracks[pub.sid].active {
		t.Fatal("down track should be inactive after unsubscribe")
	}
	if fanoutLen(pub) != 0 {
		t.Fatalf("expected fanout=0 after unsubscribe, got %d", fanoutLen(pub))
	}

	// 重订阅：恢复转发
	r.setSubscription(sub, pub.uid, true)
	if sub.isUnsubscribed(pub.uid) {
		t.Fatal("unsubscribed bitmap should be cleared after subscribe")
	}
	if fanoutLen(pub) != 1 {
		t.Fatalf("expected fanout=1 after resubscribe, got %d", fanoutLen(pub))
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
	if ShouldForward(pub.Caps(), sub.Caps(), sub.isUnsubscribed(pub.uid)) {
		t.Fatal("should not forward to subscriber without subscribe_audio")
	}
}
