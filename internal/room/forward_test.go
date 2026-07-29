package room

import (
	"testing"

	"github.com/newtspeak/newt-sfu/internal/auth"
)

func caps(list ...string) auth.CapSet { return auth.NewCapSet(list) }

func TestShouldForward(t *testing.T) {
	full := caps(auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio)
	noPub := caps(auth.CapJoin, auth.CapSubscribeAudio)
	noSub := caps(auth.CapJoin, auth.CapPublishAudio)

	cases := []struct {
		name         string
		pub, sub     auth.CapSet
		unsubscribed bool
		want         bool
	}{
		{"双方全权限且订阅", full, full, false, true},
		{"订阅者已退订", full, full, true, false},
		{"发布者无 publish_audio", noPub, full, false, false},
		{"订阅者无 subscribe_audio", full, noSub, false, false},
		{"双方均无对应权限", noPub, noSub, false, false},
		{"退订且无权限", noPub, noSub, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldForward(tc.pub, tc.sub, tc.unsubscribed); got != tc.want {
				t.Fatalf("ShouldForward(%v,%v,unsub=%v) = %v, want %v",
					tc.pub.Slice(), tc.sub.Slice(), tc.unsubscribed, got, tc.want)
			}
		})
	}
}

// TestCapsUpdateForwardBehavior 模拟 UpdateParticipantCaps 后的转发决策变化：
// 去掉 publish_audio → 停止转发；去掉 subscribe_audio → 不再接收；恢复后转发恢复。
func TestCapsUpdateForwardBehavior(t *testing.T) {
	pub := caps(auth.CapJoin, auth.CapPublishAudio)
	sub := caps(auth.CapJoin, auth.CapSubscribeAudio)

	if !ShouldForward(pub, sub, false) {
		t.Fatal("baseline should forward")
	}

	// Server 下发全量替换：发布者被禁言（收回 publish_audio）
	pubMuted := caps(auth.CapJoin, auth.CapSubscribeAudio)
	if ShouldForward(pubMuted, sub, false) {
		t.Fatal("must stop forwarding after publisher loses publish_audio")
	}

	// 恢复发布权
	if !ShouldForward(pub, sub, false) {
		t.Fatal("forwarding must resume after caps restored")
	}

	// 订阅者被收回 subscribe_audio
	subDeaf := caps(auth.CapJoin)
	if ShouldForward(pub, subDeaf, false) {
		t.Fatal("must stop forwarding after subscriber loses subscribe_audio")
	}
}
