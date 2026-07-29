package room

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

// noopMessenger 信令桩。
type noopMessenger struct{ closedCode atomic.Value }

func (n *noopMessenger) Send(string, any) error { return nil }

func (n *noopMessenger) CloseWithReason(code, _ string) { n.closedCode.Store(code) }

func newDemandTestManager(t *testing.T) *Manager {
	t.Helper()
	engine, err := owlsfu.NewEngine(0, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	return NewManager(slog.Default(), engine, observability.NewMetrics(), stats.NewCollector(100), 100)
}

func demandJoin(t *testing.T, m *Manager, uid, sid string, caps []string) *Participant {
	t.Helper()
	p, _, err := m.Join(&auth.Token{
		UID: uid, RID: "r1", SID: sid, NID: "n1",
		Caps: auth.NewCapSet(caps), ExpiresAt: time.Now().Add(time.Minute),
	}, &noopMessenger{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// TestLocalDemand 验证 NodeWant 的本地分量聚合（08 §5.1）：
// 默认全订；speaker 仅当「每个听众要么是其本人要么已退订」才进入排除集。
func TestLocalDemand(t *testing.T) {
	m := newDemandTestManager(t)
	m.EnsureRoom("r1")

	// 空房：无需求
	if want, _ := m.LocalDemand("r1"); want {
		t.Fatal("空房不应有需求")
	}
	// 不存在的房间：无需求
	if want, _ := m.LocalDemand("nope"); want {
		t.Fatal("未知房间不应有需求")
	}

	full := []string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio}
	pa := demandJoin(t, m, "ua", "sa", full)
	pb := demandJoin(t, m, "ub", "sb", full)

	// 默认全订：want=true 且无排除
	want, except := m.LocalDemand("r1")
	if !want || len(except) != 0 {
		t.Fatalf("默认全订: want=%v except=%v", want, except)
	}

	// ua 退订 ux：ub 仍要 ux → 不进排除集
	pa.Unsubscribe("ux")
	if _, except = m.LocalDemand("r1"); len(except) != 0 {
		t.Fatalf("仅一名听众退订不应剪枝: %v", except)
	}
	// ub 也退订 ux：全体听众都不要 → 进入排除集
	pb.Unsubscribe("ux")
	if _, except = m.LocalDemand("r1"); len(except) != 1 || except[0] != "ux" {
		t.Fatalf("全体退订应剪枝 ux: %v", except)
	}
	// speaker 本人不构成对自己的需求：ub 退订 ua 后，唯一的另一听众是 ua 本人 → ua 进排除集
	pb.Unsubscribe("ua")
	_, except = m.LocalDemand("r1")
	hasUA := false
	for _, x := range except {
		if x == "ua" {
			hasUA = true
		}
	}
	if !hasUA {
		t.Fatalf("ua 应进入排除集（唯一潜在听众是其本人）: %v", except)
	}

	// 无 subscribe cap 的成员不算听众
	pa.Close(CloseDisconnected, "bye")
	pb.Close(CloseDisconnected, "bye")
	demandJoin(t, m, "uc", "sc", []string{auth.CapJoin, auth.CapPublishAudio})
	if want, _ := m.LocalDemand("r1"); want {
		t.Fatal("无 subscribe_audio 听众时不应有需求")
	}
}
