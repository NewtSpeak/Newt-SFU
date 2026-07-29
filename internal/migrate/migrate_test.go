package migrate

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/newtspeak/newt-sfu/internal/auth"
	"github.com/newtspeak/newt-sfu/internal/observability"
	"github.com/newtspeak/newt-sfu/internal/room"
	owlsfu "github.com/newtspeak/newt-sfu/internal/sfu"
	"github.com/newtspeak/newt-sfu/internal/stats"
)

// fakeMessenger 记录 closed 帧原因码的信令桩。
type fakeMessenger struct {
	closedCode atomic.Value // string
}

func (f *fakeMessenger) Send(string, any) error { return nil }

func (f *fakeMessenger) CloseWithReason(code, _ string) { f.closedCode.Store(code) }

func (f *fakeMessenger) code() string {
	v, _ := f.closedCode.Load().(string)
	return v
}

func newTestRoomManager(t *testing.T) (*room.Manager, *observability.Metrics) {
	t.Helper()
	engine, err := owlsfu.NewEngine(0, "") // 端口 0 = ephemeral UDPMux
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	metrics := observability.NewMetrics()
	mgr := room.NewManager(slog.Default(), engine, metrics, stats.NewCollector(100), 100)
	return mgr, metrics
}

func testToken(uid, sid, rid string) *auth.Token {
	return &auth.Token{
		UID: uid, RID: rid, SID: sid, NID: "test-node",
		Caps:      auth.NewCapSet([]string{auth.CapJoin, auth.CapPublishAudio, auth.CapSubscribeAudio}),
		ExpiresAt: time.Now().Add(time.Minute),
	}
}

// TestDualSessionCoexistence 验证迁移 pre-warm 前提（15 BJ.2）：
// 同一 uid 的新会话（新 sid）与旧会话并存互不冲突（会话键 = sid）。
func TestDualSessionCoexistence(t *testing.T) {
	mgr, _ := newTestRoomManager(t)
	mgr.EnsureRoom("room-1")

	oldMsgr, newMsgr := &fakeMessenger{}, &fakeMessenger{}
	if _, _, err := mgr.Join(testToken("user-a", "sid-old", "room-1"), oldMsgr); err != nil {
		t.Fatalf("旧会话入房失败: %v", err)
	}
	// 同 uid、不同 sid（迁移 CONNECT 阶段的新会话）：必须并存，不得挤掉旧会话
	if _, _, err := mgr.Join(testToken("user-a", "sid-new", "room-1"), newMsgr); err != nil {
		t.Fatalf("新会话入房失败: %v", err)
	}
	if users, _ := mgr.Counts(); users != 2 {
		t.Fatalf("双会话应并存，got %d", users)
	}
	if oldMsgr.code() != "" {
		t.Fatalf("旧会话不应被关闭，got closed code %q", oldMsgr.code())
	}

	// 对照组：同 sid 重连让位（既有语义不被破坏）
	dupMsgr := &fakeMessenger{}
	if _, _, err := mgr.Join(testToken("user-a", "sid-new", "room-1"), dupMsgr); err != nil {
		t.Fatalf("同 sid 重连失败: %v", err)
	}
	if users, _ := mgr.Counts(); users != 2 {
		t.Fatalf("同 sid 重连后仍应为 2 会话，got %d", users)
	}
	if newMsgr.code() != room.CloseSessionRevoked {
		t.Fatalf("同 sid 旧连接应被让位关闭，got %q", newMsgr.code())
	}
}

// TestMark 验证 MARK 阶段（09 §5.1 CONNECT）：标记迁出中但继续服务（不关会话）。
func TestMark(t *testing.T) {
	mgr, metrics := newTestRoomManager(t)
	mgr.EnsureRoom("room-1")

	m1 := &fakeMessenger{}
	p, _, err := mgr.Join(testToken("user-a", "sid-1", "room-1"), m1)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(slog.Default(), mgr, metrics)
	// 标记在线 sid + 一个不存在的 sid（幂等跳过）
	if n := h.Mark("mig-1", "room-1", []string{"sid-1", "sid-gone"}); n != 1 {
		t.Fatalf("应标记 1 个会话，got %d", n)
	}
	if !p.Migrating() {
		t.Fatal("MARK 后会话应带迁出标记")
	}
	if m1.code() != "" {
		t.Fatal("MARK 阶段不得关闭会话（CUTOVER 前继续服务）")
	}
	if users, _ := mgr.Counts(); users != 1 {
		t.Fatalf("MARK 后会话应仍在线，got %d", users)
	}
	// 重放幂等
	if n := h.Mark("mig-1", "room-1", []string{"sid-1"}); n != 1 {
		t.Fatal("MARK 重放应幂等命中")
	}
}

// TestMigrateOut 验证迁出：按 sid 精确摘除、closed 码为 MIGRATED、幂等。
func TestMigrateOut(t *testing.T) {
	mgr, metrics := newTestRoomManager(t)
	mgr.EnsureRoom("room-1")

	m1, m2 := &fakeMessenger{}, &fakeMessenger{}
	if _, _, err := mgr.Join(testToken("user-a", "sid-1", "room-1"), m1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := mgr.Join(testToken("user-b", "sid-2", "room-1"), m2); err != nil {
		t.Fatal(err)
	}

	h := NewHandler(slog.Default(), mgr, metrics)
	// 迁出 sid-1 + 一个不存在的 sid（幂等跳过）
	n := h.MigrateOut("mig-1", "room-1", []string{"sid-1", "sid-gone"}, "node-b")
	if n != 1 {
		t.Fatalf("应断开 1 个会话，got %d", n)
	}
	if m1.code() != room.CloseMigrated {
		t.Fatalf("迁出应使用 MIGRATED 关闭码，got %q", m1.code())
	}
	if m2.code() != "" {
		t.Fatal("未列入迁出的会话不应被断开")
	}
	if users, _ := mgr.Counts(); users != 1 {
		t.Fatalf("迁出后应剩 1 会话，got %d", users)
	}
	// 重放同一批 sid：全部幂等跳过
	if n := h.MigrateOut("mig-1", "room-1", []string{"sid-1"}, "node-b"); n != 0 {
		t.Fatalf("重放迁出应为 0，got %d", n)
	}
}
