package cascade

import (
	"testing"
	"time"

	"github.com/owlspeak/owl-sfu/internal/room"
)

func msFromNow(d time.Duration) int64 { return time.Now().Add(d).UnixMilli() }

// TestLeaseSemantics 验证租约 epoch 单调与过期判定（08 B.5 / §3.1）。
func TestLeaseSemantics(t *testing.T) {
	rs := newRoomState("r1")

	if err := rs.applyLease(Lease{RoomID: "r1", AnchorNodeID: "n1", Epoch: 2, ExpireUnixMs: msFromNow(time.Minute)}); err != nil {
		t.Fatalf("首个租约应被接受: %v", err)
	}
	// 旧 epoch 租约必须被拒（防过期指令回放）
	if err := rs.applyLease(Lease{RoomID: "r1", AnchorNodeID: "n2", Epoch: 1, ExpireUnixMs: msFromNow(time.Minute)}); err == nil {
		t.Fatal("旧 epoch 租约应被拒绝")
	}
	// 同 epoch 续约允许（延长 expire）
	if err := rs.applyLease(Lease{RoomID: "r1", AnchorNodeID: "n1", Epoch: 2, ExpireUnixMs: msFromNow(2 * time.Minute)}); err != nil {
		t.Fatalf("同 epoch 续约应被接受: %v", err)
	}

	now := time.Now()
	// 边集 epoch 未对齐：禁止转发
	if rs.forwardingAllowed(now) {
		t.Fatal("边集 epoch(0) != 租约 epoch(2)，不应允许转发")
	}
	if _, err := rs.applyEdges(2, nil); err != nil {
		t.Fatal(err)
	}
	if !rs.forwardingAllowed(now) {
		t.Fatal("epoch 匹配且租约未过期，应允许转发")
	}
	// 租约过期：停止转发
	if err := rs.applyLease(Lease{RoomID: "r1", AnchorNodeID: "n1", Epoch: 2, ExpireUnixMs: msFromNow(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	if rs.forwardingAllowed(now) {
		t.Fatal("租约已过期，不应允许转发")
	}
}

// TestApplyEdgesEpochReplace 验证换 epoch 全量替换边集语义（08 §4.2）。
func TestApplyEdgesEpochReplace(t *testing.T) {
	rs := newRoomState("r1")
	e1 := Edge{ParentNodeID: "p", ChildNodeID: "c1", ParentEndpoint: "h1:1"}
	e2 := Edge{ParentNodeID: "p", ChildNodeID: "c2", ParentEndpoint: "h1:1"}

	if _, err := rs.applyEdges(1, []Edge{e1, e2}); err != nil {
		t.Fatal(err)
	}
	if rs.epoch != 1 || len(rs.edges) != 2 {
		t.Fatalf("epoch=%d edges=%d，期望 1/2", rs.epoch, len(rs.edges))
	}
	// 边的 room/epoch 字段被规范化
	for _, e := range rs.edges {
		if e.RoomID != "r1" || e.Epoch != 1 {
			t.Fatalf("边未规范化: %+v", e)
		}
	}

	// 挂两个假会话
	s1 := &edgeSession{edge: rs.edges[e1.key()]}
	s2 := &edgeSession{edge: rs.edges[e2.key()]}
	rs.sessions[e1.key()] = s1
	rs.sessions[e2.key()] = s2

	// 同 epoch 幂等重发：会话保留
	removed, err := rs.applyEdges(1, []Edge{e1, e2})
	if err != nil || len(removed) != 0 {
		t.Fatalf("同 epoch 幂等重发不应拆边: removed=%d err=%v", len(removed), err)
	}

	// 升 epoch 且只保留 e1：e2 会话必须拆除
	removed, err = rs.applyEdges(2, []Edge{e1})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != s2 {
		t.Fatalf("应拆除 e2 会话，got %d", len(removed))
	}
	if len(rs.edges) != 1 || rs.epoch != 2 {
		t.Fatalf("新边集应只含 e1、epoch=2")
	}
	if rs.sessions[e1.key()] != s1 {
		t.Fatal("e1 会话应保留（幂等不断边）")
	}
	if s1.edge.Epoch != 2 {
		t.Fatal("保留会话的边定义应刷新到新 epoch")
	}

	// epoch 回退：拒绝
	if _, err := rs.applyEdges(1, nil); err == nil {
		t.Fatal("epoch 回退应被拒绝")
	}

	// endpoint 变化：即使同 key 也要拆除重建
	e1b := Edge{ParentNodeID: "p", ChildNodeID: "c1", ParentEndpoint: "h2:9"}
	removed, err = rs.applyEdges(3, []Edge{e1b})
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != s1 {
		t.Fatal("endpoint 变化应拆除旧会话")
	}
}

// TestClassifyRemoteTrack 验证环路/非预期来源判定（08 C.5）。
func TestClassifyRemoteTrack(t *testing.T) {
	rs := newRoomState("r1")
	edgeA := Edge{RoomID: "r1", ParentNodeID: "me", ChildNodeID: "a"}
	edgeB := Edge{RoomID: "r1", ParentNodeID: "me", ChildNodeID: "b"}
	sa := &edgeSession{edge: edgeA}
	sb := &edgeSession{edge: edgeB}
	rs.sessions[edgeA.key()] = sa
	rs.sessions[edgeB.key()] = sb

	if got := classifyRemoteTrack(rs, sa, "sid-1"); got != "" {
		t.Fatalf("正常来源不应被拒: %s", got)
	}
	// 房间状态缺失
	if got := classifyRemoteTrack(nil, sa, "sid-1"); got != "edge_not_current" {
		t.Fatalf("nil state 应拒绝, got %q", got)
	}
	// 非当前边集的会话（epoch 替换后旧边残包）
	stale := &edgeSession{edge: edgeA}
	if got := classifyRemoteTrack(rs, stale, "sid-1"); got != "edge_not_current" {
		t.Fatalf("非当前会话应拒绝, got %q", got)
	}
	// 本地 speaker 被绕回
	rs.localSpeakers["sid-local"] = localPub{uid: "u1", kind: room.KindAudio}
	if got := classifyRemoteTrack(rs, sa, "sid-local"); got != "speaker_is_local" {
		t.Fatalf("本地 speaker 回环应拒绝, got %q", got)
	}
	// 本地屏幕轨被绕回（trackKey 带 #screen 后缀，同一套判定）
	rs.localSpeakers["sid-local#screen"] = localPub{uid: "u1", kind: room.KindScreen}
	if got := classifyRemoteTrack(rs, sa, "sid-local#screen"); got != "speaker_is_local" {
		t.Fatalf("本地屏幕轨回环应拒绝, got %q", got)
	}
	// 同 speaker 第二来源
	rs.remoteSpeakers["sid-2"] = &remoteSpeaker{key: "sid-2", baseSID: "sid-2", uid: "u2",
		kind: room.KindAudio, origin: sa}
	if got := classifyRemoteTrack(rs, sb, "sid-2"); got != "duplicate_origin" {
		t.Fatalf("第二来源应拒绝, got %q", got)
	}
	// 同屏幕轨第二来源
	rs.remoteSpeakers["sid-2#screen"] = &remoteSpeaker{key: "sid-2#screen", baseSID: "sid-2",
		uid: "u2", kind: room.KindScreen, origin: sa}
	if got := classifyRemoteTrack(rs, sb, "sid-2#screen"); got != "duplicate_origin" {
		t.Fatalf("屏幕轨第二来源应拒绝, got %q", got)
	}
	// 同边重挂（renegotiation）允许
	if got := classifyRemoteTrack(rs, sa, "sid-2"); got != "" {
		t.Fatalf("同边重挂不应被拒: %s", got)
	}
}
