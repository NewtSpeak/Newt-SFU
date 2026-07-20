package cascade

import "testing"

// TestWantSetBasics 验证三种形态的需求判定。
func TestWantSetBasics(t *testing.T) {
	none := WantNone()
	if none.Wants("a") || !none.IsNone() {
		t.Fatal("WantNone 不应需要任何 speaker")
	}
	all := WantAllExcept(nil)
	if !all.Wants("a") || !all.Wants("b") || all.IsNone() {
		t.Fatal("WantAllExcept(nil) 应需要全部 speaker")
	}
	ae := WantAllExcept([]string{"muted"})
	if ae.Wants("muted") || !ae.Wants("other") {
		t.Fatal("all_except 排除集判定错误")
	}
	only := WantOnly([]string{"x"})
	if !only.Wants("x") || only.Wants("y") {
		t.Fatal("only 集合判定错误")
	}
}

// TestWantSetUnion 验证 NodeWant 聚合算子（08 §5.1：OR 语义）。
func TestWantSetUnion(t *testing.T) {
	cases := []struct {
		name       string
		a, b       WantSet
		wants      []string
		notWants   []string
		expectNone bool
	}{
		{
			name:     "all_except ∪ all_except = 交集排除",
			a:        WantAllExcept([]string{"x", "y"}),
			b:        WantAllExcept([]string{"y", "z"}),
			wants:    []string{"x", "z", "other"}, // x 被 b 需要、z 被 a 需要
			notWants: []string{"y"},               // 双方都排除
		},
		{
			name:     "all_except ∪ only = 差集排除",
			a:        WantAllExcept([]string{"x", "y"}),
			b:        WantOnly([]string{"x"}),
			wants:    []string{"x", "other"},
			notWants: []string{"y"},
		},
		{
			name:  "only ∪ only = 并集",
			a:     WantOnly([]string{"a"}),
			b:     WantOnly([]string{"b"}),
			wants: []string{"a", "b"},
			notWants: []string{
				"c",
			},
		},
		{
			name:       "none ∪ none = none",
			a:          WantNone(),
			b:          WantNone(),
			notWants:   []string{"a"},
			expectNone: true,
		},
		{
			name:  "none ∪ all = all",
			a:     WantNone(),
			b:     WantAllExcept(nil),
			wants: []string{"a", "b"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Union 必须可交换
			for _, u := range []WantSet{Union(tc.a, tc.b), Union(tc.b, tc.a)} {
				for _, x := range tc.wants {
					if !u.Wants(x) {
						t.Fatalf("应需要 %s", x)
					}
				}
				for _, x := range tc.notWants {
					if u.Wants(x) {
						t.Fatalf("不应需要 %s", x)
					}
				}
				if u.IsNone() != tc.expectNone {
					t.Fatalf("IsNone = %v, 期望 %v", u.IsNone(), tc.expectNone)
				}
			}
		})
	}
}

// TestWantSetWireRoundtrip 验证 want 消息序列化往返与 Equal 去重。
func TestWantSetWireRoundtrip(t *testing.T) {
	for _, w := range []WantSet{
		WantNone(),
		WantAllExcept(nil),
		WantAllExcept([]string{"a", "b"}),
		WantOnly([]string{"x"}),
	} {
		got := wantFromWire(w.toWire())
		if !got.Equal(w) {
			t.Fatalf("roundtrip 不等价: %+v vs %+v", got, w)
		}
	}
	if WantAllExcept([]string{"a"}).Equal(WantAllExcept([]string{"b"})) {
		t.Fatal("不同排除集不应相等")
	}
	if WantAllExcept(nil).Equal(WantOnly(nil)) {
		t.Fatal("all 与 none 不应相等")
	}
}
