// Package cascade 实现池内级联（docs 08/15 BG）：AnchorLease/epoch 执行、
// 节点间 mTLS 信令 + WebRTC PC 媒体、NodeWant 订阅剪枝与环路防御。
package cascade

import "sort"

// WantSet 表示一个「speaker 需求集合」（NodeWant，08 §5.1）。
// 因为客户端默认全订（静音才退订），需求集合有两种形态：
//   - all=true：要「除 except 外的全部 speaker」（能覆盖未来出现的新 speaker）
//   - all=false：只要 include 中列出的 speaker（目前只用于「空集」，保留一般性）
type WantSet struct {
	all     bool
	except  map[string]struct{} // all=true 时的排除集
	include map[string]struct{} // all=false 时的需要集
}

// WantNone 返回空需求（无人订阅任何 speaker）。
func WantNone() WantSet { return WantSet{} }

// WantAllExcept 返回「全要，除 except 外」。
func WantAllExcept(except []string) WantSet {
	w := WantSet{all: true}
	if len(except) > 0 {
		w.except = make(map[string]struct{}, len(except))
		for _, x := range except {
			w.except[x] = struct{}{}
		}
	}
	return w
}

// WantOnly 返回「只要这些 speaker」。
func WantOnly(uids []string) WantSet {
	w := WantSet{}
	if len(uids) > 0 {
		w.include = make(map[string]struct{}, len(uids))
		for _, x := range uids {
			w.include[x] = struct{}{}
		}
	}
	return w
}

// Wants 判断是否需要某 speaker（按 uid）。
func (w WantSet) Wants(uid string) bool {
	if w.all {
		_, off := w.except[uid]
		return !off
	}
	_, on := w.include[uid]
	return on
}

// IsNone 判断是否为空需求。
func (w WantSet) IsNone() bool { return !w.all && len(w.include) == 0 }

// Union 返回两个需求集合的并（NodeWant 聚合算子）：
//
//	all_except(A) ∪ all_except(B) = all_except(A ∩ B)
//	all_except(A) ∪ only(S)       = all_except(A \ S)
//	only(S1)      ∪ only(S2)      = only(S1 ∪ S2)
func Union(a, b WantSet) WantSet {
	switch {
	case a.all && b.all:
		inter := make(map[string]struct{})
		for x := range a.except {
			if _, ok := b.except[x]; ok {
				inter[x] = struct{}{}
			}
		}
		return WantSet{all: true, except: inter}
	case a.all:
		return unionAllExceptOnly(a, b)
	case b.all:
		return unionAllExceptOnly(b, a)
	default:
		u := make(map[string]struct{}, len(a.include)+len(b.include))
		for x := range a.include {
			u[x] = struct{}{}
		}
		for x := range b.include {
			u[x] = struct{}{}
		}
		return WantSet{include: u}
	}
}

func unionAllExceptOnly(ae, only WantSet) WantSet {
	diff := make(map[string]struct{})
	for x := range ae.except {
		if _, ok := only.include[x]; !ok {
			diff[x] = struct{}{}
		}
	}
	return WantSet{all: true, except: diff}
}

// Equal 判断两个需求集合是否等价（want 消息去重用）。
func (w WantSet) Equal(o WantSet) bool {
	if w.all != o.all {
		return false
	}
	if w.all {
		return setEqual(w.except, o.except)
	}
	return setEqual(w.include, o.include)
}

func setEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for x := range a {
		if _, ok := b[x]; !ok {
			return false
		}
	}
	return true
}

// wireWant 为 want 消息的线上形态。
type wireWant struct {
	All  bool     `json:"all"`
	UIDs []string `json:"uids,omitempty"` // all=true 时为 except，否则为 include
}

// toWire 序列化（列表排序保证稳定）。
func (w WantSet) toWire() wireWant {
	var uids []string
	src := w.include
	if w.all {
		src = w.except
	}
	for x := range src {
		uids = append(uids, x)
	}
	sort.Strings(uids)
	return wireWant{All: w.all, UIDs: uids}
}

// wantFromWire 反序列化。
func wantFromWire(ww wireWant) WantSet {
	if ww.All {
		return WantAllExcept(ww.UIDs)
	}
	return WantOnly(ww.UIDs)
}
