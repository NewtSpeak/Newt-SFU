// cascade.go 级联凭证验签（docs 08 §6.2 E.4 / 15 BG.2 三重校验之三）：
// Server 用 Media Token 同源 Ed25519 密钥签发 cascade token，claims 绑定
// logical_room_id + epoch + 边两端 node_id，短 TTL。parent 在级联握手时验签，
// 防持证节点跨房/跨边滥用凭证拉流。
package auth

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// CascadeTokenTyp typ claim 固定值（与 Owl-Server mediatoken.CascadeTokenTyp 对齐）。
const CascadeTokenTyp = "cascade"

// cascadeClaims 级联 token claims（docs 协议级联信令一节）。
type cascadeClaims struct {
	V      int    `json:"v"`
	Typ    string `json:"typ"`
	RID    string `json:"rid"`
	Epoch  uint64 `json:"epoch"`
	Parent string `json:"parent"`
	Child  string `json:"child"`
	jwt.RegisteredClaims
}

// VerifyCascade 校验级联 token：EdDSA 签名（Media Token 同源公钥，按 kid）、
// 未过期、且 claims 与该边（room+epoch+parent+child）完全绑定。
func (v *Verifier) VerifyCascade(raw, roomID string, epoch uint64, parentNodeID, childNodeID string) error {
	claims := &cascadeClaims{}
	_, err := v.parser.ParseWithClaims(raw, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("missing kid header")
		}
		key, ok := v.keyByKid(kid)
		if !ok {
			return nil, fmt.Errorf("unknown kid %q", kid)
		}
		return key, nil
	})
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return fmt.Errorf("cascade token expired: %w", err)
		}
		return fmt.Errorf("cascade token invalid: %w", err)
	}
	if claims.Typ != CascadeTokenTyp {
		return fmt.Errorf("cascade token typ %q != %q", claims.Typ, CascadeTokenTyp)
	}
	if claims.RID != roomID || claims.Epoch != epoch ||
		claims.Parent != parentNodeID || claims.Child != childNodeID {
		return fmt.Errorf("cascade token binding mismatch: rid=%s epoch=%d parent=%s child=%s (want %s/%d/%s/%s)",
			claims.RID, claims.Epoch, claims.Parent, claims.Child,
			roomID, epoch, parentNodeID, childNodeID)
	}
	return nil
}
