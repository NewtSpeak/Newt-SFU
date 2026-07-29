package auth

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

// 验签失败原因码，对齐信令协议 §2.4 关闭码。
const (
	CodeTokenExpired   = "TOKEN_EXPIRED"
	CodeTokenInvalid   = "TOKEN_INVALID"
	CodeWrongNode      = "WRONG_NODE"
	CodeRoomMismatch   = "ROOM_MISMATCH"
	CodeCapDenied      = "CAP_DENIED"
	CodeSessionRevoked = "SESSION_REVOKED"
)

// VerifyError 携带协议关闭码的验签错误。
type VerifyError struct {
	Code string
	Err  error
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e *VerifyError) Unwrap() error { return e.Err }

// ErrCode 提取错误中的协议关闭码，非 VerifyError 时返回 TOKEN_INVALID。
func ErrCode(err error) string {
	var ve *VerifyError
	if errors.As(err, &ve) {
		return ve.Code
	}
	return CodeTokenInvalid
}

// mediaClaims 为 Media Token claims 的 JWT 映射（docs/协议/README.md §1）。
type mediaClaims struct {
	V    int      `json:"v"`
	UID  string   `json:"uid"`
	GID  string   `json:"gid"`
	CID  string   `json:"cid"`
	NID  string   `json:"nid"`
	RID  string   `json:"rid"`
	SID    string   `json:"sid"`
	Bot    bool     `json:"bot"`
	Hidden bool     `json:"hidden"`
	Audit  bool     `json:"audit"`
	Caps   []string `json:"caps"`
	jwt.RegisteredClaims
}

// Verifier 按 kid 持有 Ed25519 公钥并验签 Media Token；同时内置会话吊销表。
type Verifier struct {
	nodeID string
	parser *jwt.Parser

	mu   sync.RWMutex
	keys map[string]ed25519.PublicKey

	revMu    sync.Mutex
	revoked  map[string]time.Time // sid -> 吊销时间
	revTTL   time.Duration
	stopOnce sync.Once
	stopCh   chan struct{}
}

// NewVerifier 创建验签器；keys 可为空，后续经 UpdateKeys 补充。
func NewVerifier(nodeID string, keys []*owlsfuv1.MediaTokenKey) *Verifier {
	v := &Verifier{
		nodeID: nodeID,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"EdDSA"}),
			jwt.WithExpirationRequired(),
		),
		keys:    make(map[string]ed25519.PublicKey),
		revoked: make(map[string]time.Time),
		revTTL:  15 * time.Minute,
		stopCh:  make(chan struct{}),
	}
	v.UpdateKeys(keys)
	go v.revokeJanitor()
	return v
}

// Close 停止后台清理协程。
func (v *Verifier) Close() {
	v.stopOnce.Do(func() { close(v.stopCh) })
}

// UpdateKeys 合并新公钥集（kid 轮换：新 kid 追加，同 kid 覆盖）。
func (v *Verifier) UpdateKeys(keys []*owlsfuv1.MediaTokenKey) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, k := range keys {
		if k == nil || k.GetKid() == "" || len(k.GetEd25519PublicKey()) != ed25519.PublicKeySize {
			continue
		}
		v.keys[k.GetKid()] = ed25519.PublicKey(k.GetEd25519PublicKey())
	}
}

func (v *Verifier) keyByKid(kid string) (ed25519.PublicKey, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	k, ok := v.keys[kid]
	return k, ok
}

// Verify 验签并校验 claims；requireJoin 通常为 true（auth 帧必须含 join cap）。
func (v *Verifier) Verify(raw string) (*Token, error) {
	claims := &mediaClaims{}
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
			return nil, &VerifyError{Code: CodeTokenExpired, Err: err}
		}
		return nil, &VerifyError{Code: CodeTokenInvalid, Err: err}
	}
	if claims.V != 1 {
		return nil, &VerifyError{Code: CodeTokenInvalid, Err: fmt.Errorf("unsupported token version %d", claims.V)}
	}
	if claims.NID != v.nodeID {
		return nil, &VerifyError{Code: CodeWrongNode, Err: fmt.Errorf("token nid %q != node %q", claims.NID, v.nodeID)}
	}
	if claims.SID == "" || claims.RID == "" || claims.UID == "" {
		return nil, &VerifyError{Code: CodeTokenInvalid, Err: errors.New("missing sid/rid/uid claim")}
	}
	caps := NewCapSet(claims.Caps)
	if !caps.Has(CapJoin) {
		return nil, &VerifyError{Code: CodeCapDenied, Err: errors.New("token lacks join cap")}
	}
	if v.IsRevoked(claims.SID) {
		return nil, &VerifyError{Code: CodeSessionRevoked, Err: fmt.Errorf("session %s revoked", claims.SID)}
	}
	tok := &Token{
		UID:  claims.UID,
		GID:  claims.GID,
		CID:  claims.CID,
		NID:  claims.NID,
		RID:  claims.RID,
		SID:    claims.SID,
		Bot:    claims.Bot,
		Hidden: claims.Hidden,
		Audit:  claims.Audit,
		Caps:   caps,
		JTI:    claims.ID,
	}
	if claims.ExpiresAt != nil {
		tok.ExpiresAt = claims.ExpiresAt.Time
	}
	return tok, nil
}

// Revoke 将 session_id 加入吊销表，立即生效。
func (v *Verifier) Revoke(sessionID string) {
	if sessionID == "" {
		return
	}
	v.revMu.Lock()
	v.revoked[sessionID] = time.Now()
	v.revMu.Unlock()
}

// IsRevoked 判断会话是否已吊销。
func (v *Verifier) IsRevoked(sessionID string) bool {
	v.revMu.Lock()
	defer v.revMu.Unlock()
	_, ok := v.revoked[sessionID]
	return ok
}

// revokeJanitor 定期清理超过 TTL 的吊销条目（token 早已过期，无需永久保留）。
func (v *Verifier) revokeJanitor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-v.stopCh:
			return
		case now := <-t.C:
			v.revMu.Lock()
			for sid, at := range v.revoked {
				if now.Sub(at) > v.revTTL {
					delete(v.revoked, sid)
				}
			}
			v.revMu.Unlock()
		}
	}
}
