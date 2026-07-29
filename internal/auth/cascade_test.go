package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

// signCascade 模拟 Newt-Server 侧签发（claims 结构对齐 mediatoken.SignCascade）。
func signCascade(t *testing.T, priv ed25519.PrivateKey, kid, typ, rid string,
	epoch uint64, parent, child string, ttl time.Duration) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"v": 1, "typ": typ, "rid": rid, "epoch": epoch,
		"parent": parent, "child": child,
		"iat": now.Unix(), "exp": now.Add(ttl).Unix(), "jti": "test-jti",
	})
	tok.Header["kid"] = kid
	raw, err := tok.SignedString(priv)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestVerifyCascade 三重校验之 token 项：签名 + TTL + room/epoch/edge 绑定。
func TestVerifyCascade(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	v := NewVerifier("node-c", []*owlsfuv1.MediaTokenKey{{Kid: "k1", Ed25519PublicKey: pub}})
	defer v.Close()

	good := signCascade(t, priv, "k1", CascadeTokenTyp, "room-1", 3, "node-p", "node-c", time.Minute)
	if err := v.VerifyCascade(good, "room-1", 3, "node-p", "node-c"); err != nil {
		t.Fatalf("合法 token 应通过: %v", err)
	}

	cases := []struct {
		name  string
		token string
		rid   string
		epoch uint64
		p, c  string
	}{
		{"房间不匹配", good, "room-2", 3, "node-p", "node-c"},
		{"epoch 不匹配", good, "room-1", 4, "node-p", "node-c"},
		{"parent 不匹配", good, "room-1", 3, "node-x", "node-c"},
		{"child 不匹配", good, "room-1", 3, "node-p", "node-x"},
		{"typ 错误", signCascade(t, priv, "k1", "media", "room-1", 3, "node-p", "node-c", time.Minute),
			"room-1", 3, "node-p", "node-c"},
		{"已过期", signCascade(t, priv, "k1", CascadeTokenTyp, "room-1", 3, "node-p", "node-c", -time.Second),
			"room-1", 3, "node-p", "node-c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := v.VerifyCascade(tc.token, tc.rid, tc.epoch, tc.p, tc.c); err == nil {
				t.Fatal("应校验失败")
			}
		})
	}

	// 未知 kid / 错误密钥签名。
	_, otherPriv, _ := ed25519.GenerateKey(rand.Reader)
	forged := signCascade(t, otherPriv, "k1", CascadeTokenTyp, "room-1", 3, "node-p", "node-c", time.Minute)
	if err := v.VerifyCascade(forged, "room-1", 3, "node-p", "node-c"); err == nil {
		t.Fatal("非 Server 密钥签发的 token 应被拒")
	}
}
