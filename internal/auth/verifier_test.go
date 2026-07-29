package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

const testNodeID = "node-1"

type tokenOpts struct {
	kid  string
	v    int
	nid  string
	sid  string
	rid  string
	uid  string
	caps []string
	exp  time.Duration
}

func defaultOpts() tokenOpts {
	return tokenOpts{
		kid: "k1", v: 1, nid: testNodeID,
		sid: "sess-1", rid: "room-1", uid: "user-1",
		caps: []string{CapJoin, CapPublishAudio, CapSubscribeAudio},
		exp:  2 * time.Minute,
	}
}

func signToken(t *testing.T, priv ed25519.PrivateKey, o tokenOpts) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{
		"v": o.v, "uid": o.uid, "gid": "guild-1", "cid": "chan-1",
		"nid": o.nid, "rid": o.rid, "sid": o.sid, "caps": o.caps,
		"iat": now.Unix(), "exp": now.Add(o.exp).Unix(), "jti": "jti-1",
	})
	tok.Header["kid"] = o.kid
	raw, err := tok.SignedString(priv)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return raw
}

func newTestVerifier(t *testing.T) (*Verifier, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	v := NewVerifier(testNodeID, []*owlsfuv1.MediaTokenKey{
		{Kid: "k1", Ed25519PublicKey: pub},
	})
	t.Cleanup(v.Close)
	return v, priv
}

func TestVerify(t *testing.T) {
	v, priv := newTestVerifier(t)
	_, otherPriv, _ := ed25519.GenerateKey(rand.Reader)

	cases := []struct {
		name     string
		token    func() string
		wantCode string // "" 表示应通过
	}{
		{
			name:  "合法 token",
			token: func() string { return signToken(t, priv, defaultOpts()) },
		},
		{
			name: "过期",
			token: func() string {
				o := defaultOpts()
				o.exp = -time.Minute
				return signToken(t, priv, o)
			},
			wantCode: CodeTokenExpired,
		},
		{
			name: "错 nid",
			token: func() string {
				o := defaultOpts()
				o.nid = "node-other"
				return signToken(t, priv, o)
			},
			wantCode: CodeWrongNode,
		},
		{
			name: "错签名（其他私钥）",
			token: func() string {
				return signToken(t, otherPriv, defaultOpts())
			},
			wantCode: CodeTokenInvalid,
		},
		{
			name: "未知 kid",
			token: func() string {
				o := defaultOpts()
				o.kid = "k-unknown"
				return signToken(t, priv, o)
			},
			wantCode: CodeTokenInvalid,
		},
		{
			name: "错版本",
			token: func() string {
				o := defaultOpts()
				o.v = 2
				return signToken(t, priv, o)
			},
			wantCode: CodeTokenInvalid,
		},
		{
			name: "缺 join cap",
			token: func() string {
				o := defaultOpts()
				o.caps = []string{CapSubscribeAudio}
				return signToken(t, priv, o)
			},
			wantCode: CodeCapDenied,
		},
		{
			name: "已吊销会话",
			token: func() string {
				o := defaultOpts()
				o.sid = "sess-revoked"
				v.Revoke("sess-revoked")
				return signToken(t, priv, o)
			},
			wantCode: CodeSessionRevoked,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tok, err := v.Verify(tc.token())
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("expected valid token, got error: %v", err)
				}
				if tok.SID != "sess-1" || tok.UID != "user-1" || tok.RID != "room-1" {
					t.Fatalf("unexpected claims: %+v", tok)
				}
				if !tok.Caps.Has(CapJoin) || !tok.Caps.Has(CapPublishAudio) {
					t.Fatalf("caps not parsed: %v", tok.Caps.Slice())
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error code %s, got nil", tc.wantCode)
			}
			if got := ErrCode(err); got != tc.wantCode {
				t.Fatalf("expected code %s, got %s (%v)", tc.wantCode, got, err)
			}
		})
	}
}

func TestUpdateKeysRotation(t *testing.T) {
	v, _ := newTestVerifier(t)
	pub2, priv2, _ := ed25519.GenerateKey(rand.Reader)

	o := defaultOpts()
	o.kid = "k2"
	raw := signToken(t, priv2, o)

	if _, err := v.Verify(raw); err == nil {
		t.Fatal("expected failure before key rotation")
	}
	v.UpdateKeys([]*owlsfuv1.MediaTokenKey{{Kid: "k2", Ed25519PublicKey: pub2}})
	if _, err := v.Verify(raw); err != nil {
		t.Fatalf("expected success after key rotation: %v", err)
	}
}

func TestRevokeAfterVerify(t *testing.T) {
	v, priv := newTestVerifier(t)
	raw := signToken(t, priv, defaultOpts())
	if _, err := v.Verify(raw); err != nil {
		t.Fatalf("precondition: %v", err)
	}
	v.Revoke("sess-1")
	if _, err := v.Verify(raw); err == nil || ErrCode(err) != CodeSessionRevoked {
		t.Fatalf("expected SESSION_REVOKED after revoke, got %v", err)
	}
}
