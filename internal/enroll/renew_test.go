package enroll

// 证书续期测试：
//   - RenewalDue 纯函数（1/3 剩余有效期阈值）
//   - RenewIfDue 全流程：mTLS 调 RenewCertificate → 原子落盘 → CertSource 热更新
//     （进程内起假 EnrollmentService gRPC 服务，测试 CA 签发/续签节点证书）

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

// TestRenewalDue 剩余有效期 < 总有效期 1/3 才进入续期窗口。
func TestRenewalDue(t *testing.T) {
	now := time.Now()
	mk := func(notBefore, notAfter time.Time) *x509.Certificate {
		return &x509.Certificate{NotBefore: notBefore, NotAfter: notAfter}
	}
	// 总 3h、剩 2h：不续期
	if RenewalDue(mk(now.Add(-time.Hour), now.Add(2*time.Hour)), now) {
		t.Fatal("剩余 2/3 不应续期")
	}
	// 总 3h、剩 30m：续期
	if !RenewalDue(mk(now.Add(-150*time.Minute), now.Add(30*time.Minute)), now) {
		t.Fatal("剩余 < 1/3 应续期")
	}
	// 已过期：续期
	if !RenewalDue(mk(now.Add(-2*time.Hour), now.Add(-time.Minute)), now) {
		t.Fatal("已过期应续期")
	}
}

// ---- 测试 CA 与假 EnrollmentService ----

type renewTestCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newRenewTestCA(t *testing.T) *renewTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Renew Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &renewTestCA{cert: cert, key: key, pool: pool}
}

// issueTLS 直接签一张 tls.Certificate（节点 / 服务端共用）。
func (ca *renewTestCA) issueTLS(t *testing.T, cn string, notBefore, notAfter time.Time, ips []net.IP) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der := ca.signCSRKey(t, cn, &key.PublicKey, notBefore, notAfter, ips)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func (ca *renewTestCA) signCSRKey(t *testing.T, cn string, pub any, notBefore, notAfter time.Time, ips []net.IP) []byte {
	t.Helper()
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     []string{cn},
		IPAddresses:  ips,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// fakeEnrollServer 实现 EnrollmentService：RenewCertificate 校验 mTLS 身份并签新证书。
type fakeEnrollServer struct {
	owlsfuv1.UnimplementedEnrollmentServiceServer
	t     *testing.T
	ca    *renewTestCA
	calls atomic.Int64
}

func (s *fakeEnrollServer) RenewCertificate(_ context.Context, req *owlsfuv1.RenewCertificateRequest) (*owlsfuv1.RenewCertificateResponse, error) {
	s.calls.Add(1)
	block, _ := pem.Decode([]byte(req.GetCsrPem()))
	if block == nil {
		return nil, fmt.Errorf("bad csr pem")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, err
	}
	notAfter := time.Now().Add(6 * time.Hour)
	der := s.ca.signCSRKey(s.t, csr.Subject.CommonName, csr.PublicKey,
		time.Now().Add(-time.Minute), notAfter, nil)
	return &owlsfuv1.RenewCertificateResponse{
		CertificatePem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		CertNotAfter:   notAfter.Format(time.RFC3339),
	}, nil
}

// TestRenewIfDue 全流程：临近过期的节点证书触发续期，落盘 + CertSource 热更新；
// 新证书未到窗口时二次调用为 no-op。
func TestRenewIfDue(t *testing.T) {
	ca := newRenewTestCA(t)
	dataDir := t.TempDir()

	// 节点证书：总 3h、剩 10m → 已进入续期窗口
	nodeCert := ca.issueTLS(t, "node-renew",
		time.Now().Add(-170*time.Minute), time.Now().Add(10*time.Minute), nil)
	source := NewCertSource(nodeCert)

	// 假 EnrollmentService（mTLS：要求并校验客户端证书）
	serverCert := ca.issueTLS(t, "server",
		time.Now().Add(-time.Hour), time.Now().Add(24*time.Hour),
		[]net.IP{net.ParseIP("127.0.0.1")})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.pool,
		MinVersion:   tls.VersionTLS12,
	})))
	fake := &fakeEnrollServer{t: t, ca: ca}
	owlsfuv1.RegisterEnrollmentServiceServer(srv, fake)
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	r := NewRenewer(slog.Default(), RenewerOptions{
		DataDir:  dataDir,
		NodeID:   "node-renew",
		Endpoint: ln.Addr().String(),
		CAPool:   ca.pool,
		Source:   source,
	})

	renewed, err := r.RenewIfDue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !renewed {
		t.Fatal("临近过期应触发续期")
	}
	if fake.calls.Load() != 1 {
		t.Fatalf("RenewCertificate 应被调用 1 次，got %d", fake.calls.Load())
	}

	// 落盘材料可作为一对 key/cert 加载
	loaded, err := tls.LoadX509KeyPair(
		filepath.Join(dataDir, certFile), filepath.Join(dataDir, keyFile))
	if err != nil {
		t.Fatalf("落盘的证书/私钥应可配对加载: %v", err)
	}
	leaf, err := x509.ParseCertificate(loaded.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if leaf.Subject.CommonName != "node-renew" {
		t.Fatalf("新证书 CN 应保持 node_id，got %s", leaf.Subject.CommonName)
	}
	// 文件权限 0600（与首次 Enroll 一致）
	if fi, err := os.Stat(filepath.Join(dataDir, keyFile)); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("私钥权限应为 0600: %v %v", fi.Mode(), err)
	}

	// CertSource 热更新：当前证书已是新证书（有效期变长）
	curLeaf, err := leafOf(source.Get())
	if err != nil {
		t.Fatal(err)
	}
	if !curLeaf.NotAfter.After(time.Now().Add(5 * time.Hour)) {
		t.Fatalf("CertSource 应热更新为新证书, not_after=%s", curLeaf.NotAfter)
	}

	// 新证书剩余 > 1/3：二次调用不应再续期
	renewed, err = r.RenewIfDue(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if renewed || fake.calls.Load() != 1 {
		t.Fatal("未进入续期窗口不应再次续期")
	}

	// 热更新后的证书可用于新的 mTLS 握手（模拟 cascade/control 的 GetCert 路径）
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return source.Get(), nil
			},
			RootCAs:    ca.pool,
			MinVersion: tls.VersionTLS12,
		})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := owlsfuv1.NewEnrollmentServiceClient(conn).RenewCertificate(ctx,
		&owlsfuv1.RenewCertificateRequest{CsrPem: "x"}); err == nil {
		t.Fatal("bad csr 应报错（但握手必须成功）")
	}
	if fake.calls.Load() != 2 {
		t.Fatalf("新证书 mTLS 握手应成功并到达服务端，calls=%d", fake.calls.Load())
	}
}
