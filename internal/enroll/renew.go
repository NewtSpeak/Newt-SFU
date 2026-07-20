// 证书续期（03 §4）：节点证书剩余有效期低于 1/3 时，用现有 mTLS 身份调
// EnrollmentService.RenewCertificate 换新证书，原子落盘 data_dir 并经 CertSource
// 热更新 control/cascade 的 TLS 配置（无需重启）。
package enroll

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
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
)

// CertSource 持有节点证书的当前版本，供 TLS 配置以回调形式取用
// （tls.Config.GetCertificate / GetClientCertificate），续期后 Set 即全局热更新。
type CertSource struct {
	cur atomic.Pointer[tls.Certificate]
}

// NewCertSource 用初始证书构建。
func NewCertSource(cert tls.Certificate) *CertSource {
	s := &CertSource{}
	s.cur.Store(&cert)
	return s
}

// Get 返回当前证书（并发安全；给 tls.Config 回调用）。
func (s *CertSource) Get() *tls.Certificate { return s.cur.Load() }

// Set 原子替换当前证书。
func (s *CertSource) Set(cert tls.Certificate) { s.cur.Store(&cert) }

// RenewalDue 判断证书是否进入续期窗口：剩余有效期 < 总有效期的 1/3。
func RenewalDue(leaf *x509.Certificate, now time.Time) bool {
	total := leaf.NotAfter.Sub(leaf.NotBefore)
	remaining := leaf.NotAfter.Sub(now)
	return remaining < total/3
}

// RenewerOptions 为 Renewer 输入。
type RenewerOptions struct {
	DataDir string
	NodeID  string
	// Endpoint 为 EnrollmentService gRPC 地址（与控制通道同源，mTLS）。
	Endpoint string
	// CAPool 校验 Server 证书（enrollment 下发的集群 CA）。
	CAPool *x509.CertPool
	Source *CertSource
	// CheckInterval 检查周期（默认 1h）；RetryInterval 进入续期窗口后
	// 调用失败的重试间隔（默认 1m）。
	CheckInterval time.Duration
	RetryInterval time.Duration
}

// Renewer 为证书续期后台任务。
type Renewer struct {
	log  *slog.Logger
	opts RenewerOptions
}

// NewRenewer 构建续期任务。
func NewRenewer(log *slog.Logger, opts RenewerOptions) *Renewer {
	if opts.CheckInterval <= 0 {
		opts.CheckInterval = time.Hour
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = time.Minute
	}
	return &Renewer{log: log.With("mod", "renew"), opts: opts}
}

// Run 周期检查并续期，直到 ctx 取消。
func (r *Renewer) Run(ctx context.Context) {
	for {
		wait := r.opts.CheckInterval
		renewed, err := r.RenewIfDue(ctx)
		switch {
		case err != nil:
			// 已进入续期窗口但调用失败：缩短重试间隔（窗口 = 1/3 有效期，时间充裕）
			r.log.Warn("certificate renewal failed, will retry", "err", err,
				"retry_in", r.opts.RetryInterval.String())
			wait = r.opts.RetryInterval
		case renewed:
			r.log.Info("node certificate renewed")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// RenewIfDue 检查当前证书，进入续期窗口则执行续期；返回是否完成了一次续期。
func (r *Renewer) RenewIfDue(ctx context.Context) (bool, error) {
	cur := r.opts.Source.Get()
	leaf, err := leafOf(cur)
	if err != nil {
		return false, fmt.Errorf("parse current cert: %w", err)
	}
	if !RenewalDue(leaf, time.Now()) {
		return false, nil
	}
	r.log.Info("certificate entering renewal window",
		"not_after", leaf.NotAfter.Format(time.RFC3339))

	// 新私钥 + CSR（与首次 Enroll 同规格：ECDSA P-256、CN = node_id）
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return false, fmt.Errorf("generate key: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return false, fmt.Errorf("marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: r.opts.NodeID},
	}, priv)
	if err != nil {
		return false, fmt.Errorf("create csr: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	// RenewCertificate 走 mTLS：客户端证书经 Source 取当前版本
	conn, err := grpc.NewClient(r.opts.Endpoint, grpc.WithTransportCredentials(
		credentials.NewTLS(&tls.Config{
			GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
				return r.opts.Source.Get(), nil
			},
			RootCAs:    r.opts.CAPool,
			MinVersion: tls.VersionTLS12,
		})))
	if err != nil {
		return false, fmt.Errorf("grpc client: %w", err)
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := owlsfuv1.NewEnrollmentServiceClient(conn).RenewCertificate(callCtx,
		&owlsfuv1.RenewCertificateRequest{CsrPem: string(csrPEM)})
	if err != nil {
		return false, fmt.Errorf("renew rpc: %w", err)
	}
	if resp.GetCertificatePem() == "" {
		return false, fmt.Errorf("renew response missing certificate")
	}
	newCert, err := tls.X509KeyPair([]byte(resp.GetCertificatePem()), keyPEM)
	if err != nil {
		return false, fmt.Errorf("new cert/key mismatch: %w", err)
	}

	// 原子落盘（temp + rename；key 先落，两文件间崩溃由下次启动 loadIdentity 报错兜底，
	// 旧材料在 rename 前始终完整）
	if err := atomicWrite(filepath.Join(r.opts.DataDir, keyFile), keyPEM); err != nil {
		return false, err
	}
	if err := atomicWrite(filepath.Join(r.opts.DataDir, certFile),
		[]byte(resp.GetCertificatePem())); err != nil {
		return false, err
	}
	if ca := resp.GetCaBundlePem(); ca != "" {
		if err := atomicWrite(filepath.Join(r.opts.DataDir, caFile), []byte(ca)); err != nil {
			return false, err
		}
	}

	// 热更新：cascade 监听/拨号与控制通道的 TLS 回调即刻取到新证书
	r.opts.Source.Set(newCert)
	r.log.Info("certificate renewed and hot-swapped",
		"cert_not_after", resp.GetCertNotAfter())
	return true, nil
}

// leafOf 解析 tls.Certificate 的叶子证书（Leaf 未缓存时解析 DER）。
func leafOf(cert *tls.Certificate) (*x509.Certificate, error) {
	if cert.Leaf != nil {
		return cert.Leaf, nil
	}
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("empty certificate chain")
	}
	return x509.ParseCertificate(cert.Certificate[0])
}

// atomicWrite 临时文件 + rename 的原子写（0600）。
func atomicWrite(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", path, err)
	}
	return nil
}
