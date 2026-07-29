// Package enroll 实现节点接入：一次性 token → 本地私钥 + CSR → 领取节点证书与 CA。
package enroll

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	owlsfuv1 "github.com/newtspeak/newt-sfu/gen/owlsfu/v1"
)

const (
	keyFile             = "node.key"
	certFile            = "node.crt"
	caFile              = "ca.pem"
	mediaKeysFile       = "media_keys.json"
	controlEndpointFile = "control_endpoint"
)

// Identity 为节点身份材料：mTLS 证书、集群 CA、控制面地址、media token 验签公钥。
type Identity struct {
	NodeCert tls.Certificate
	CAPool   *x509.CertPool
	// ControlEndpoint 为 Enroll 时 Server 下发的控制通道 gRPC 地址（可为空，
	// 空时回落到配置的 server_enroll_endpoint）。
	ControlEndpoint string
	MediaTokenKeys  []*owlsfuv1.MediaTokenKey
}

// Options 为 EnsureIdentity 输入。
type Options struct {
	DataDir        string
	NodeID         string
	EnrollToken    string
	EnrollEndpoint string
	// EnrollInsecure 为 true 时 Enroll 阶段跳过 Server 证书校验（仅 dev）。
	EnrollInsecure bool
	NodeVersion    string
}

// EnsureIdentity 加载已有证书；不存在时执行 Enroll 流程并落盘（文件权限 0600）。
func EnsureIdentity(ctx context.Context, log *slog.Logger, opts Options) (*Identity, error) {
	if err := os.MkdirAll(opts.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir data_dir: %w", err)
	}
	keyPath := filepath.Join(opts.DataDir, keyFile)
	certPath := filepath.Join(opts.DataDir, certFile)

	if fileExists(keyPath) && fileExists(certPath) {
		log.Info("loading existing node identity", "data_dir", opts.DataDir)
		return loadIdentity(opts.DataDir)
	}
	log.Info("no node certificate found, enrolling", "server", opts.EnrollEndpoint, "enroll_insecure", opts.EnrollInsecure)
	return doEnroll(ctx, log, opts)
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// doEnroll 生成私钥与 CSR，调 EnrollmentService.Enroll，保存全部身份材料。
func doEnroll(ctx context.Context, log *slog.Logger, opts Options) (*Identity, error) {
	if opts.EnrollToken == "" {
		return nil, fmt.Errorf("enroll_token required for first enroll")
	}

	// 证书用 ECDSA P-256（比 Ed25519 在 TLS 生态更通用）
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: opts.NodeID},
	}, priv)
	if err != nil {
		return nil, fmt.Errorf("create csr: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	// bootstrap 阶段的 TLS：dev 可跳过校验；生产应预置公有信任或系统根
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if opts.EnrollInsecure {
		tlsCfg.InsecureSkipVerify = true //nolint:gosec // dev bootstrap 专用，拿到 CA 后严格校验
	}
	conn, err := grpc.NewClient(opts.EnrollEndpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("grpc client: %w", err)
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := owlsfuv1.NewEnrollmentServiceClient(conn).Enroll(callCtx, &owlsfuv1.EnrollRequest{
		NodeId:          opts.NodeID,
		EnrollmentToken: opts.EnrollToken,
		CsrPem:          string(csrPEM),
		NodeVersion:     opts.NodeVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("enroll rpc: %w", err)
	}
	if resp.GetCertificatePem() == "" || resp.GetCaBundlePem() == "" {
		return nil, fmt.Errorf("enroll response missing certificate or ca bundle")
	}

	// 全部落盘（0600）
	files := map[string][]byte{
		keyFile:  keyPEM,
		certFile: []byte(resp.GetCertificatePem()),
		caFile:   []byte(resp.GetCaBundlePem()),
	}
	if ep := resp.GetControlEndpoint(); ep != "" {
		files[controlEndpointFile] = []byte(ep)
	}
	if b, err := marshalMediaKeys(resp.GetMediaTokenKeys()); err == nil {
		files[mediaKeysFile] = b
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(opts.DataDir, name), data, 0o600); err != nil {
			return nil, fmt.Errorf("write %s: %w", name, err)
		}
	}
	log.Info("enrollment complete",
		"cert_not_after", resp.GetCertNotAfter(),
		"media_token_keys", len(resp.GetMediaTokenKeys()))
	return loadIdentity(opts.DataDir)
}

// loadIdentity 从 data_dir 加载身份材料。
func loadIdentity(dataDir string) (*Identity, error) {
	cert, err := tls.LoadX509KeyPair(filepath.Join(dataDir, certFile), filepath.Join(dataDir, keyFile))
	if err != nil {
		return nil, fmt.Errorf("load node cert: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(dataDir, caFile))
	if err != nil {
		return nil, fmt.Errorf("read ca bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("ca bundle contains no valid certificates")
	}
	id := &Identity{NodeCert: cert, CAPool: pool}

	// 控制面地址与 media token 公钥缺失不致命：前者回落配置，后者 RegisterAck 会再次下发
	if raw, err := os.ReadFile(filepath.Join(dataDir, controlEndpointFile)); err == nil {
		id.ControlEndpoint = strings.TrimSpace(string(raw))
	}
	if raw, err := os.ReadFile(filepath.Join(dataDir, mediaKeysFile)); err == nil {
		id.MediaTokenKeys, _ = unmarshalMediaKeys(raw)
	}
	return id, nil
}

// mediaKeyJSON 为 media token 公钥落盘格式。
type mediaKeyJSON struct {
	Kid       string `json:"kid"`
	PubKeyB64 string `json:"ed25519_public_key_b64"`
}

func marshalMediaKeys(keys []*owlsfuv1.MediaTokenKey) ([]byte, error) {
	out := make([]mediaKeyJSON, 0, len(keys))
	for _, k := range keys {
		out = append(out, mediaKeyJSON{
			Kid:       k.GetKid(),
			PubKeyB64: base64.StdEncoding.EncodeToString(k.GetEd25519PublicKey()),
		})
	}
	return json.MarshalIndent(out, "", "  ")
}

func unmarshalMediaKeys(raw []byte) ([]*owlsfuv1.MediaTokenKey, error) {
	var in []mediaKeyJSON
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	out := make([]*owlsfuv1.MediaTokenKey, 0, len(in))
	for _, k := range in {
		pub, err := base64.StdEncoding.DecodeString(k.PubKeyB64)
		if err != nil {
			continue
		}
		out = append(out, &owlsfuv1.MediaTokenKey{Kid: k.Kid, Ed25519PublicKey: pub})
	}
	return out, nil
}
