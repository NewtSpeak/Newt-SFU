// Package config 加载 YAML 配置并支持环境变量覆盖（OWLSFU_ 前缀）。
package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// Config 为 owl-sfu 全量配置。
type Config struct {
	NodeID string `yaml:"node_id"`
	// EnrollToken 为 Server 预创建节点占位时发放的一次性 token（仅首次接入需要）。
	EnrollToken string `yaml:"enroll_token"`

	// ServerEnrollEndpoint 为 Newt-Server gRPC 地址（Enroll 用；控制通道优先用
	// EnrollResponse 下发的 control_endpoint，缺省回落到此地址）。
	ServerEnrollEndpoint string `yaml:"server_enroll_endpoint"`
	// EnrollInsecure 为 true 时 Enroll 首次连接跳过 TLS 校验（仅 dev）；
	// 拿到 ca_bundle 后控制通道始终用 CA 严格校验。
	EnrollInsecure bool `yaml:"enroll_insecure"`
	// DataDir 存节点私钥/证书/CA/控制面地址/media token 公钥。
	DataDir string `yaml:"data_dir"`

	// WSSListen 为客户端信令监听地址（含 /ws /rtt /healthz /metrics /debug/pprof）。
	WSSListen string `yaml:"wss_listen"`
	// NoTLS 为 true 时信令端点退化为纯 HTTP/WS（dev/bot 联调）。
	NoTLS       bool   `yaml:"no_tls"`
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`

	MediaUDPPort int `yaml:"media_udp_port"`
	// PublicIP 非空时配置 NAT1To1 host candidate。
	PublicIP string `yaml:"public_ip"`
	// AdvertiseWSSURL 经控制通道上报、由 Server 下发给客户端的信令地址。
	AdvertiseWSSURL string `yaml:"advertise_wss_url"`

	// MaxUsers 本地防御硬顶。
	MaxUsers int `yaml:"max_users"`

	// CascadeListen 级联 mTLS 信令监听地址（15 BH：默认 tcp/8843）。
	CascadeListen string `yaml:"cascade_listen"`
	// AdvertiseCascadeEndpoint 经控制通道上报给 Server 的级联端点 host:port
	//（Server 下发 CascadeEdge.parent_cascade_endpoint 时使用）；
	// 为空时回落 public_ip + cascade_listen 端口。
	AdvertiseCascadeEndpoint string `yaml:"advertise_cascade_endpoint"`

	// DrainTimeoutSec SIGTERM 后等待 Server 迁空会话的时限（09 I.6 默认 60s，超时硬关）。
	DrainTimeoutSec int `yaml:"drain_timeout_sec"`

	// ---- 音频审计（adminpresence 专项）----
	// AuditIngestURL 主节点录音上传地址（形如 https://server/audit-api/records）；
	// 为空时审计录音仅落本地盘不上传。
	AuditIngestURL string `yaml:"audit_ingest_url"`
	// AuditIngestToken 与 Newt-Server AUDIT_INGEST_TOKEN 对齐的共享密钥。
	AuditIngestToken string `yaml:"audit_ingest_token"`
	// AuditKeepLocal 上传成功后是否保留本地录音（默认 false = 删除节省磁盘）。
	AuditKeepLocal bool `yaml:"audit_keep_local"`
}

// Default 返回带默认值的配置。
func Default() Config {
	return Config{
		ServerEnrollEndpoint: "127.0.0.1:9443",
		DataDir:              "./data",
		WSSListen:            ":8443",
		MediaUDPPort:         3478,
		MaxUsers:             1200,
		CascadeListen:        ":8843",
		DrainTimeoutSec:      60,
	}
}

// Load 读取 YAML 文件（可不存在，此时仅默认值+环境变量），再应用环境变量覆盖。
func Load(path string) (Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("parse config %s: %w", path, err)
		}
	case os.IsNotExist(err):
		// 允许无配置文件，全部走环境变量
	default:
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	applyEnv(&cfg)
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if c.MediaUDPPort <= 0 || c.MediaUDPPort > 65535 {
		return fmt.Errorf("media_udp_port out of range: %d", c.MediaUDPPort)
	}
	if !c.NoTLS && (c.TLSCertFile == "" || c.TLSKeyFile == "") {
		return fmt.Errorf("tls_cert_file/tls_key_file required unless no_tls=true")
	}
	return nil
}

func applyEnv(c *Config) {
	envStr("OWLSFU_NODE_ID", &c.NodeID)
	envStr("OWLSFU_ENROLL_TOKEN", &c.EnrollToken)
	envStr("OWLSFU_SERVER_ENROLL_ENDPOINT", &c.ServerEnrollEndpoint)
	envBool("OWLSFU_ENROLL_INSECURE", &c.EnrollInsecure)
	envStr("OWLSFU_DATA_DIR", &c.DataDir)
	envStr("OWLSFU_WSS_LISTEN", &c.WSSListen)
	envBool("OWLSFU_NO_TLS", &c.NoTLS)
	envStr("OWLSFU_TLS_CERT_FILE", &c.TLSCertFile)
	envStr("OWLSFU_TLS_KEY_FILE", &c.TLSKeyFile)
	envInt("OWLSFU_MEDIA_UDP_PORT", &c.MediaUDPPort)
	envStr("OWLSFU_PUBLIC_IP", &c.PublicIP)
	envStr("OWLSFU_ADVERTISE_WSS_URL", &c.AdvertiseWSSURL)
	envInt("OWLSFU_MAX_USERS", &c.MaxUsers)
	envStr("OWLSFU_CASCADE_LISTEN", &c.CascadeListen)
	envStr("OWLSFU_ADVERTISE_CASCADE_ENDPOINT", &c.AdvertiseCascadeEndpoint)
	envInt("OWLSFU_DRAIN_TIMEOUT_SEC", &c.DrainTimeoutSec)
	envStr("OWLSFU_AUDIT_INGEST_URL", &c.AuditIngestURL)
	envStr("OWLSFU_AUDIT_INGEST_TOKEN", &c.AuditIngestToken)
	envBool("OWLSFU_AUDIT_KEEP_LOCAL", &c.AuditKeepLocal)
}

func envStr(key string, dst *string) {
	if v, ok := os.LookupEnv(key); ok {
		*dst = v
	}
}

func envBool(key string, dst *bool) {
	if v, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			*dst = b
		}
	}
}

func envInt(key string, dst *int) {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			*dst = n
		}
	}
}
