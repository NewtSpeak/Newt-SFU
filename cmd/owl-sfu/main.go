// owl-sfu 主入口：配置 → enrollment → metrics/signal/UDPMux → 控制通道 → 优雅退出。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	ossignal "os/signal"
	"path/filepath"
	"syscall"
	"time"

	owlsfuv1 "github.com/owlspeak/owl-sfu/gen/owlsfu/v1"
	"github.com/owlspeak/owl-sfu/internal/audit"
	"github.com/owlspeak/owl-sfu/internal/auth"
	"github.com/owlspeak/owl-sfu/internal/buildinfo"
	"github.com/owlspeak/owl-sfu/internal/cascade"
	"github.com/owlspeak/owl-sfu/internal/config"
	"github.com/owlspeak/owl-sfu/internal/control"
	"github.com/owlspeak/owl-sfu/internal/enroll"
	"github.com/owlspeak/owl-sfu/internal/migrate"
	"github.com/owlspeak/owl-sfu/internal/observability"
	"github.com/owlspeak/owl-sfu/internal/room"
	owlsfu "github.com/owlspeak/owl-sfu/internal/sfu"
	"github.com/owlspeak/owl-sfu/internal/signal"
	"github.com/owlspeak/owl-sfu/internal/stats"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(*configPath, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// controlBackend 把控制指令适配到 room.Manager / auth.Verifier / cascade / migrate。
type controlBackend struct {
	mgr      *room.Manager
	verifier *auth.Verifier
	cascade  *cascade.Manager
	migrate  *migrate.Handler
}

func (b *controlBackend) EnsureRoom(roomID string) { b.mgr.EnsureRoom(roomID) }

func (b *controlBackend) CloseRoom(roomID string) {
	b.mgr.CloseRoom(roomID)
	b.cascade.CloseRoom(roomID) // 房间回收时同步拆除级联状态与边
}

func (b *controlBackend) DisconnectUser(roomID, userID, sessionID, reason string) int {
	return b.mgr.DisconnectUser(roomID, userID, sessionID, reason)
}

func (b *controlBackend) RevokeSession(sessionID, _ string) bool {
	// 先入吊销表（防同 token 重连），再断在线会话
	b.verifier.Revoke(sessionID)
	return b.mgr.CloseSession(sessionID, room.CloseSessionRevoked, "session revoked by server")
}

func (b *controlBackend) UpdateCaps(roomID, sessionID string, caps []string) error {
	return b.mgr.UpdateCaps(roomID, sessionID, caps)
}

func (b *controlBackend) SetDraining(v bool) { b.mgr.SetDraining(v) }

// ---- 级联指令（M3）----

func (b *controlBackend) SetAnchorLease(roomID, anchorNodeID string, epoch uint64, expireUnixMs int64) error {
	return b.cascade.SetAnchorLease(roomID, anchorNodeID, epoch, expireUnixMs)
}

func (b *controlBackend) SetCascadeEdges(roomID string, epoch uint64, edges []*owlsfuv1.CascadeEdge) error {
	return b.cascade.SetCascadeEdges(roomID, epoch, edges)
}

// ---- 热迁移指令（M4）----

func (b *controlBackend) MigrateParticipantsMark(migrationID, roomID string, sessionIDs []string) int {
	return b.migrate.Mark(migrationID, roomID, sessionIDs)
}

func (b *controlBackend) MigrateParticipants(migrationID, roomID string, sessionIDs []string, toNodeID string) int {
	return b.migrate.MigrateOut(migrationID, roomID, sessionIDs, toNodeID)
}

func run(configPath string, log *slog.Logger) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log.Info("config loaded", "node_id", cfg.NodeID, "version", buildinfo.Version,
		"wss_listen", cfg.WSSListen, "media_udp_port", cfg.MediaUDPPort)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// enrollment（已有证书直接加载）
	identity, err := enroll.EnsureIdentity(ctx, log, enroll.Options{
		DataDir:        cfg.DataDir,
		NodeID:         cfg.NodeID,
		EnrollToken:    cfg.EnrollToken,
		EnrollEndpoint: cfg.ServerEnrollEndpoint,
		EnrollInsecure: cfg.EnrollInsecure,
		NodeVersion:    buildinfo.Version,
	})
	if err != nil {
		return fmt.Errorf("enrollment: %w", err)
	}
	// 控制通道地址：优先 Enroll 下发的 control_endpoint，缺省回落 enroll 地址
	controlAddr := identity.ControlEndpoint
	if controlAddr == "" {
		controlAddr = cfg.ServerEnrollEndpoint
	}

	// 证书热更新源：cascade/control 的 TLS 配置经回调取当前证书，续期后即刻生效
	certSource := enroll.NewCertSource(identity.NodeCert)

	verifier := auth.NewVerifier(cfg.NodeID, identity.MediaTokenKeys)
	defer verifier.Close()

	metrics := observability.NewMetrics()
	st := stats.NewCollector(cfg.MaxUsers)

	engine, err := owlsfu.NewEngine(cfg.MediaUDPPort, cfg.PublicIP)
	if err != nil {
		return fmt.Errorf("media engine: %w", err)
	}
	defer engine.Close()
	log.Info("media udp mux listening", "port", cfg.MediaUDPPort, "public_ip", cfg.PublicIP)

	mgr := room.NewManager(log, engine, metrics, st, cfg.MaxUsers)

	// 音频审计（adminpresence 专项）：token 带 audit claim 的会话录制上行音频，
	// 落 DataDir/audit 后经 uploader 上传主节点。
	// 本地 yaml/env 优先；若留空则等 RegisterAck 由主节点下发（零配置对齐）。
	auditDir := filepath.Join(cfg.DataDir, "audit")
	var lastAuditURL, lastAuditToken string
	applyAuditIngest := func(url, token string, source string) {
		if url == lastAuditURL && token == lastAuditToken {
			return
		}
		lastAuditURL, lastAuditToken = url, token
		if url == "" || token == "" {
			mgr.SetAudit(cfg.NodeID, auditDir, nil)
			if source == "local_config" {
				log.Info("audit ingest waiting for RegisterAck (or set OWLSFU_AUDIT_INGEST_*)")
			}
			return
		}
		uploader := audit.NewUploader(log, url, token, cfg.AuditKeepLocal)
		mgr.SetAudit(cfg.NodeID, auditDir, uploader)
		log.Info("audit ingest configured", "url", url, "source", source)
	}
	applyAuditIngest(cfg.AuditIngestURL, cfg.AuditIngestToken, "local_config")

	// 级联（M3）：mTLS 信令端口 + 节点间媒体（复用 engine UDPMux）；
	// token 验签走 Media Token 同源公钥（Enroll/RegisterAck 下发，BG.2 三重校验）。
	casc, err := cascade.New(log, cascade.Config{
		NodeID:      cfg.NodeID,
		Listen:      cfg.CascadeListen,
		Cert:        identity.NodeCert,
		CAPool:      identity.CAPool,
		GetCert:     certSource.Get,
		VerifyToken: verifier.VerifyCascade,
	}, engine, mgr, metrics)
	if err != nil {
		return fmt.Errorf("cascade: %w", err)
	}
	defer casc.Close()
	mgr.SetCascade(casc)

	mig := migrate.NewHandler(log, mgr, metrics)

	sigServer := signal.NewServer(log, cfg.WSSListen, cfg.NoTLS,
		cfg.TLSCertFile, cfg.TLSKeyFile, verifier, mgr, metrics)
	sigServer.Start()

	// 证书续期后台任务（03 §4）：剩余有效期 < 1/3 时经 mTLS 调 RenewCertificate，
	// 原子落盘并经 certSource 热更新 cascade/control 的 TLS 配置
	renewer := enroll.NewRenewer(log, enroll.RenewerOptions{
		DataDir:  cfg.DataDir,
		NodeID:   cfg.NodeID,
		Endpoint: controlAddr,
		CAPool:   identity.CAPool,
		Source:   certSource,
	})
	go renewer.Run(ctx)

	// 控制通道
	ctrl := control.NewClient(log, controlAddr, buildinfo.Version,
		certSource.Get, identity.CAPool,
		&owlsfuv1.NodeAdvertise{
			WssUrl:          cfg.AdvertiseWSSURL,
			MediaUdpPort:    uint32(cfg.MediaUDPPort),
			MediaIps:        advertiseIPs(cfg.PublicIP),
			CascadeEndpoint: cascadeEndpoint(cfg, casc.Addr()),
		},
		&controlBackend{mgr: mgr, verifier: verifier, cascade: casc, migrate: mig},
		verifier, st, metrics)
	// RegisterAck 可下发 audit_ingest_*：本地未配置时采用主节点下发值。
	ctrl.SetOnRegisterAck(func(ack *owlsfuv1.RegisterAck) {
		url, token := cfg.AuditIngestURL, cfg.AuditIngestToken
		if url == "" {
			url = ack.GetAuditIngestUrl()
		}
		if token == "" {
			token = ack.GetAuditIngestToken()
		}
		// 仅在相对启动时有变化时重配，避免心跳重连反复刷日志；
		// 但 token/url 可能从空变为有，必须应用。
		applyAuditIngest(url, token, "register_ack")
	})
	mgr.SetEvents(ctrl)
	casc.SetReporter(ctrl)
	go ctrl.Run(ctx)

	// SIGTERM/SIGINT 优雅退出：先请求 Drain（09 §3.5），等 Server 迁空或超时再硬关
	sigCh := make(chan os.Signal, 2)
	ossignal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Info("shutdown signal received, requesting drain", "signal", sig.String(),
		"drain_timeout_sec", cfg.DrainTimeoutSec)

	// 本地立即停止接受新会话，并向 Server 请求排空（编排/迁移权威在 Server）
	mgr.SetDraining(true)
	ctrl.RequestDrain("sigterm")
	waitDrained(log, mgr, sigCh, time.Duration(cfg.DrainTimeoutSec)*time.Second)

	mgr.CloseAll(room.CloseNodeDraining, "node shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	sigServer.Shutdown(shutdownCtx)
	casc.Close()
	cancel() // 停控制通道
	log.Info("shutdown complete")
	return nil
}

// waitDrained 等待存量会话被 Server 迁空：全空 / 超时（硬迁剩余，09 I.6）/ 二次信号即返回。
func waitDrained(log *slog.Logger, mgr *room.Manager, sigCh <-chan os.Signal, timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	deadline := time.After(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			users, _ := mgr.Counts()
			log.Warn("drain timeout, forcing shutdown", "remaining_sessions", users)
			return
		case s := <-sigCh:
			log.Warn("second signal received, skipping drain wait", "signal", s.String())
			return
		case <-tick.C:
			if users, _ := mgr.Counts(); users == 0 {
				log.Info("all sessions drained")
				return
			}
		}
	}
}

// cascadeEndpoint 计算对外通告的级联端点：显式配置优先，否则 public_ip + 监听端口。
func cascadeEndpoint(cfg config.Config, listenAddr string) string {
	if cfg.AdvertiseCascadeEndpoint != "" {
		return cfg.AdvertiseCascadeEndpoint
	}
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return ""
	}
	host := cfg.PublicIP
	if host == "" {
		if ips := advertiseIPs(""); len(ips) > 0 {
			host = ips[0]
		}
	}
	if host == "" {
		return ""
	}
	return net.JoinHostPort(host, port)
}

// advertiseIPs 返回对外通告的媒体 IP：public_ip 优先，否则取本机非环回 IPv4。
func advertiseIPs(publicIP string) []string {
	if publicIP != "" {
		return []string{publicIP}
	}
	var out []string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.To4() == nil {
			continue
		}
		out = append(out, ipNet.IP.String())
	}
	return out
}
