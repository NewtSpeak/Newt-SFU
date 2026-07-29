# Newt-SFU

NewtSpeak **媒体面**节点：基于 Go + [Pion](https://github.com/pion/webrtc) 的自研 SFU。  
只做 **选路转发**（不转码、不混流）；业务权限与调度权威在 [Newt-Server](https://github.com/NewtSpeak/Newt-Server)。

```text
Desktop ──WSS 信令──► owl-sfu :8443（常经 Caddy 终结 TLS）
Desktop ──UDP 媒体──► owl-sfu :3478（直连，不经 Server）
owl-sfu ──mTLS gRPC──► Server :9443（主动外连，管理面不反向暴露）
```

## 功能

| 能力 | 说明 |
|------|------|
| **入会与转发** | WebRTC 建连；音频 Opus 轨 fanout |
| **鉴权** | Server 签发的 Media Token（Ed25519 JWT）验签 + caps 执行 |
| **Enrollment** | 一次性 token → CSR → 领 mTLS 证书；剩余 &lt; 1/3 有效期自动续期 |
| **控制通道** | 双向 gRPC 流：注册、心跳、指令与 Ack（迁移/级联/升级等） |
| **屏幕共享** | VP8 选路、系统音频伴轨、simulcast 选层（`set_layer`） |
| **级联 (M3)** | 多节点音频/屏幕转发、NodeWant 剪枝、PLI 跨节点 |
| **热迁移 (M4)** | 执行 Server 编排的会话迁出/迁入；SIGTERM 可 drain |
| **可观测** | `/healthz`、`/metrics`（Prometheus）、可选 pprof |
| **审计上行** | 带 audit claim 的会话可录制并上传 Server |

## 仓库结构

```text
Newt-SFU/
├── cmd/owl-sfu/       # 主进程
├── cmd/loadbot/       # headless Pion 联调 / 压测客户端
├── internal/
│   ├── enroll/        # 领证与续期
│   ├── control/       # mTLS 控制流
│   ├── auth/          # Media Token
│   ├── signal/        # 客户端 WSS + healthz/metrics
│   ├── room/          # 房间与 caps
│   ├── sfu/           # Pion PC / 订阅图
│   ├── cascade/       # 级联
│   ├── migrate/       # 热迁移执行
│   └── … 
├── gen/owlsfu/v1/     # gRPC 生成码（proto 源在 Newt-Server）
├── config.example.yaml
└── Makefile
```

## 端口

| 端口 | 用途 |
|------|------|
| **tcp** 信令（默认 8443） | `/ws`、`/rtt`、`/healthz`、`/metrics` |
| **udp** 3478 | 全部 WebRTC 媒体（UDPMux，**须公网可达**） |
| **tcp** 8843 | 级联 mTLS（多节点时） |
| 出站 **tcp** 9443 | 连 Server 控制面 |

## 快速开始

```bash
cp config.example.yaml config.yaml
# 或全部用 NEWTSFU_ 环境变量（见 config.example.yaml 注释）

# 本机构建
make build
./bin/newt-sfu --config config.yaml

# 交叉编译 linux/amd64
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o bin/newt-sfu ./cmd/owl-sfu
```

**首次上线：**

1. Server 面板「节点管理」创建占位 → 取得 `node_id` + 一次性 `enroll_token`  
2. 配置 `NEWTSFU_SERVER_ENROLL_ENDPOINT`、`NEWTSFU_PUBLIC_IP`、`NEWTSFU_ADVERTISE_WSS_URL`  
3. 启动后证书落盘 `DATA_DIR`；之后可清空 enroll token  

生产推荐：本机 listen + Caddy 终结 WSS；媒体 UDP 直通。详见部署文档。

### loadbot

```bash
go run ./cmd/loadbot \
  --ws-url ws://127.0.0.1:8443/ws \
  --token <media-token-jwt> \
  --duration 30s
```

连 WS → auth → 发合成 Opus，统计下行 RTP；退出码 0 表示至少收到一个远端 track。

## 文档

| 文档 | 说明 |
|------|------|
| [docs/deploy.md](./docs/deploy.md) | **生产部署**（env / systemd / 防火墙） |
| [Newt-Server 部署](https://github.com/NewtSpeak/Newt-Server/blob/main/docs/deploy/server.md) | 控制面与 enroll 前置 |
| [Newt-Server 设计讨论](https://github.com/NewtSpeak/Newt-Server/tree/main/docs/设计讨论) | 选型、级联、热迁移、容量 |
| [Newt-Server 协议](https://github.com/NewtSpeak/Newt-Server/tree/main/docs/协议) | 信令与 token |
| [Newt-Server proto](https://github.com/NewtSpeak/Newt-Server/tree/main/proto) | 控制面 protobuf 权威源 |

## 相关仓库

| 仓库 | 关系 |
|------|------|
| [Newt-Server](https://github.com/NewtSpeak/Newt-Server) | 调度、token、节点生命周期 |
| [Newt-Desktop](https://github.com/NewtSpeak/Newt-Desktop) | 用户媒体客户端 |
| [NewtBotSdk](https://github.com/NewtSpeak/NewtBotSdk) | Bot 进房拿 Media Token，媒体层可参考 loadbot |

## 许可证

双重许可。见 [`LICENSE`](./LICENSE)、[`LICENSE-NONCOMMERCIAL.md`](./LICENSE-NONCOMMERCIAL.md)、[`LICENSE-COMMERCIAL.md`](./LICENSE-COMMERCIAL.md)。
