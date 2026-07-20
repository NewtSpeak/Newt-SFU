# Owl-SFU

OwlSpeak 媒体面节点（Go + Pion 自研 SFU）。控制面见 [Owl-Server](../Owl-Server)。

## 职责

- WebRTC 入会、音频轨选路转发（不转码、不混流）
- Enrollment + mTLS 控制通道（gRPC，主动外连 Server）+ 证书自动续期（剩余 < 1/3 有效期时 RenewCertificate 热更新）
- Media Token（Ed25519 JWT）验签与 caps 执行
- 屏幕共享转发：VP8 选路、系统音频伴轨（BA.4，`<sid>#screen-audio`）、
  simulcast 选层（BA.3 最小版，`set_layer` 信令，rid a/b/c 或 h/m/l）
- 级联转发（M3，音频 + 屏幕 + 伴轨，NodeWant 分 kind 剪枝、PLI 跨节点回传）、热迁移执行（M4）

设计文档：`Owl-Server/docs/设计讨论/`（权威编号 01–15），协议：`Owl-Server/docs/协议/` + `Owl-Server/proto/`。

## 结构

```
cmd/owl-sfu/       入口
cmd/loadbot/       headless Pion 测试客户端（e2e 联调 / 压测）
gen/owlsfu/v1/     buf 生成的 gRPC 代码（源 proto 在 Owl-Server 仓）
internal/
  enroll/          一次性 token → CSR → 领证书
  control/         mTLS gRPC 双向流：注册/心跳/指令+Ack
  auth/            Media Token 验签（Ed25519, kid 轮换）+ 吊销表
  signal/          客户端 WSS 信令 + /rtt + /healthz + /metrics + pprof
  room/            local room / participant（键=sid）/ caps 执行
  sfu/             Pion：PC 管理、track fanout、订阅图
  stats/           容量采集与心跳上报
  observability/   Prometheus 指标定义与挂载
```

## 运行

```bash
cp config.example.yaml config.yaml   # 按需修改；全部字段可用 OWLSFU_ 环境变量覆盖
go run ./cmd/owl-sfu --config config.yaml
```

首次接入需 Server 侧创建节点占位并获取 enroll token（见 Owl-Server 节点管理 API）。

## loadbot（联调/压测）

```bash
go run ./cmd/loadbot --ws-url ws://127.0.0.1:8443/ws --token <media-token-jwt> --duration 30s
```

连 WS → auth → 发布一条合成 Opus 轨并统计收到的下行 RTP；退出码 0 = 收到至少一个远端 track 的 RTP 包。

## 端口

| 端口 | 用途 |
|------|------|
| tcp/443（可配） | 客户端 WSS 信令 + /rtt |
| udp/3478（可配） | 全部 WebRTC 媒体（UDPMux） |
| tcp/8843（可配） | 级联 mTLS 信令（M3） |

## License

双许可，见 LICENSE / LICENSE-COMMERCIAL.md / LICENSE-NONCOMMERCIAL.md。
