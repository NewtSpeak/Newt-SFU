// Package buildinfo 暴露 owl-sfu 运行时版本（可由 -ldflags 注入）。
package buildinfo

// Version 节点程序版本号，Register / Heartbeat 上报给 Server。
// 发布时建议：
//
//	go build -ldflags "-X github.com/owlspeak/owl-sfu/internal/buildinfo.Version=1.2.3"
var Version = "0.1.0-m1"
