// Package update 实现远程热更：下载新二进制 → 校验 SHA-256 → 原子替换 → 进程退出由 systemd 拉起。
package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Options 升级参数（来自 Server UpdateBinary 指令）。
type Options struct {
	TargetVersion string
	DownloadURL   string
	SHA256Hex     string // 可空；非空则强制校验
	Force         bool
	// CurrentVersion 当前进程版本；未 Force 且相等时跳过。
	CurrentVersion string
	// RestartDelay Ack 返回后延迟退出，给控制通道发送 Ack 留时间。
	RestartDelay time.Duration
}

// Result 升级结果（给 CommandAck）。
type Result struct {
	Skipped bool
	Message string
}

var (
	mu       sync.Mutex
	inFlight bool
)

// Apply 同步下载并替换当前可执行文件；成功后异步延迟 os.Exit(0) 触发重启。
// 并发升级会返回错误。
func Apply(ctx context.Context, log *slog.Logger, opts Options) (Result, error) {
	if opts.DownloadURL == "" {
		return Result{}, fmt.Errorf("download_url 不能为空")
	}
	if opts.TargetVersion == "" {
		return Result{}, fmt.Errorf("target_version 不能为空")
	}
	if !opts.Force && opts.CurrentVersion != "" && opts.CurrentVersion == opts.TargetVersion {
		return Result{Skipped: true, Message: "already at target version"}, nil
	}
	if opts.RestartDelay <= 0 {
		opts.RestartDelay = 800 * time.Millisecond
	}

	mu.Lock()
	if inFlight {
		mu.Unlock()
		return Result{}, fmt.Errorf("另一升级任务进行中")
	}
	inFlight = true
	mu.Unlock()
	// 成功路径会 exit；失败路径需要释放 inFlight。
	success := false
	defer func() {
		if !success {
			mu.Lock()
			inFlight = false
			mu.Unlock()
		}
	}()

	exe, err := os.Executable()
	if err != nil {
		return Result{}, fmt.Errorf("定位可执行文件: %w", err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		return Result{}, fmt.Errorf("解析可执行路径: %w", err)
	}

	log.Info("update: downloading binary",
		"url", opts.DownloadURL,
		"target_version", opts.TargetVersion,
		"current_version", opts.CurrentVersion,
		"exe", exe,
	)

	tmpPath := exe + ".new"
	if err := downloadTo(ctx, opts.DownloadURL, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return Result{}, err
	}
	if opts.SHA256Hex != "" {
		if err := verifySHA256(tmpPath, opts.SHA256Hex); err != nil {
			_ = os.Remove(tmpPath)
			return Result{}, err
		}
	}
	if err := os.Chmod(tmpPath, 0o755); err != nil {
		_ = os.Remove(tmpPath)
		return Result{}, fmt.Errorf("chmod: %w", err)
	}

	// 备份当前二进制，失败时尽量回滚。
	bakPath := exe + ".bak"
	_ = os.Remove(bakPath)
	if err := os.Rename(exe, bakPath); err != nil {
		_ = os.Remove(tmpPath)
		return Result{}, fmt.Errorf("备份当前二进制: %w", err)
	}
	if err := os.Rename(tmpPath, exe); err != nil {
		_ = os.Rename(bakPath, exe)
		_ = os.Remove(tmpPath)
		return Result{}, fmt.Errorf("替换二进制: %w", err)
	}

	success = true
	log.Info("update: binary replaced, scheduling restart",
		"target_version", opts.TargetVersion,
		"restart_delay", opts.RestartDelay.String(),
	)
	go func() {
		time.Sleep(opts.RestartDelay)
		log.Info("update: exiting for restart")
		os.Exit(0)
	}()
	return Result{Message: "binary replaced; restarting"}, nil
}

func downloadTo(ctx context.Context, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("下载失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("下载 HTTP %d", resp.StatusCode)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	defer f.Close()
	// 限制最大约 256 MiB，防止异常大文件打满磁盘。
	const maxBytes = 256 << 20
	if _, err := io.Copy(f, io.LimitReader(resp.Body, maxBytes+1)); err != nil {
		return fmt.Errorf("写入临时文件: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > maxBytes {
		return fmt.Errorf("下载文件超过上限 %d 字节", maxBytes)
	}
	if info.Size() < 1024 {
		return fmt.Errorf("下载文件过小，可能不是合法二进制")
	}
	return nil
}

func verifySHA256(path, wantHex string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, strings.TrimSpace(wantHex)) {
		return fmt.Errorf("sha256 不匹配: got=%s want=%s", got, wantHex)
	}
	return nil
}
