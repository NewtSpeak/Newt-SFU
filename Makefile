# Newt-SFU 本地构建辅助（与 CI/Release 命名约定保持一致）
.PHONY: test vet build build-all clean

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS = -s -w -X github.com/newtspeak/newt-sfu/internal/buildinfo.Version=$(VERSION)
CGO_ENABLED ?= 0
export CGO_ENABLED

test:
	go test ./... -count=1

vet:
	go vet ./...

# 本机平台
build:
	mkdir -p bin
	go build -trimpath -ldflags "$(LDFLAGS)" -o bin/newt-sfu ./cmd/newt-sfu
	@echo "built bin/newt-sfu (version=$(VERSION))"

# 与 Server 发布目录兼容的多平台工件
build-all:
	mkdir -p dist
	@for pair in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64; do \
		GOOS=$${pair%/*}; GOARCH=$${pair#*/}; \
		OUT=dist/newt-sfu-$(VERSION)-$$GOOS-$$GOARCH; \
		echo "→ $$OUT"; \
		GOOS=$$GOOS GOARCH=$$GOARCH go build -trimpath -ldflags "$(LDFLAGS)" -o "$$OUT" ./cmd/newt-sfu; \
	done
	cd dist && sha256sum newt-sfu-* > SHA256SUMS
	@ls -lh dist/

clean:
	rm -rf bin dist
