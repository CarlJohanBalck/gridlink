.PHONY: proto build build-linux-arm64 engine engine-check release test run-coord run-agent lint

LLAMA_LIB := agent/internal/engine/third_party/llama.cpp/build/src/libllama.a

proto: ## regenerate Go code from contracts/proto (requires buf: https://buf.build)
	cd contracts && buf generate && buf lint

engine: ## fetch + build llama.cpp for the macOS Metal engine (build machine only)
	./scripts/fetch-llama.sh

# On macOS the agent links llama.cpp, so a missing build is a confusing pile of
# cgo linker errors. Fail early with the one command that fixes it.
engine-check:
	@if [ "$$(uname -s)" = "Darwin" ] && [ ! -f "$(LLAMA_LIB)" ]; then \
		echo "error: Metal engine not built. Run: make engine" >&2; exit 1; \
	fi

build: engine-check ## build all binaries into ./bin
	mkdir -p bin
	cd coordinator && go build -o ../bin/coordinator ./cmd/coordinator
	cd agent && go build -o ../bin/agent ./cmd/agent
	cd gateway && go build -o ../bin/gateway ./cmd/gateway

build-linux-arm64: ## cross-compile for linux/arm64 providers (e.g. Raspberry Pi 5)
	mkdir -p bin
	cd agent && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/agent-linux-arm64 ./cmd/agent
	cd coordinator && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/coordinator-linux-arm64 ./cmd/coordinator

# release builds the agent binaries providers download, plus the checksum list
# scripts/install.sh verifies against.
#
# Only the host platform's GPU build can be produced here: the macOS agent
# links llama.cpp via cgo, so it must be built ON an Apple Silicon Mac. The
# linux binaries are pure Go (CGO_ENABLED=0) and therefore have NO inference
# engine -- they can run container jobs only, not GPU inference.
release: engine-check
	rm -rf dist && mkdir -p dist
ifeq ($(shell uname -s),Darwin)
	cd agent && go build -o ../dist/gridlink-agent-darwin-arm64 ./cmd/agent
endif
	cd agent && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../dist/gridlink-agent-linux-arm64 ./cmd/agent
	cd agent && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../dist/gridlink-agent-linux-amd64 ./cmd/agent
	cd dist && shasum -a 256 gridlink-agent-* > SHA256SUMS
	@echo "--> dist/"
	@ls -1 dist

test:
	cd contracts && go test ./...
	cd coordinator && go test ./...
	cd agent && go test ./...
	cd gateway && go test ./...

run-coord: build
	GRIDLINK_TOKEN=$${GRIDLINK_TOKEN:-dev-token} ./bin/coordinator

run-agent: build
	GRIDLINK_TOKEN=$${GRIDLINK_TOKEN:-dev-token} \
	GRIDLINK_COORDINATOR=$${GRIDLINK_COORDINATOR:-localhost:50051} \
	./bin/agent

lint:
	cd contracts && buf lint
