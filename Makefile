.PHONY: proto build build-linux-arm64 test run-coord run-agent lint

proto: ## regenerate Go code from contracts/proto (requires buf: https://buf.build)
	cd contracts && buf generate && buf lint

build: ## build both binaries into ./bin
	mkdir -p bin
	cd coordinator && go build -o ../bin/coordinator ./cmd/coordinator
	cd agent && go build -o ../bin/agent ./cmd/agent
	cd gateway && go build -o ../bin/gateway ./cmd/gateway

build-linux-arm64: ## cross-compile for linux/arm64 providers (e.g. Raspberry Pi 5)
	mkdir -p bin
	cd agent && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/agent-linux-arm64 ./cmd/agent
	cd coordinator && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/coordinator-linux-arm64 ./cmd/coordinator

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
