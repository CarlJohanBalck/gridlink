// Command gateway is the OpenAI-compatible inference front door. It routes
// /v1/* requests to READY vLLM replicas on provider nodes, streams SSE
// through untouched, and reports token usage to the coordinator.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gridlink/gateway/internal/dialer"
	"gridlink/gateway/internal/proxy"
	"gridlink/gateway/internal/router"
	"gridlink/gateway/internal/usage"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	httpAddr := envOr("GRIDLINK_GATEWAY_LISTEN", ":8080")
	coordAddr := envOr("GRIDLINK_COORDINATOR", "localhost:50051")
	token := os.Getenv("GRIDLINK_TOKEN")
	if token == "" {
		logger.Error("GRIDLINK_TOKEN is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Phase 2a: DirectDialer just net.Dials the replica addr (tailnet IP).
	// Phase 2b: swap for a reverse-tunnel dialer without touching router/proxy.
	d := dialer.NewDirect()

	// TODO(claude): router.New — gRPC client to coordinator GatewayService,
	// ResolveModel with a small TTL cache (2s) + round-robin across replicas.
	r := router.New(coordAddr, token, logger)

	// TODO(claude): usage.NewReporter — batches ReportUsage calls, drops on
	// overflow rather than blocking the request path.
	u := usage.NewReporter(coordAddr, token, logger)

	// TODO(claude): proxy.Serve — the HTTP server. See package doc for plan.
	if err := proxy.Serve(ctx, proxy.Config{
		Addr:    httpAddr,
		Router:  r,
		Dialer:  d,
		Usage:   u,
		APIKeys: proxy.KeysFromEnv("GRIDLINK_API_KEYS"), // "key1,key2" Phase 2
		Logger:  logger,
	}); err != nil && ctx.Err() == nil {
		logger.Error("gateway exited with error", "err", err)
		os.Exit(1)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
