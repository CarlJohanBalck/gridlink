// Command gateway is the OpenAI-compatible inference front door. It routes
// /v1/* requests to READY replicas on provider nodes, streams SSE through
// untouched, and reports token usage to the coordinator.
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

	r := router.New(coordAddr, token, logger)
	defer r.Close()

	u := usage.NewReporter(coordAddr, token, logger)
	// Closed before the router so queued usage is flushed while the
	// coordinator connection is still up.
	defer u.Close()

	keys := proxy.KeysFromEnv("GRIDLINK_API_KEYS")
	if len(keys) == 0 {
		logger.Warn("GRIDLINK_API_KEYS is empty: the gateway will accept unauthenticated requests")
	}

	if err := proxy.Serve(ctx, proxy.Config{
		Addr:    httpAddr,
		Router:  r,
		Dialer:  d,
		Usage:   u,
		APIKeys: keys, // "key1,key2" Phase 2
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
