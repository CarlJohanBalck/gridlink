package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gridlink/agent/internal/engine"
)

// runEngine is the `agent engine` subcommand: load one model and serve it over
// an OpenAI-compatible API on localhost.
//
// This runs as a SUBPROCESS of the agent, not inside it. One binary, two roles
// (the agent re-execs itself), so providers still install exactly one file —
// but a segfault in llama.cpp kills only the engine, not the agent, its
// coordinator stream, or any other deployment.
func runEngine(args []string) error {
	fs := flag.NewFlagSet("engine", flag.ContinueOnError)
	modelPath := fs.String("model", "", "path to a .gguf file (required)")
	modelName := fs.String("served-model-name", "", "name to advertise via /v1/models (required)")
	addr := fs.String("addr", "127.0.0.1:0", "listen address; port 0 picks a free one")
	ctxLen := fs.Uint("context-length", 0, "context window; 0 = model default")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *modelPath == "" || *modelName == "" {
		fs.Usage()
		return errors.New("engine: -model and -served-model-name are required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("role", "engine")

	if !engine.Supported() {
		return fmt.Errorf("engine: %w (needs macOS/arm64 with cgo)", engine.ErrUnsupported)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Listen BEFORE loading: loading an 8B takes seconds, and binding first
	// means the parent can watch for the port instead of racing the load.
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	// The parent reads this line to learn the chosen port. Keep the format
	// stable: deploy.parseEnginePort depends on it.
	fmt.Printf("GRIDLINK_ENGINE_LISTENING %s\n", ln.Addr().String())
	os.Stdout.Sync()

	if st, err := engine.GPUStats(); err == nil {
		logger.Info("gpu ready", "gpu", st.GPUName, "usable_vram_mb", st.UsableVRAMMb)
	} else {
		logger.Warn("could not read gpu stats", "err", err)
	}

	logger.Info("loading model", "path", *modelPath, "name", *modelName)
	start := time.Now()
	model, err := engine.Load(engine.Params{
		ModelPath:     *modelPath,
		ContextLength: uint32(*ctxLen),
	})
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("load model: %w", err)
	}
	defer model.Close()
	logger.Info("model loaded", "took", time.Since(start).Round(time.Millisecond))

	srv := &http.Server{
		Handler: engine.NewServer(model, *modelName, logger).Handler(),
		// No write timeout: a long generation is not a stuck connection, and
		// streaming responses legitimately stay open for minutes.
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve: %w", err)
	}
}
