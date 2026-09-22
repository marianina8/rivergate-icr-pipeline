// Command dashboard serves the human review queue: every item the router did
// not confidently handle, with the model's classification, confidence and
// reasoning, plus approve / override controls. It reads and writes the same
// local data dir as the CLI and worker.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
)

func main() {
	var (
		cfgPath  = flag.String("config", pipeline.Env("ICR_CONFIG", pipeline.DefaultConfig), "instance config")
		dataDir  = flag.String("data", pipeline.Env("ICR_DATA_DIR", pipeline.DefaultDataDir), "local data dir")
		addr     = flag.String("addr", "127.0.0.1:8080", "listen address (keep it on localhost: the demo has no auth)")
		reviewer = flag.String("reviewer", pipeline.Env("USER", "reviewer"), "default reviewer name in forms")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The dashboard never classifies new tickets, but approve/override
	// re-routes, so it opens the same pipeline (mock classifier by default).
	p, err := pipeline.OpenLocal(ctx, *cfgPath, *dataDir, "mock", log)
	if err != nil {
		log.Error("open pipeline", "err", err)
		os.Exit(1)
	}
	s, err := newServer(p.Service, pipeline.OutboxDir(*dataDir), *reviewer)
	if err != nil {
		log.Error("templates", "err", err)
		os.Exit(1)
	}
	srv := &http.Server{Addr: *addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("review dashboard", "url", "http://"+*addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("dashboard stopped", "err", err)
		os.Exit(1)
	}
}
