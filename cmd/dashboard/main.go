// Command dashboard serves the human review queue: every item the router did
// not confidently handle, with the model's classification, confidence and
// reasoning, plus approve / override controls. By default it reads and writes
// the same local data dir as the CLI and worker; with -store dynamo it works
// against the deployed stack's DynamoDB table.
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

	"github.com/marianina8/rivergate-icr-pipeline/internal/awsapp"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
)

func main() {
	var (
		cfgPath  = flag.String("config", pipeline.Env("ICR_CONFIG", pipeline.DefaultConfig), "instance config")
		dataDir  = flag.String("data", pipeline.Env("ICR_DATA_DIR", pipeline.DefaultDataDir), "local data dir")
		addr     = flag.String("addr", "127.0.0.1:8080", "listen address (keep it on localhost: the demo has no auth)")
		reviewer = flag.String("reviewer", pipeline.Env("USER", "reviewer"), "default reviewer name in forms")
		storeK   = flag.String("store", pipeline.Env("ICR_STORE", "file"), "file | dynamo")
		table    = flag.String("table", os.Getenv("ICR_ITEMS_TABLE"), "DynamoDB table (ItemsTableName stack output) for -store dynamo")
		profile  = flag.String("profile", os.Getenv("AWS_PROFILE"), "AWS CLI profile for -store dynamo")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The dashboard never classifies new tickets; approve/override only
	// re-routes, so the mock classifier is enough in both modes.
	var s *server
	var err error
	switch *storeK {
	case "dynamo":
		var app *awsapp.App
		app, err = awsapp.New(ctx, awsapp.Options{ConfigPath: *cfgPath, Table: *table, Classifier: "mock", Profile: *profile, ActionLog: os.Stderr, Log: log})
		if err == nil {
			s, err = newServer(app.Svc, auditActions(app.Svc), *reviewer)
		}
	default:
		var p *pipeline.Local
		p, err = pipeline.OpenLocal(ctx, *cfgPath, *dataDir, "mock", log)
		if err == nil {
			s, err = newServer(p.Service, fileActions(pipeline.OutboxDir(*dataDir)), *reviewer)
		}
	}
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	srv := &http.Server{Addr: *addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("review dashboard", "url", "http://"+*addr, "store", *storeK)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("dashboard stopped", "err", err)
		os.Exit(1)
	}
}
