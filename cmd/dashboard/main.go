// Command dashboard serves the web UI locally: submit a ticket and watch it
// get classified and routed, review the human queue, approve or override.
// By default it uses the local data dir (tickets are processed immediately,
// no worker needed); with -store dynamo it works against the deployed stack.
// The same UI is hosted on Lambda by cmd/lambda/dashboard.
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
	"github.com/marianina8/rivergate-icr-pipeline/internal/dashboard"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
)

func main() {
	var (
		cfgPath  = flag.String("config", pipeline.Env("ICR_CONFIG", pipeline.DefaultConfig), "instance config")
		dataDir  = flag.String("data", pipeline.Env("ICR_DATA_DIR", pipeline.DefaultDataDir), "local data dir")
		addr     = flag.String("addr", "127.0.0.1:8080", "listen address (keep it on localhost unless a password is set)")
		reviewer = flag.String("reviewer", pipeline.Env("USER", "reviewer"), "default reviewer name in forms")
		storeK   = flag.String("store", pipeline.Env("ICR_STORE", "file"), "file | dynamo")
		table    = flag.String("table", os.Getenv("ICR_ITEMS_TABLE"), "DynamoDB table (ItemsTableName output) for -store dynamo")
		queueURL = flag.String("queue-url", os.Getenv("ICR_QUEUE_URL"), "SQS queue (TicketQueueUrl output) so -store dynamo can accept submissions")
		profile  = flag.String("profile", os.Getenv("AWS_PROFILE"), "AWS CLI profile for -store dynamo")
		examples = flag.String("examples", "demo/tickets", "directory of example tickets for the submit page")
		basePath = flag.String("base-path", "", "serve under a path prefix, e.g. /demos/rivergate")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ex, err := dashboard.LoadExamples(*examples)
	if err != nil {
		log.Warn("no examples loaded", "err", err)
	}
	// Set ICR_DASHBOARD_PASSWORD to require a login (e.g. when sharing the screen or the port).
	opt := dashboard.Options{Reviewer: *reviewer, Examples: ex, Password: os.Getenv("ICR_DASHBOARD_PASSWORD"), BasePath: *basePath}
	switch *storeK {
	case "dynamo":
		app, err := awsapp.New(ctx, awsapp.Options{ConfigPath: *cfgPath, Table: *table, QueueURL: *queueURL, Classifier: "mock", Profile: *profile, ActionLog: os.Stderr, Log: log})
		if err != nil {
			log.Error("startup failed", "err", err)
			os.Exit(1)
		}
		opt.Svc, opt.Actions = app.Svc, dashboard.AuditActions(app.Svc)
	default:
		p, err := pipeline.OpenLocal(ctx, *cfgPath, *dataDir, "", log)
		if err != nil {
			log.Error("startup failed", "err", err)
			os.Exit(1)
		}
		opt.Svc, opt.Actions = p.Service, dashboard.FileActions(pipeline.OutboxDir(*dataDir))
		opt.ProcessInline = func(ctx context.Context) error { _, err := p.RunOnce(ctx, 10); return err }
	}
	s, err := dashboard.New(opt)
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	srv := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("review dashboard", "url", "http://"+*addr+*basePath+"/", "store", *storeK, "password_protected", opt.Password != "")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("dashboard stopped", "err", err)
		os.Exit(1)
	}
}
