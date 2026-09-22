// Command worker is the pipeline's queue consumer. Locally it also runs the
// stand-ins for the AWS entry points:
//
//	drop zone  <data>/dropzone/incoming/*.json   (S3 bucket + event notification)
//	webhook    POST http://127.0.0.1:8081/tickets (API Gateway + webhook Lambda)
//	queue      <data>/queue/                      (SQS)
//
// In phase 5 the same Service.RunOnce logic becomes the SQS-triggered Lambda
// handler; see infra/template.yaml.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
)

func main() {
	var (
		cfgPath    = flag.String("config", pipeline.Env("ICR_CONFIG", pipeline.DefaultConfig), "instance config")
		dataDir    = flag.String("data", pipeline.Env("ICR_DATA_DIR", pipeline.DefaultDataDir), "local data dir")
		classifier = flag.String("classifier", pipeline.Env("ICR_CLASSIFIER", ""), "mock | bedrock (default from config)")
		webhook    = flag.String("webhook", "127.0.0.1:8081", "webhook receiver address (empty to disable)")
		interval   = flag.Duration("interval", time.Second, "poll interval for the queue and drop zone")
		batch      = flag.Int("batch", 10, "max messages per receive (SQS max is 10)")
		once       = flag.Bool("once", false, "drain the drop zone and queue once, then exit")
	)
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, log, *cfgPath, *dataDir, *classifier, *webhook, *interval, *batch, *once); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, cfgPath, dataDir, classifier, webhookAddr string, interval time.Duration, batch int, once bool) error {
	p, err := pipeline.OpenLocal(ctx, cfgPath, dataDir, classifier, log)
	if err != nil {
		return err
	}
	if n, err := p.DirQueue.Recover(); err != nil {
		return err
	} else if n > 0 {
		log.Warn("returned in-flight messages to the queue after an unclean stop", "count", n)
	}
	dz := ingest.DropZone{Root: pipeline.DropZoneDir(dataDir), Sink: p.Ingest}

	if once {
		if _, err := scan(ctx, log, dz); err != nil {
			return err
		}
		total := 0
		for {
			n, err := p.RunOnce(ctx, batch)
			total += n
			if err != nil {
				log.Error("batch had failures", "err", err)
			}
			if n == 0 {
				break
			}
		}
		log.Info("drained", "processed", total)
		return nil
	}

	if webhookAddr != "" {
		srv := &http.Server{Addr: webhookAddr, Handler: ingest.WebhookHandler(p.Ingest), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Info("webhook receiver listening", "addr", "http://"+webhookAddr+"/tickets")
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("webhook receiver failed", "err", err)
			}
		}()
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
	}
	log.Info("worker running", "drop_zone", dz.Root+"/incoming", "classifier", p.Classifier.Name(), "interval", interval.String())
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if _, err := scan(ctx, log, dz); err != nil {
			log.Error("drop zone scan failed", "err", err)
		}
		for {
			n, err := p.RunOnce(ctx, batch)
			if err != nil {
				log.Error("batch had failures", "err", err)
			}
			if n == 0 {
				break
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func scan(ctx context.Context, log *slog.Logger, dz ingest.DropZone) (int, error) {
	ids, rejected, err := dz.Scan(ctx)
	if len(ids) > 0 {
		log.Info("drop zone ingested", "count", len(ids), "ids", fmt.Sprint(ids))
	}
	if len(rejected) > 0 {
		log.Warn("drop zone rejected files (moved to rejected/)", "files", fmt.Sprint(rejected))
	}
	return len(ids), err
}
