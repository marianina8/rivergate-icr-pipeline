package pipeline

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

// Defaults shared by every binary. Override with flags or env vars.
const (
	DefaultConfig  = "config/rivergate.yaml"
	DefaultDataDir = ".icr"
)

// Env returns the value of an env var or a fallback.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Local is a fully local pipeline: file store, directory queue, outbox
// action stubs. This is the phase 1-4 runtime; nothing here calls AWS unless
// the Bedrock classifier is explicitly selected.
type Local struct {
	*Service
	DirQueue *ingest.DirQueue
	DataDir  string
}

// Paths under the data dir.
func ItemsDir(data string) string    { return filepath.Join(data, "items") }
func QueueDir(data string) string    { return filepath.Join(data, "queue") }
func OutboxDir(data string) string   { return filepath.Join(data, "outbox") }
func DropZoneDir(data string) string { return filepath.Join(data, "dropzone") }

// OpenLocal wires a local pipeline from a config file and a data dir.
// classifier may be "" (use config), "mock" or "bedrock".
func OpenLocal(ctx context.Context, cfgPath, dataDir, classifier string, log *slog.Logger) (*Local, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	cl, err := classify.New(ctx, classifier, cfg)
	if err != nil {
		return nil, err
	}
	st, err := store.NewFile(ItemsDir(dataDir))
	if err != nil {
		return nil, err
	}
	q, err := ingest.NewDirQueue(QueueDir(dataDir))
	if err != nil {
		return nil, err
	}
	ob, err := router.NewOutbox(OutboxDir(dataDir), cfg.Actions.KBArticles)
	if err != nil {
		return nil, err
	}
	svc := &Service{Cfg: cfg, Classifier: cl, Store: st, Router: router.New(cfg), Actions: ob, Queue: q, Log: log}
	return &Local{Service: svc, DirQueue: q, DataDir: dataDir}, nil
}
