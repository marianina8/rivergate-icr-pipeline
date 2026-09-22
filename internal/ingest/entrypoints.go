package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Sink accepts a raw payload from an entry point and enqueues it. The pipeline
// service provides this (it also records the "received" state).
type Sink func(ctx context.Context, payload []byte, source string) (Ticket, bool, error)

// DropZone is the local stand-in for the S3 drop-zone bucket and its
// new-object -> SQS event notification. Files placed in <root>/incoming are
// ingested, then moved to processed/ (or rejected/ if they don't parse).
type DropZone struct {
	Root string
	Sink Sink
}

// Scan ingests every *.json file currently in incoming/. It returns the IDs
// ingested and the names of rejected files.
func (d DropZone) Scan(ctx context.Context) (ingested []string, rejected []string, err error) {
	in := filepath.Join(d.Root, "incoming")
	for _, sub := range []string{"incoming", "processed", "rejected"} {
		if err := os.MkdirAll(filepath.Join(d.Root, sub), 0o755); err != nil {
			return nil, nil, err
		}
	}
	es, err := os.ReadDir(in)
	if err != nil {
		return nil, nil, err
	}
	var names []string
	for _, e := range es {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		p := filepath.Join(in, n)
		b, err := os.ReadFile(p)
		if err != nil {
			return ingested, rejected, err
		}
		t, _, err := d.Sink(ctx, b, "dropzone")
		dest := "processed"
		if err != nil {
			dest = "rejected"
			rejected = append(rejected, n)
		} else {
			ingested = append(ingested, t.ID)
		}
		if err := os.Rename(p, filepath.Join(d.Root, dest, n)); err != nil {
			return ingested, rejected, err
		}
	}
	return ingested, rejected, nil
}

// MaxWebhookBody caps webhook payload size (API Gateway's limit is larger;
// support tickets never need it).
const MaxWebhookBody = 256 << 10

// WebhookHandler is the local stand-in for API Gateway + the webhook Lambda:
// POST a channel payload as JSON, get 202 + the ticket ID back.
func WebhookHandler(sink Sink) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tickets", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxWebhookBody))
		if err != nil {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "payload too large"})
			return
		}
		t, dup, err := sink(r.Context(), body, "webhook")
		if err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, ErrInvalid) {
				code = http.StatusBadRequest
			}
			writeJSON(w, code, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"id": t.ID, "duplicate": dup, "status": "queued"})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
