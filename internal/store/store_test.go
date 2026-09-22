package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

// conformance runs the same behavioural checks against any Store. The phase 5
// DynamoDB store should be added here too.
func conformance(t *testing.T, s store.Store) {
	ctx := context.Background()
	t0 := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)

	if _, err := s.Get(ctx, "RG-MISSING"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing item: err = %v", err)
	}
	a := store.Item{ID: "RG-A", Ticket: ingest.Ticket{ID: "RG-A", ReceivedAt: t0.Add(time.Minute)}, Status: store.StatusReceived}
	a.AddEvent(t0, "received", "test", "hello", map[string]any{"k": "v"})
	b := store.Item{ID: "RG-B", Ticket: ingest.Ticket{ID: "RG-B", ReceivedAt: t0}, Status: store.StatusPendingReview, Queue: "human-review", NeedsReview: true,
		Classification: &classify.Classification{Category: "other", Confidence: 0.3}}
	for _, it := range []store.Item{a, b} {
		if err := s.Put(ctx, it); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Get(ctx, "RG-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || got.Events[0].Data["k"] != "v" {
		t.Errorf("events not round-tripped: %+v", got.Events)
	}
	// Mutating a returned copy must not change the stored item.
	got.Status = store.StatusFailed
	if again, _ := s.Get(ctx, "RG-A"); again.Status != store.StatusReceived {
		t.Error("store returned a shared reference")
	}

	all, _ := s.List(ctx, store.Filter{})
	if len(all) != 2 || all[0].ID != "RG-B" {
		t.Errorf("list should be ordered by received time: %v", ids(all))
	}
	yes := true
	rq, _ := s.List(ctx, store.Filter{NeedsReview: &yes})
	if len(rq) != 1 || rq[0].ID != "RG-B" || rq[0].Classification.Confidence != 0.3 {
		t.Errorf("review filter: %v", ids(rq))
	}
	byQueue, _ := s.List(ctx, store.Filter{Queue: "human-review", Status: store.StatusPendingReview})
	if len(byQueue) != 1 {
		t.Errorf("queue+status filter: %v", ids(byQueue))
	}
}

func ids(items []store.Item) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.ID)
	}
	return out
}

func TestMemoryStore(t *testing.T) { conformance(t, store.NewMemory()) }

func TestFileStore(t *testing.T) {
	s, err := store.NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, s)
}

func TestFileStoreRejectsPathTraversal(t *testing.T) {
	s, _ := store.NewFile(t.TempDir())
	if _, err := s.Get(context.Background(), "../../etc/passwd"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v", err)
	}
	if err := s.Put(context.Background(), store.Item{ID: "../x"}); err == nil {
		t.Error("put with traversal id should fail")
	}
}
