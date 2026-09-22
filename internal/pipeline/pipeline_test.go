package pipeline_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

type harness struct {
	svc    *pipeline.Service
	outbox string
}

func newHarness(t *testing.T, cl classify.Classifier) harness {
	t.Helper()
	cfg := testutil.Config(t)
	if cl == nil {
		m, err := classify.NewMock(cfg)
		if err != nil {
			t.Fatal(err)
		}
		cl = m
	}
	q, err := ingest.NewDirQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outDir := t.TempDir()
	ob, err := router.NewOutbox(outDir, cfg.Actions.KBArticles)
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	svc := &pipeline.Service{
		Cfg: cfg, Classifier: cl, Store: store.NewMemory(), Router: router.New(cfg), Actions: ob, Queue: q,
		Now: func() time.Time { clock = clock.Add(time.Second); return clock },
	}
	return harness{svc: svc, outbox: outDir}
}

func (h harness) ingestAll(t *testing.T) map[string]string {
	t.Helper()
	ctx := context.Background()
	ids := map[string]string{}
	for _, f := range testutil.Fixtures(t) {
		tk, dup, err := h.svc.Ingest(ctx, f.Payload, "test")
		if err != nil || dup {
			t.Fatalf("%s: dup=%v err=%v", f.Name, dup, err)
		}
		ids[f.Name] = tk.ID
	}
	for {
		n, err := h.svc.RunOnce(ctx, 4)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	return ids
}

// The demo storyline, end to end: every fixture lands where the Rivergate
// routing rules say it should.
func TestFixturesEndToEnd(t *testing.T) {
	h := newHarness(t, nil)
	ids := h.ingestAll(t)
	want := map[string]struct {
		rule, queue string
		status      store.Status
		review      bool
		actions     int
	}{
		"01-webform-outage-503.json":          {"outage-or-critical", "incident", store.StatusRouted, false, 2},
		"02-email-data-loss-critical.json":    {"outage-or-critical", "incident", store.StatusRouted, false, 2},
		"03-chat-double-charge.json":          {"billing", "billing", store.StatusRouted, false, 0},
		"04-email-timeline-crash.json":        {"bug", "bug", store.StatusRouted, false, 1},
		"05-webform-recurring-tasks.json":     {"feature-request", "product", store.StatusRouted, false, 0},
		"06-chat-export-csv.json":             {"how-to", "self-serve", store.StatusRouted, false, 1},
		"07-email-guest-seats-ambiguous.json": {"human-review", "human-review", store.StatusPendingReview, true, 0},
		"08-webform-partnership.json":         {"human-review", "human-review", store.StatusPendingReview, true, 0},
		"09-chat-vague-asap.json":             {"human-review", "human-review", store.StatusPendingReview, true, 0},
		"10-email-late-notifications.json":    {"human-review", "human-review", store.StatusPendingReview, true, 0},
	}
	ctx := context.Background()
	for name, id := range ids {
		it, err := h.svc.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		w := want[name]
		if it.Decision == nil || it.Decision.Rule != w.rule || it.Queue != w.queue || it.Status != w.status || it.NeedsReview != w.review || len(it.ExecutedActions) != w.actions {
			t.Errorf("%s: rule=%v queue=%s status=%s review=%v actions=%d; want %+v", name, it.Decision, it.Queue, it.Status, it.NeedsReview, len(it.ExecutedActions), w)
		}
		// Lifecycle is fully recorded.
		types := eventTypes(it)
		if !strings.HasPrefix(types, "received,classified,routed") {
			t.Errorf("%s: events = %s", name, types)
		}
		// Every automatic action is logged with the reasoning behind it.
		for _, e := range it.Events {
			if e.Type == "action" {
				if e.Data["reason"] == "" || e.Data["confidence"] == nil || e.Data["rationale"] == "" {
					t.Errorf("%s: action event missing reasoning: %+v", name, e.Data)
				}
			}
		}
	}
	entries, _ := router.ReadOutbox(h.outbox)
	if len(entries) != 6 {
		t.Errorf("outbox has %d entries, want 6 (2 slack, 3 tickets, 1 kb)", len(entries))
	}
	st, _ := h.svc.Stats(ctx)
	if st.Total != 10 || st.Automatic != 6 || st.NeedsReview != 4 {
		t.Errorf("stats = %+v", st)
	}
}

func eventTypes(it store.Item) string {
	var xs []string
	for _, e := range it.Events {
		xs = append(xs, e.Type)
	}
	return strings.Join(xs, ",")
}

func TestIngestIsIdempotent(t *testing.T) {
	h := newHarness(t, nil)
	f := testutil.Fixtures(t)[0]
	ctx := context.Background()
	a, dup, err := h.svc.Ingest(ctx, f.Payload, "cli")
	if err != nil || dup {
		t.Fatal(dup, err)
	}
	b, dup, err := h.svc.Ingest(ctx, f.Payload, "webhook")
	if err != nil || !dup || a.ID != b.ID {
		t.Fatalf("second ingest should be a duplicate: %v %v", dup, err)
	}
	if n, _ := h.svc.RunOnce(ctx, 10); n != 1 {
		t.Errorf("processed %d, want 1 (duplicate must not be re-queued)", n)
	}
	// Redelivery of an already-processed message is a no-op (no double page).
	it, _ := h.svc.Get(ctx, a.ID)
	if _, err := h.svc.Process(ctx, it.Ticket, "worker"); err != nil {
		t.Fatal(err)
	}
	again, _ := h.svc.Get(ctx, a.ID)
	if len(again.Events) != len(it.Events) {
		t.Error("redelivered message changed the item")
	}
}

type failingClassifier struct{}

func (failingClassifier) Classify(context.Context, ingest.Ticket) (classify.Classification, error) {
	return classify.Classification{}, errors.New("model timeout")
}
func (failingClassifier) Name() string { return "failing" }

type liarClassifier struct{}

func (liarClassifier) Classify(context.Context, ingest.Ticket) (classify.Classification, error) {
	return classify.Classification{Category: "refund", Priority: "high", Confidence: 0.99, Summary: "x"}, nil
}
func (liarClassifier) Name() string { return "liar" }

func TestClassifierFailureDefersToHuman(t *testing.T) {
	for _, cl := range []classify.Classifier{failingClassifier{}, liarClassifier{}} {
		t.Run(cl.Name(), func(t *testing.T) {
			h := newHarness(t, cl)
			ctx := context.Background()
			f := testutil.Fixtures(t)[0] // the outage ticket
			tk, _, err := h.svc.Ingest(ctx, f.Payload, "test")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.svc.RunOnce(ctx, 1); err != nil {
				t.Fatal(err)
			}
			it, _ := h.svc.Get(ctx, tk.ID)
			if !it.NeedsReview || it.Queue != "human-review" || it.Classification.Confidence != 0 {
				t.Errorf("failure must route to a human: %+v", it.Decision)
			}
			if !strings.Contains(eventTypes(it), "error") {
				t.Error("failure should be recorded in the audit trail")
			}
		})
	}
}

func TestApproveAndOverride(t *testing.T) {
	h := newHarness(t, nil)
	ids := h.ingestAll(t)
	ctx := context.Background()

	// Approve the low-confidence bug (late notifications): now follows the bug rule.
	id := ids["10-email-late-notifications.json"]
	it, err := h.svc.Approve(ctx, id, "dashboard:sam", "confirmed with customer")
	if err != nil {
		t.Fatal(err)
	}
	if it.Status != store.StatusReviewed || it.Queue != "bug" || it.NeedsReview || it.Review.Outcome != "approved" {
		t.Errorf("approve: status=%s queue=%s review=%v", it.Status, it.Queue, it.NeedsReview)
	}
	if len(it.ExecutedActions) != 1 {
		t.Errorf("approved bug should open a backlog ticket: %v", it.ExecutedActions)
	}

	// Override the ambiguous guest-seat ticket to how_to.
	id = ids["07-email-guest-seats-ambiguous.json"]
	it, err = h.svc.Override(ctx, id, "cli:sam", "how_to", "", "mostly a permissions question")
	if err != nil {
		t.Fatal(err)
	}
	if it.Queue != "self-serve" || it.Classification.Category != "how_to" || it.Review.Original.Category != "billing" {
		t.Errorf("override: queue=%s class=%+v", it.Queue, it.Classification)
	}

	// Invalid override label is rejected, nothing changes.
	if _, err := h.svc.Override(ctx, id, "cli:sam", "refunds", "", ""); !errors.Is(err, pipeline.ErrInvalidLabel) {
		t.Errorf("err = %v, want ErrInvalidLabel", err)
	}
	if _, err := h.svc.Approve(ctx, id, " ", ""); err == nil {
		t.Error("reviewer is required")
	}

	// Approving an already-paged outage must not page twice.
	id = ids["01-webform-outage-503.json"]
	before, _ := router.ReadOutbox(h.outbox)
	it, err = h.svc.Approve(ctx, id, "dashboard:sam", "")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := router.ReadOutbox(h.outbox)
	if len(after) != len(before) {
		t.Errorf("approval re-ran actions: %d -> %d outbox entries", len(before), len(after))
	}
	if !strings.Contains(eventTypes(it), "review") {
		t.Error("review not in audit trail")
	}
}

func TestPreviewHasNoSideEffects(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	tk, err := ingest.Normalize(testutil.Fixtures(t)[0].Payload, "preview", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	c, d, err := h.svc.Preview(ctx, tk)
	if err != nil || c.Category != "outage" || d.Rule != "outage-or-critical" {
		t.Fatalf("preview: %+v %+v %v", c, d, err)
	}
	if items, _ := h.svc.List(ctx, store.Filter{}); len(items) != 0 {
		t.Error("preview stored an item")
	}
	if entries, _ := router.ReadOutbox(h.outbox); len(entries) != 0 {
		t.Error("preview ran actions")
	}
}

func TestRouteRequiresClassification(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	tk, _, _ := h.svc.Ingest(ctx, testutil.Fixtures(t)[0].Payload, "test")
	if _, err := h.svc.Route(ctx, tk.ID, "cli"); !errors.Is(err, pipeline.ErrNotClassified) {
		t.Errorf("err = %v", err)
	}
}
