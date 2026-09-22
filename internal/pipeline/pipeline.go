// Package pipeline is the worker lifecycle shared by every front end (CLI,
// worker, dashboard, MCP): normalize -> classify -> write state -> route ->
// act. Front ends differ only in who the "actor" is in the audit trail.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

// Service wires the pipeline stages together.
type Service struct {
	Cfg        *config.Config
	Classifier classify.Classifier
	Store      store.Store
	Router     *router.Router
	Actions    router.ActionSink
	Queue      ingest.Queue
	Now        func() time.Time
	Log        *slog.Logger

	mu sync.Mutex // serializes read-modify-write of items within one process
}

// ErrNotClassified is returned when routing/approving an item that has no
// classification yet.
var ErrNotClassified = errors.New("item has not been classified yet")

// ErrInvalidLabel is returned when an override uses a label outside the taxonomy.
var ErrInvalidLabel = errors.New("invalid label")

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.New(slog.DiscardHandler)
}

// Ingest normalizes a raw payload, records it as received and enqueues it.
// Re-ingesting an identical ticket is a no-op (duplicate=true).
// It satisfies ingest.Sink so the drop zone and webhook can call it directly.
func (s *Service) Ingest(ctx context.Context, payload []byte, source string) (ingest.Ticket, bool, error) {
	t, err := ingest.Normalize(payload, source, s.now())
	if err != nil {
		return ingest.Ticket{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.Store.Get(ctx, t.ID); err == nil {
		return t, true, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return t, false, err
	}
	it := newItem(t, s.now(), source)
	if err := s.Store.Put(ctx, it); err != nil {
		return t, false, err
	}
	if s.Queue == nil {
		return t, false, errors.New("no queue configured")
	}
	if err := s.Queue.Send(ctx, t); err != nil {
		return t, false, fmt.Errorf("enqueue: %w", err)
	}
	s.log().Info("ingested", "id", t.ID, "channel", t.Channel, "source", source)
	return t, false, nil
}

func newItem(t ingest.Ticket, now time.Time, actor string) store.Item {
	it := store.Item{ID: t.ID, Ticket: t, Status: store.StatusReceived, CreatedAt: now}
	it.AddEvent(now, "received", actor, fmt.Sprintf("received via %s (%s)", t.Channel, t.Source), nil)
	return it
}

// RunOnce drains up to max messages from the queue: the worker's inner loop
// (and the body of the SQS-triggered Lambda in phase 5).
func (s *Service) RunOnce(ctx context.Context, max int) (processed int, err error) {
	msgs, err := s.Queue.Receive(ctx, max)
	if err != nil {
		return 0, err
	}
	var errs []error
	for _, m := range msgs {
		if _, perr := s.Process(ctx, m.Ticket, "worker"); perr != nil {
			s.log().Error("process failed", "id", m.Ticket.ID, "err", perr)
			errs = append(errs, perr)
			if nerr := s.Queue.Nack(ctx, m, perr); nerr != nil {
				errs = append(errs, nerr)
			}
			continue
		}
		if aerr := s.Queue.Ack(ctx, m); aerr != nil {
			errs = append(errs, aerr)
		}
		processed++
	}
	return processed, errors.Join(errs...)
}

// Process classifies and routes one ticket. Duplicate deliveries of an item
// that is already past "received" are ignored (at-least-once queue safety).
func (s *Service) Process(ctx context.Context, t ingest.Ticket, actor string) (store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, err := s.Store.Get(ctx, t.ID)
	if errors.Is(err, store.ErrNotFound) {
		it = newItem(t, s.now(), actor) // enqueued by another producer
	} else if err != nil {
		return it, err
	} else if it.Status != store.StatusReceived {
		return it, nil
	}
	s.classifyInto(ctx, &it, actor)
	if err := s.Store.Put(ctx, it); err != nil {
		return it, err
	}
	return s.routeLocked(ctx, it, actor)
}

// classifyInto runs the classifier and records the result. A classifier
// failure is recorded as the safe fallback (other @ 0.0 -> human review).
func (s *Service) classifyInto(ctx context.Context, it *store.Item, actor string) {
	c, err := s.Classifier.Classify(ctx, it.Ticket)
	if err == nil {
		err = s.validate(c)
	}
	if err != nil {
		it.AddEvent(s.now(), "error", actor, "classification failed; falling back to human review", map[string]any{"error": err.Error()})
		c = classify.Fallback(err, s.Classifier.Name(), s.now())
	}
	it.Classification = &c
	it.Status = store.StatusClassified
	it.AddEvent(s.now(), "classified", actor,
		fmt.Sprintf("%s / %s @ %.2f by %s", c.Category, c.Priority, c.Confidence, c.Classifier),
		classificationData(c))
}

func (s *Service) validate(c classify.Classification) error {
	tx := classify.TaxonomyFrom(s.Cfg)
	ok := func(xs []string, v string) bool {
		for _, x := range xs {
			if x == v {
				return true
			}
		}
		return false
	}
	if !ok(tx.Categories, c.Category) || !ok(tx.Priorities, c.Priority) || c.Confidence < 0 || c.Confidence > 1 {
		return fmt.Errorf("%w: %s/%s@%.2f", classify.ErrBadOutput, c.Category, c.Priority, c.Confidence)
	}
	return nil
}

func classificationData(c classify.Classification) map[string]any {
	return map[string]any{
		"category": c.Category, "priority": c.Priority, "confidence": c.Confidence,
		"summary": c.Summary, "rationale": c.Rationale, "classifier": c.Classifier,
		"human_verified": c.HumanVerified,
	}
}

// Reclassify re-runs the classifier on a stored item and re-routes it.
// Actions already executed for the item are not repeated.
func (s *Service) Reclassify(ctx context.Context, id, actor string) (store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, err := s.Store.Get(ctx, id)
	if err != nil {
		return it, err
	}
	s.classifyInto(ctx, &it, actor)
	if err := s.Store.Put(ctx, it); err != nil {
		return it, err
	}
	return s.routeLocked(ctx, it, actor)
}

// Preview classifies and routes a ticket without writing anything or taking
// any action (CLI `classify --file`, MCP-safe dry run).
func (s *Service) Preview(ctx context.Context, t ingest.Ticket) (classify.Classification, router.Decision, error) {
	c, err := s.Classifier.Classify(ctx, t)
	if err == nil {
		err = s.validate(c)
	}
	if err != nil {
		c = classify.Fallback(err, s.Classifier.Name(), s.now())
	}
	return c, s.Router.Route(c), nil
}

// Route (re-)applies the routing rules to a classified item.
func (s *Service) Route(ctx context.Context, id, actor string) (store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	it, err := s.Store.Get(ctx, id)
	if err != nil {
		return it, err
	}
	return s.routeLocked(ctx, it, actor)
}

func (s *Service) routeLocked(ctx context.Context, it store.Item, actor string) (store.Item, error) {
	if it.Classification == nil {
		return it, fmt.Errorf("%w: %s", ErrNotClassified, it.ID)
	}
	c := *it.Classification
	d := s.Router.Route(c)
	it.Decision = &d
	it.Queue = d.Queue
	it.NeedsReview = d.NeedsReview
	switch {
	case c.HumanVerified:
		it.Status = store.StatusReviewed
	case d.Automatic:
		it.Status = store.StatusRouted
	default:
		it.Status = store.StatusPendingReview
	}
	data := classificationData(c)
	data["rule"], data["queue"], data["automatic"], data["needs_review"] = d.Rule, d.Queue, d.Automatic, d.NeedsReview
	it.AddEvent(s.now(), "routed", actor, d.Reason, data)
	s.log().Info("routed", "id", it.ID, "rule", d.Rule, "queue", d.Queue, "automatic", d.Automatic, "needs_review", d.NeedsReview)

	var errs []error
	for _, a := range d.Actions {
		key := strings.Join([]string{d.Rule, a.Type, a.Target}, "|")
		if contains(it.ExecutedActions, key) {
			it.AddEvent(s.now(), "action", actor, fmt.Sprintf("skipped %s %s: already done for this item", a.Type, a.Target), map[string]any{"key": key})
			continue
		}
		req := router.ActionRequest{
			Action: a, ItemID: it.ID, Subject: it.Ticket.Subject, Summary: c.Summary,
			Category: c.Category, Priority: c.Priority, Rule: d.Rule, Reason: d.Reason,
			Text: it.Ticket.Subject + "\n" + it.Ticket.Body,
		}
		res, err := s.Actions.Execute(ctx, req)
		if err != nil {
			it.AddEvent(s.now(), "error", actor, fmt.Sprintf("action %s %s failed", a.Type, a.Target), map[string]any{"error": err.Error(), "key": key})
			errs = append(errs, err)
			continue
		}
		it.ExecutedActions = append(it.ExecutedActions, key)
		// Every automatic action is logged with the reasoning that triggered it.
		ad := classificationData(c)
		ad["action"], ad["target"], ad["ref"], ad["rule"], ad["reason"] = a.Type, a.Target, res.Ref, d.Rule, d.Reason
		it.AddEvent(s.now(), "action", actor, fmt.Sprintf("%s -> %s: %s", a.Type, orDash(a.Target), res.Detail), ad)
	}
	if err := s.Store.Put(ctx, it); err != nil {
		errs = append(errs, err)
	}
	return it, errors.Join(errs...)
}

// Approve records that a human accepted the model's classification, then
// routes it as human-verified (confidence gates no longer apply).
func (s *Service) Approve(ctx context.Context, id, reviewer, note string) (store.Item, error) {
	return s.review(ctx, id, reviewer, note, "", "")
}

// Override records a human correction to category and/or priority, then
// routes it as human-verified. Empty values keep the model's label.
func (s *Service) Override(ctx context.Context, id, reviewer, category, priority, note string) (store.Item, error) {
	if category == "" && priority == "" {
		return store.Item{}, fmt.Errorf("%w: override needs a category or a priority", ErrInvalidLabel)
	}
	return s.review(ctx, id, reviewer, note, category, priority)
}

func (s *Service) review(ctx context.Context, id, reviewer, note, category, priority string) (store.Item, error) {
	if strings.TrimSpace(reviewer) == "" {
		return store.Item{}, errors.New("reviewer is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	it, err := s.Store.Get(ctx, id)
	if err != nil {
		return it, err
	}
	if it.Classification == nil {
		return it, fmt.Errorf("%w: %s", ErrNotClassified, id)
	}
	orig := *it.Classification
	c := orig
	outcome := "approved"
	if category != "" || priority != "" {
		outcome = "overridden"
		if category != "" {
			c.Category = category
		}
		if priority != "" {
			c.Priority = priority
		}
		if err := s.validate(c); err != nil {
			return it, fmt.Errorf("%w: %v", ErrInvalidLabel, err)
		}
		c.Rationale = fmt.Sprintf("overridden by %s (model said %s/%s @ %.2f)", reviewer, orig.Category, orig.Priority, orig.Confidence)
	}
	c.HumanVerified = true
	it.Classification = &c
	rv := &store.Review{Reviewer: reviewer, Outcome: outcome, Note: note, At: s.now()}
	if outcome == "overridden" {
		rv.Original = &orig
	}
	it.Review = rv
	detail := fmt.Sprintf("%s by %s: %s/%s", outcome, reviewer, c.Category, c.Priority)
	if note != "" {
		detail += " — " + note
	}
	it.AddEvent(s.now(), "review", reviewer, detail, map[string]any{
		"outcome": outcome, "category": c.Category, "priority": c.Priority,
		"model_category": orig.Category, "model_priority": orig.Priority, "model_confidence": orig.Confidence,
	})
	return s.routeLocked(ctx, it, reviewer)
}

// Get returns one item.
func (s *Service) Get(ctx context.Context, id string) (store.Item, error) {
	return s.Store.Get(ctx, id)
}

// List returns items matching a filter.
func (s *Service) List(ctx context.Context, f store.Filter) ([]store.Item, error) {
	return s.Store.List(ctx, f)
}

// ReviewQueue returns items waiting on a human.
func (s *Service) ReviewQueue(ctx context.Context) ([]store.Item, error) {
	yes := true
	return s.Store.List(ctx, store.Filter{NeedsReview: &yes})
}

// Stats summarizes the pipeline for `status` and the dashboard.
type Stats struct {
	Total       int            `json:"total"`
	ByStatus    map[string]int `json:"by_status"`
	ByQueue     map[string]int `json:"by_queue"`
	NeedsReview int            `json:"needs_review"`
	Automatic   int            `json:"automatic"`
}

// Stats counts items by status and queue.
func (s *Service) Stats(ctx context.Context) (Stats, error) {
	items, err := s.Store.List(ctx, store.Filter{})
	if err != nil {
		return Stats{}, err
	}
	st := Stats{ByStatus: map[string]int{}, ByQueue: map[string]int{}}
	for _, it := range items {
		st.Total++
		st.ByStatus[string(it.Status)]++
		if it.Queue != "" {
			st.ByQueue[it.Queue]++
		}
		if it.NeedsReview {
			st.NeedsReview++
		}
		if it.Decision != nil && it.Decision.Automatic {
			st.Automatic++
		}
	}
	return st, nil
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
