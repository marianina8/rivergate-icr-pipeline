// Package store records every item's full lifecycle — received -> classified
// -> routed -> actioned (or pending review -> reviewed) — with timestamps and
// the reasoning attached at each step. In AWS this is DynamoDB (phase 5);
// locally it is an in-memory map (tests) or one JSON file per item (demos).
package store

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
)

// Status is an item's lifecycle state.
type Status string

const (
	StatusReceived      Status = "received"
	StatusClassified    Status = "classified"
	StatusRouted        Status = "routed"         // automatic routing done
	StatusPendingReview Status = "pending_review" // waiting on a human
	StatusReviewed      Status = "reviewed"       // a human approved/overrode it
	StatusFailed        Status = "failed"
)

// Item is one ticket and everything that has happened to it.
type Item struct {
	ID             string                   `json:"id"`
	Ticket         ingest.Ticket            `json:"ticket"`
	Status         Status                   `json:"status"`
	Classification *classify.Classification `json:"classification,omitempty"`
	Decision       *router.Decision         `json:"decision,omitempty"`
	Queue          string                   `json:"queue,omitempty"`
	NeedsReview    bool                     `json:"needs_review"`
	Review         *Review                  `json:"review,omitempty"`
	// ExecutedActions are idempotency keys (rule/type/target) of actions
	// already performed, so a re-route or approval never double-pages.
	ExecutedActions []string  `json:"executed_actions,omitempty"`
	Events          []Event   `json:"events"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// Review records a human decision.
type Review struct {
	Reviewer string    `json:"reviewer"`
	Outcome  string    `json:"outcome"` // approved | overridden
	Note     string    `json:"note,omitempty"`
	At       time.Time `json:"at"`
	// Original is the model's classification before an override.
	Original *classify.Classification `json:"original,omitempty"`
}

// Event is one audit-trail entry.
type Event struct {
	At     time.Time      `json:"at"`
	Type   string         `json:"type"`  // received | classified | routed | action | review | error
	Actor  string         `json:"actor"` // worker | cli | dashboard:<name> | mcp:<client>
	Detail string         `json:"detail"`
	Data   map[string]any `json:"data,omitempty"`
}

// ErrNotFound is returned for unknown item IDs.
var ErrNotFound = errors.New("item not found")

// Filter narrows List results. Zero values mean "any".
type Filter struct {
	Status      Status
	Queue       string
	NeedsReview *bool
}

// Store is the persistence contract. The DynamoDB implementation arrives in
// phase 5 and must pass the same conformance tests (store_test.go).
type Store interface {
	Get(ctx context.Context, id string) (Item, error)
	Put(ctx context.Context, it Item) error
	List(ctx context.Context, f Filter) ([]Item, error)
}

// AddEvent appends an audit event and bumps UpdatedAt.
func (it *Item) AddEvent(at time.Time, typ, actor, detail string, data map[string]any) {
	it.Events = append(it.Events, Event{At: at.UTC(), Type: typ, Actor: actor, Detail: detail, Data: data})
	it.UpdatedAt = at.UTC()
}

func (f Filter) match(it Item) bool {
	if f.Status != "" && it.Status != f.Status {
		return false
	}
	if f.Queue != "" && it.Queue != f.Queue {
		return false
	}
	if f.NeedsReview != nil && it.NeedsReview != *f.NeedsReview {
		return false
	}
	return true
}

func sortItems(items []Item) {
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].Ticket.ReceivedAt.Equal(items[j].Ticket.ReceivedAt) {
			return items[i].Ticket.ReceivedAt.Before(items[j].Ticket.ReceivedAt)
		}
		return items[i].ID < items[j].ID
	})
}
