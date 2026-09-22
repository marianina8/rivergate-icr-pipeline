package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Queue is the SQS-shaped contract the worker consumes. The local
// implementation (DirQueue) stands in for SQS; phase 5 adds an SQS-backed one.
type Queue interface {
	Send(ctx context.Context, t Ticket) error
	// Receive claims up to max messages. Claimed messages are invisible to
	// other receivers until Ack'd or Nack'd (SQS visibility timeout analogue).
	Receive(ctx context.Context, max int) ([]Message, error)
	Ack(ctx context.Context, m Message) error
	// Nack returns a message for retry, or dead-letters it after MaxAttempts.
	Nack(ctx context.Context, m Message, reason error) error
}

// Message is a queued ticket plus delivery metadata.
type Message struct {
	Ticket     Ticket    `json:"ticket"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	Attempts   int       `json:"attempts"`
	LastError  string    `json:"last_error,omitempty"`
	receipt    string
}

// MaxAttempts before a message moves to the dead-letter directory.
const MaxAttempts = 3

// DirQueue is a filesystem queue: pending/ -> inflight/ (claimed by atomic
// rename) -> deleted on Ack, or back to pending/ / dead/ on Nack. Safe for
// several local processes (CLI, worker, webhook) sharing one data dir.
type DirQueue struct {
	root string
	now  func() time.Time
}

// NewDirQueue creates (if needed) the queue directories under root.
func NewDirQueue(root string) (*DirQueue, error) {
	for _, d := range []string{"pending", "inflight", "dead"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	return &DirQueue{root: root, now: time.Now}, nil
}

func (q *DirQueue) dir(name string) string { return filepath.Join(q.root, name) }

func (q *DirQueue) Send(_ context.Context, t Ticket) error {
	return q.write("pending", Message{Ticket: t, EnqueuedAt: q.now().UTC()})
}

func (q *DirQueue) write(sub string, m Message) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	name := fmt.Sprintf("%020d-%s.json", m.EnqueuedAt.UnixNano(), m.Ticket.ID)
	tmp := filepath.Join(q.root, "."+name+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(q.dir(sub), name))
}

func (q *DirQueue) Receive(_ context.Context, max int) ([]Message, error) {
	entries, err := os.ReadDir(q.dir("pending"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // FIFO by enqueue time
	var out []Message
	for _, n := range names {
		if len(out) >= max {
			break
		}
		src, dst := filepath.Join(q.dir("pending"), n), filepath.Join(q.dir("inflight"), n)
		if err := os.Rename(src, dst); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // another receiver claimed it first
			}
			return out, err
		}
		b, err := os.ReadFile(dst)
		if err != nil {
			return out, err
		}
		var m Message
		if err := json.Unmarshal(b, &m); err != nil {
			_ = os.Rename(dst, filepath.Join(q.dir("dead"), n))
			continue
		}
		m.receipt = n
		out = append(out, m)
	}
	return out, nil
}

func (q *DirQueue) Ack(_ context.Context, m Message) error {
	return os.Remove(filepath.Join(q.dir("inflight"), m.receipt))
}

func (q *DirQueue) Nack(_ context.Context, m Message, reason error) error {
	_ = os.Remove(filepath.Join(q.dir("inflight"), m.receipt))
	m.Attempts++
	if reason != nil {
		m.LastError = reason.Error()
	}
	if m.Attempts >= MaxAttempts {
		return q.write("dead", m)
	}
	return q.write("pending", m)
}

// Depth reports message counts per state, for status output.
func (q *DirQueue) Depth() (pending, inflight, dead int) {
	count := func(d string) int {
		es, _ := os.ReadDir(q.dir(d))
		n := 0
		for _, e := range es {
			if strings.HasSuffix(e.Name(), ".json") {
				n++
			}
		}
		return n
	}
	return count("pending"), count("inflight"), count("dead")
}

// Recover moves in-flight messages left by a crashed worker back to pending
// (the equivalent of an SQS visibility timeout expiring).
func (q *DirQueue) Recover() (int, error) {
	es, err := os.ReadDir(q.dir("inflight"))
	if err != nil {
		return 0, err
	}
	n := 0
	for _, e := range es {
		if strings.HasSuffix(e.Name(), ".json") {
			if err := os.Rename(filepath.Join(q.dir("inflight"), e.Name()), filepath.Join(q.dir("pending"), e.Name())); err == nil {
				n++
			}
		}
	}
	return n, nil
}
