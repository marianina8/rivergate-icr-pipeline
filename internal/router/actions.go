package router

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
)

// ActionRequest carries everything an action needs, including the reasoning
// that triggered it (so the downstream record is self-explanatory).
type ActionRequest struct {
	Action   config.Action `json:"action"`
	ItemID   string        `json:"item_id"`
	Subject  string        `json:"subject"`
	Summary  string        `json:"summary"`
	Category string        `json:"category"`
	Priority string        `json:"priority"`
	Rule     string        `json:"rule"`
	Reason   string        `json:"reason"`
	Text     string        `json:"-"` // subject + body, for KB matching only
}

// ActionResult is what an action produced (a ticket key, a message ref...).
type ActionResult struct {
	Ref    string `json:"ref"`
	Detail string `json:"detail"`
}

// ActionSink performs routing actions. The demo uses Outbox (JSONL files);
// a real deployment swaps in Slack / Jira / Zendesk clients behind this.
type ActionSink interface {
	Execute(ctx context.Context, req ActionRequest) (ActionResult, error)
}

// Outbox is the stub ActionSink: each action appends a JSON line to
// <dir>/<channel>.jsonl, which the dashboard and `icr outbox` display.
type Outbox struct {
	Dir      string
	Articles []config.KBArticle
	mu       sync.Mutex
	now      func() time.Time
}

// NewOutbox creates the outbox directory.
func NewOutbox(dir string, articles []config.KBArticle) (*Outbox, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Outbox{Dir: dir, Articles: articles, now: time.Now}, nil
}

// OutboxEntry is one line in an outbox file.
type OutboxEntry struct {
	At      time.Time     `json:"at"`
	Kind    string        `json:"kind"`
	Ref     string        `json:"ref"`
	Target  string        `json:"target,omitempty"`
	Request ActionRequest `json:"request"`
	Detail  string        `json:"detail"`
}

// Execute performs a stubbed action.
func (o *Outbox) Execute(_ context.Context, req ActionRequest) (ActionResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch req.Action.Type {
	case "notify":
		ref := fmt.Sprintf("msg-%s-%d", req.ItemID, o.now().UnixMilli())
		detail := fmt.Sprintf(":rotating_light: [%s/%s] %s — %s (rule %s)", strings.ToUpper(req.Category), strings.ToUpper(req.Priority), req.ItemID, req.Summary, req.Rule)
		return ActionResult{Ref: ref, Detail: detail}, o.append("slack", OutboxEntry{Kind: "slack", Ref: ref, Target: req.Action.Target, Request: req, Detail: detail})
	case "create_ticket":
		n, err := o.count("tickets")
		if err != nil {
			return ActionResult{}, err
		}
		key := fmt.Sprintf("%s-%d", req.Action.Target, 1001+n)
		detail := fmt.Sprintf("[%s] %s", req.Action.Priority, req.Subject)
		return ActionResult{Ref: key, Detail: detail}, o.append("tickets", OutboxEntry{Kind: "ticket", Ref: key, Target: req.Action.Target, Request: req, Detail: detail})
	case "suggest_kb":
		a, ok := o.matchArticle(req.Text)
		if !ok {
			return ActionResult{Ref: "none", Detail: "no matching KB article; self-serve reply without a link"}, o.append("kb", OutboxEntry{Kind: "kb", Ref: "none", Request: req, Detail: "no match"})
		}
		detail := fmt.Sprintf("Suggested: %q %s", a.Title, a.URL)
		return ActionResult{Ref: a.URL, Detail: detail}, o.append("kb", OutboxEntry{Kind: "kb", Ref: a.URL, Request: req, Detail: detail})
	default:
		return ActionResult{}, fmt.Errorf("unknown action type %q", req.Action.Type)
	}
}

func (o *Outbox) matchArticle(text string) (config.KBArticle, bool) {
	best, bestScore := config.KBArticle{}, 0
	for _, a := range o.Articles {
		score := 0
		for _, kw := range a.Keywords {
			pat := `(?i)\b` + regexp.QuoteMeta(strings.TrimSuffix(kw, "*"))
			if !strings.HasSuffix(kw, "*") {
				pat += `\b`
			}
			if regexp.MustCompile(pat).MatchString(text) {
				score++
			}
		}
		if score > bestScore {
			best, bestScore = a, score
		}
	}
	return best, bestScore > 0
}

func (o *Outbox) path(kind string) string { return filepath.Join(o.Dir, kind+".jsonl") }

func (o *Outbox) append(kind string, e OutboxEntry) error {
	e.At = o.now().UTC()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(o.path(kind), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

func (o *Outbox) count(kind string) (int, error) {
	b, err := os.ReadFile(o.path(kind))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strings.Count(string(b), "\n"), nil
}

// ReadOutbox returns all outbox entries across kinds, oldest first.
func ReadOutbox(dir string) ([]OutboxEntry, error) {
	var all []OutboxEntry
	for _, kind := range []string{"slack", "tickets", "kb"} {
		b, err := os.ReadFile(filepath.Join(dir, kind+".jsonl"))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var e OutboxEntry
			if err := json.Unmarshal([]byte(line), &e); err == nil {
				all = append(all, e)
			}
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].At.Before(all[j].At) })
	return all, nil
}
