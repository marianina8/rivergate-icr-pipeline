package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
)

// LogSink is the ActionSink used in AWS until real Slack / ticketing
// integrations are wired in: every action is written as one structured JSON
// line (CloudWatch Logs in Lambda), and the item's audit trail records it
// too. Ticket keys are derived from the item ID so they are stable across
// Lambda invocations without a counter.
type LogSink struct {
	W        io.Writer
	Articles []config.KBArticle
	Now      func() time.Time
	mu       sync.Mutex
}

// Execute logs the action and returns a deterministic reference.
func (l *LogSink) Execute(_ context.Context, req ActionRequest) (ActionResult, error) {
	var res ActionResult
	switch req.Action.Type {
	case "notify":
		res = ActionResult{Ref: "notify:" + req.Action.Target + ":" + req.ItemID,
			Detail: fmt.Sprintf("[%s/%s] %s — %s (rule %s)", strings.ToUpper(req.Category), strings.ToUpper(req.Priority), req.ItemID, req.Summary, req.Rule)}
	case "create_ticket":
		res = ActionResult{Ref: fmt.Sprintf("%s-%s", req.Action.Target, strings.TrimPrefix(req.ItemID, "RG-")),
			Detail: fmt.Sprintf("[%s] %s", req.Action.Priority, req.Subject)}
	case "suggest_kb":
		o := Outbox{Articles: l.Articles}
		if a, ok := o.matchArticle(req.Text); ok {
			res = ActionResult{Ref: a.URL, Detail: fmt.Sprintf("Suggested: %q %s", a.Title, a.URL)}
		} else {
			res = ActionResult{Ref: "none", Detail: "no matching KB article"}
		}
	default:
		return ActionResult{}, fmt.Errorf("unknown action type %q", req.Action.Type)
	}
	now := time.Now
	if l.Now != nil {
		now = l.Now
	}
	line, err := json.Marshal(map[string]any{
		"msg": "icr_action", "at": now().UTC(), "action": req.Action.Type, "target": req.Action.Target,
		"ref": res.Ref, "detail": res.Detail, "item_id": req.ItemID, "rule": req.Rule, "reason": req.Reason,
		"category": req.Category, "priority": req.Priority,
	})
	if err != nil {
		return ActionResult{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, err = l.W.Write(append(line, '\n'))
	return res, err
}
