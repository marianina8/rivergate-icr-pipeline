package pipeline

import (
	"sort"

	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

var actionKinds = map[string]string{"notify": "slack", "create_ticket": "ticket", "suggest_kb": "kb"}

// ActionsFromItems rebuilds the action feed from the audit trails. This is
// how the CLI and dashboard show actions when running against DynamoDB,
// where there is no local outbox file.
func ActionsFromItems(items []store.Item) []router.OutboxEntry {
	var out []router.OutboxEntry
	str := func(m map[string]any, k string) string {
		s, _ := m[k].(string)
		return s
	}
	for _, it := range items {
		for _, ev := range it.Events {
			if ev.Type != "action" || ev.Data["action"] == nil {
				continue // skipped/duplicate actions carry no action data
			}
			act := str(ev.Data, "action")
			kind := actionKinds[act]
			if kind == "" {
				kind = act
			}
			out = append(out, router.OutboxEntry{
				At: ev.At, Kind: kind, Ref: str(ev.Data, "ref"), Target: str(ev.Data, "target"), Detail: str(ev.Data, "detail"),
				Request: router.ActionRequest{ItemID: it.ID, Subject: it.Ticket.Subject, Rule: str(ev.Data, "rule"), Reason: str(ev.Data, "reason"),
					Category: str(ev.Data, "category"), Priority: str(ev.Data, "priority")},
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}
