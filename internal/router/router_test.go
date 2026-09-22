package router_test

import (
	"context"
	"strings"
	"testing"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

func TestRoutingRules(t *testing.T) {
	r := router.New(testutil.Config(t))
	type want struct {
		rule, queue       string
		automatic, review bool
		actions           string
	}
	cases := []struct {
		name     string
		cat, pri string
		conf     float64
		want     want
	}{
		// outage OR critical: always notify + ENG ticket, regardless of confidence
		{"outage high conf", "outage", "high", 0.9, want{"outage-or-critical", "incident", true, false, "notify:#rivergate-oncall,create_ticket:ENG"}},
		{"outage low conf still pages", "outage", "medium", 0.2, want{"outage-or-critical", "incident", true, true, "notify:#rivergate-oncall,create_ticket:ENG"}},
		{"critical bug pages", "bug", "critical", 0.8, want{"outage-or-critical", "incident", true, false, "notify:#rivergate-oncall,create_ticket:ENG"}},
		{"critical billing pages", "billing", "critical", 0.5, want{"outage-or-critical", "incident", true, true, "notify:#rivergate-oncall,create_ticket:ENG"}},
		{"outage at exactly 0.6 not flagged", "outage", "high", 0.6, want{"outage-or-critical", "incident", true, false, "notify:#rivergate-oncall,create_ticket:ENG"}},

		// billing >= 0.75
		{"billing confident", "billing", "medium", 0.75, want{"billing", "billing", true, false, ""}},
		{"billing just under", "billing", "medium", 0.74, want{"human-review", "human-review", false, true, ""}},

		// bug >= 0.75 -> backlog ticket
		{"bug confident", "bug", "high", 0.9, want{"bug", "bug", true, false, "create_ticket:ENG-BACKLOG"}},
		{"bug medium conf", "bug", "medium", 0.65, want{"human-review", "human-review", false, true, ""}},

		// feature_request: no extra gate, but the global 0.6 floor still applies
		{"feature request", "feature_request", "low", 0.61, want{"feature-request", "product", true, false, ""}},
		{"feature request below floor", "feature_request", "low", 0.59, want{"human-review", "human-review", false, true, ""}},

		// how_to >= 0.6 -> self-serve + KB suggestion
		{"how-to at threshold", "how_to", "low", 0.6, want{"how-to", "self-serve", true, false, "suggest_kb:"}},
		{"how-to below", "how_to", "low", 0.59, want{"human-review", "human-review", false, true, ""}},

		// other -> always human
		{"other confident", "other", "low", 0.99, want{"human-review", "human-review", false, true, ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := r.Route(classify.Classification{Category: tc.cat, Priority: tc.pri, Confidence: tc.conf})
			var acts []string
			for _, a := range d.Actions {
				acts = append(acts, a.Type+":"+a.Target)
			}
			got := want{d.Rule, d.Queue, d.Automatic, d.NeedsReview, strings.Join(acts, ",")}
			if got != tc.want {
				t.Errorf("got  %+v\nwant %+v\nreason: %s", got, tc.want, d.Reason)
			}
			if d.Reason == "" {
				t.Error("every decision needs a reason for the audit trail")
			}
		})
	}
}

func TestHumanVerifiedRouting(t *testing.T) {
	r := router.New(testutil.Config(t))
	// A reviewer confirms a low-confidence bug: it now follows the bug rule.
	d := r.Route(classify.Classification{Category: "bug", Priority: "medium", Confidence: 0.3, HumanVerified: true})
	if d.Rule != "bug" || d.Queue != "bug" || d.Automatic || d.NeedsReview {
		t.Errorf("verified bug: %+v", d)
	}
	// A reviewer approves "other": handled by the reviewer, leaves the review queue.
	d = r.Route(classify.Classification{Category: "other", Priority: "low", Confidence: 0.3, HumanVerified: true})
	if d.Queue != "general-support" || d.NeedsReview || d.Automatic {
		t.Errorf("verified other: %+v", d)
	}
}

func TestOutboxActions(t *testing.T) {
	cfg := testutil.Config(t)
	dir := t.TempDir()
	ob, err := router.NewOutbox(dir, cfg.Actions.KBArticles)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	rule := cfg.Routing.Rules[0]
	req := router.ActionRequest{ItemID: "RG-1", Subject: "Down", Summary: "all down", Category: "outage", Priority: "critical", Rule: rule.Name, Reason: "because"}

	req.Action = rule.Actions[0]
	res, err := ob.Execute(ctx, req)
	if err != nil || !strings.Contains(res.Detail, "RG-1") {
		t.Fatalf("notify: %v %+v", err, res)
	}
	req.Action = rule.Actions[1]
	res1, _ := ob.Execute(ctx, req)
	res2, _ := ob.Execute(ctx, req)
	if res1.Ref != "ENG-1001" || res2.Ref != "ENG-1002" {
		t.Errorf("ticket keys %s %s", res1.Ref, res2.Ref)
	}

	req.Action.Type, req.Action.Target = "suggest_kb", ""
	req.Text = "How do I export our board to CSV?"
	res, _ = ob.Execute(ctx, req)
	if !strings.Contains(res.Ref, "export-csv") {
		t.Errorf("kb match = %+v", res)
	}
	req.Text = "Something unrelated"
	if res, _ = ob.Execute(ctx, req); res.Ref != "none" {
		t.Errorf("kb no-match = %+v", res)
	}

	entries, err := router.ReadOutbox(dir)
	if err != nil || len(entries) != 5 {
		t.Fatalf("outbox entries = %d, %v", len(entries), err)
	}
	for _, e := range entries {
		if e.Request.Reason != "because" {
			t.Error("every outbox entry must carry the triggering reason")
		}
	}
}
