// Package router is the rules engine that decides what happens to a
// classified ticket. Rules come from config (not hardcoded branching), are
// evaluated top to bottom, and the first match wins. Anything a rule does not
// confidently claim goes to the human review queue.
package router

import (
	"fmt"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
)

// Decision is the router's verdict for one item.
type Decision struct {
	Rule    string          `json:"rule"`
	Queue   string          `json:"queue"`
	Actions []config.Action `json:"actions,omitempty"`
	// Automatic is true when the item was routed without a human decision.
	Automatic bool `json:"automatic"`
	// NeedsReview puts the item in the human review queue. It can be true
	// alongside Automatic (outage paged immediately but the model was unsure).
	NeedsReview bool `json:"needs_review"`
	// Reason is the human-readable explanation logged with every decision.
	Reason string `json:"reason"`
}

// Router evaluates routing rules from config.
type Router struct {
	cfg config.Routing
}

// New builds a router from config.
func New(c *config.Config) *Router { return &Router{cfg: c.Routing} }

// Rules exposes the configured rules (read-only use: status, dashboard).
func (r *Router) Rules() []config.Rule { return r.cfg.Rules }

// Route decides where a classification goes. Pure function of its inputs:
// no side effects, so it is trivially testable and safe to "dry run".
func (r *Router) Route(c classify.Classification) Decision {
	conf := c.Confidence
	verified := c.HumanVerified
	for _, rule := range r.cfg.Rules {
		if !matches(rule, c) {
			continue
		}
		label := fmt.Sprintf("category=%s priority=%s confidence=%.2f", c.Category, c.Priority, conf)
		if verified {
			label = fmt.Sprintf("category=%s priority=%s (human-verified)", c.Category, c.Priority)
		}
		if !verified && !rule.BypassConfidenceFloor {
			need := max(rule.MinConfidence, r.cfg.ConfidenceFloor)
			if conf < need {
				// Matched by label but not confident enough: defer to a human.
				return r.review(fmt.Sprintf("%s matched rule %q but is below its %.2f confidence threshold", label, rule.Name, need))
			}
		}
		d := Decision{
			Rule:      rule.Name,
			Queue:     rule.Queue,
			Actions:   rule.Actions,
			Automatic: !verified,
			Reason:    fmt.Sprintf("%s matched rule %q: %s", label, rule.Name, rule.Description),
		}
		if !verified && rule.AlsoReviewBelow > 0 && conf < rule.AlsoReviewBelow {
			d.NeedsReview = true
			d.Reason += fmt.Sprintf(" Confidence below %.2f, so also queued for human review.", rule.AlsoReviewBelow)
		}
		return d
	}
	if verified {
		// A reviewer approved a classification no rule claims (e.g. "other").
		q := r.cfg.Default.ApprovedQueue
		if q == "" {
			q = r.cfg.Default.Queue
		}
		return Decision{Rule: r.cfg.Default.Name, Queue: q, Reason: fmt.Sprintf("category=%s priority=%s (human-verified) matched no rule; handled by reviewer", c.Category, c.Priority)}
	}
	return r.review(fmt.Sprintf("category=%s priority=%s confidence=%.2f matched no automatic rule", c.Category, c.Priority, conf))
}

func (r *Router) review(reason string) Decision {
	return Decision{Rule: r.cfg.Default.Name, Queue: r.cfg.Default.Queue, NeedsReview: true, Reason: reason + "; sent to human review."}
}

func matches(rule config.Rule, c classify.Classification) bool {
	for _, x := range rule.Categories {
		if x == c.Category {
			return true
		}
	}
	for _, x := range rule.Priorities {
		if x == c.Priority {
			return true
		}
	}
	return false
}
