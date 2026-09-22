// Package classify turns a normalized ticket into a structured classification:
// {category, priority, summary, confidence}. It is one bounded model call, not
// a chatbot. The model sits behind the Classifier interface so the pipeline
// runs fully offline with the Mock classifier and swaps in Bedrock in phase 5.
package classify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"text/template"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
)

// Classification is the structured model output, plus provenance.
type Classification struct {
	Category   string  `json:"category"`
	Priority   string  `json:"priority"`
	Summary    string  `json:"summary"`
	Confidence float64 `json:"confidence"`
	// Rationale is a one-line "why", kept for the audit trail so every
	// automatic action can be traced back to the reasoning that triggered it.
	Rationale    string    `json:"rationale,omitempty"`
	Classifier   string    `json:"classifier"`
	ClassifiedAt time.Time `json:"classified_at"`
	// HumanVerified is set when a reviewer approved or overrode the result.
	HumanVerified bool `json:"human_verified,omitempty"`
}

// Classifier is the seam between the pipeline and whatever model is used.
type Classifier interface {
	Classify(ctx context.Context, t ingest.Ticket) (Classification, error)
	Name() string
}

// Taxonomy is the set of allowed labels, from config.
type Taxonomy struct {
	Categories []string
	Priorities []string
}

// TaxonomyFrom builds a Taxonomy from config.
func TaxonomyFrom(c *config.Config) Taxonomy {
	return Taxonomy{Categories: c.CategoryNames(), Priorities: c.PriorityNames()}
}

func (tx Taxonomy) hasCategory(s string) bool { return in(tx.Categories, s) }
func (tx Taxonomy) hasPriority(s string) bool { return in(tx.Priorities, s) }

func in(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// ErrBadOutput means the model's response could not be trusted. The pipeline
// treats this as confidence 0 -> human review, never as a guess.
var ErrBadOutput = errors.New("classifier output invalid")

// ParseModelOutput extracts and strictly validates the JSON object the model
// returned. Unknown labels are rejected rather than coerced.
func ParseModelOutput(text string, tx Taxonomy) (Classification, error) {
	s := strings.TrimSpace(text)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	start, end := strings.Index(s, "{"), strings.LastIndex(s, "}")
	if start < 0 || end <= start {
		return Classification{}, fmt.Errorf("%w: no JSON object in response", ErrBadOutput)
	}
	var out struct {
		Category   string   `json:"category"`
		Priority   string   `json:"priority"`
		Summary    string   `json:"summary"`
		Confidence *float64 `json:"confidence"`
		Rationale  string   `json:"rationale"`
	}
	dec := json.NewDecoder(strings.NewReader(s[start : end+1]))
	if err := dec.Decode(&out); err != nil {
		return Classification{}, fmt.Errorf("%w: %v", ErrBadOutput, err)
	}
	c := Classification{
		Category:  strings.ToLower(strings.TrimSpace(out.Category)),
		Priority:  strings.ToLower(strings.TrimSpace(out.Priority)),
		Summary:   strings.TrimSpace(out.Summary),
		Rationale: strings.TrimSpace(out.Rationale),
	}
	if !tx.hasCategory(c.Category) {
		return Classification{}, fmt.Errorf("%w: unknown category %q", ErrBadOutput, out.Category)
	}
	if !tx.hasPriority(c.Priority) {
		return Classification{}, fmt.Errorf("%w: unknown priority %q", ErrBadOutput, out.Priority)
	}
	if out.Confidence == nil || math.IsNaN(*out.Confidence) || *out.Confidence < 0 || *out.Confidence > 1 {
		return Classification{}, fmt.Errorf("%w: confidence missing or outside [0,1]", ErrBadOutput)
	}
	if c.Summary == "" {
		return Classification{}, fmt.Errorf("%w: empty summary", ErrBadOutput)
	}
	c.Confidence = round2(*out.Confidence)
	return c, nil
}

// Fallback is the classification recorded when the classifier fails: it is
// deliberately "other" at confidence 0 so the router sends it to a human.
func Fallback(err error, classifier string, now time.Time) Classification {
	return Classification{
		Category:     "other",
		Priority:     "medium",
		Summary:      "Automatic classification failed; needs a human to read it.",
		Confidence:   0,
		Rationale:    "classifier error: " + err.Error(),
		Classifier:   classifier,
		ClassifiedAt: now.UTC(),
	}
}

// Prompt renders the system and user prompts from config templates.
type Prompt struct {
	system *template.Template
	user   *template.Template
	cfg    *config.Config
}

// NewPrompt parses the prompt templates in config.
func NewPrompt(c *config.Config) (*Prompt, error) {
	sys, err := template.New("system").Option("missingkey=error").Parse(c.Prompt.System)
	if err != nil {
		return nil, fmt.Errorf("prompt.system: %w", err)
	}
	usr, err := template.New("user").Option("missingkey=error").Parse(c.Prompt.User)
	if err != nil {
		return nil, fmt.Errorf("prompt.user: %w", err)
	}
	return &Prompt{system: sys, user: usr, cfg: c}, nil
}

// System renders the system prompt (taxonomy + output contract).
func (p *Prompt) System() (string, error) {
	var b bytes.Buffer
	err := p.system.Execute(&b, p.cfg.Taxonomy)
	return b.String(), err
}

// User renders the per-ticket user message.
func (p *Prompt) User(t ingest.Ticket) (string, error) {
	var b bytes.Buffer
	err := p.user.Execute(&b, t)
	return b.String(), err
}

func round2(f float64) float64 { return math.Round(f*100) / 100 }
