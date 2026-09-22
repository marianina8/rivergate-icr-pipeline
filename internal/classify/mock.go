package classify

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
)

// Mock is a deterministic keyword classifier. It exists so every phase of the
// pipeline can be built, tested and demoed with zero network and zero AWS.
// It is intentionally simple — it stands in for Bedrock; it is not the product.
type Mock struct {
	tx         Taxonomy
	categories []scored // in taxonomy order, for stable tie-breaks
	priorities map[string][]*regexp.Regexp
	now        func() time.Time
}

type scored struct {
	name     string
	patterns []*regexp.Regexp
	words    []string
}

// NewMock builds a mock classifier from the config keyword lists.
func NewMock(c *config.Config) (*Mock, error) {
	m := &Mock{tx: TaxonomyFrom(c), priorities: map[string][]*regexp.Regexp{}, now: time.Now}
	for _, cat := range m.tx.Categories {
		words := c.Classifier.Mock.Categories[cat]
		ps, err := compile(words)
		if err != nil {
			return nil, fmt.Errorf("mock keywords for %s: %w", cat, err)
		}
		m.categories = append(m.categories, scored{name: cat, patterns: ps, words: words})
	}
	for p, words := range c.Classifier.Mock.Priorities {
		ps, err := compile(words)
		if err != nil {
			return nil, fmt.Errorf("mock keywords for %s: %w", p, err)
		}
		m.priorities[p] = ps
	}
	return m, nil
}

// KeywordRegexp compiles a config keyword into a case-insensitive whole-word
// pattern; a trailing "*" means prefix match (e.g. "invoice*").
func KeywordRegexp(kw string) (*regexp.Regexp, error) {
	kw = strings.ToLower(strings.TrimSpace(kw))
	suffix := `\b`
	if strings.HasSuffix(kw, "*") {
		kw, suffix = strings.TrimSuffix(kw, "*"), ``
	}
	return regexp.Compile(`(?i)\b` + regexp.QuoteMeta(kw) + suffix)
}

func compile(words []string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(words))
	for _, w := range words {
		re, err := KeywordRegexp(w)
		if err != nil {
			return nil, err
		}
		out = append(out, re)
	}
	return out, nil
}

// Name identifies this classifier in the audit trail.
func (m *Mock) Name() string { return "mock-keyword-v1" }

// Classify scores each category by distinct keyword hits and derives a
// confidence from how many hits the winner has and how far ahead it is.
func (m *Mock) Classify(_ context.Context, t ingest.Ticket) (Classification, error) {
	text := normalizeText(t.Subject + "\n" + t.Body)

	type hit struct {
		name  string
		score int
		words []string
	}
	var hits []hit
	for _, c := range m.categories {
		h := hit{name: c.name}
		for i, re := range c.patterns {
			if re.MatchString(text) {
				h.score++
				h.words = append(h.words, c.words[i])
			}
		}
		hits = append(hits, h)
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	top, second := hits[0], hits[1]

	c := Classification{Classifier: m.Name(), ClassifiedAt: m.now().UTC()}
	if top.score == 0 {
		c.Category, c.Confidence = "other", 0.35
		c.Rationale = "mock: no category keywords matched"
	} else {
		c.Category = top.name
		strength := float64(min(top.score, 3))
		conf := 0.45 + 0.15*strength - 0.2*float64(second.score)/float64(top.score)
		c.Confidence = round2(clamp(conf, 0.05, 0.95))
		c.Rationale = fmt.Sprintf("mock: %d keyword(s) for %s %q", top.score, top.name, top.words)
		if second.score > 0 {
			c.Rationale += fmt.Sprintf("; runner-up %s (%d)", second.name, second.score)
		}
	}
	c.Priority = m.priority(text, c.Category)
	c.Summary = mockSummary(t)
	return c, nil
}

func (m *Mock) priority(text, category string) string {
	for _, p := range []string{"critical", "high", "low"} {
		for _, re := range m.priorities[p] {
			if re.MatchString(text) {
				return p
			}
		}
	}
	if category == "outage" {
		return "high"
	}
	return "medium"
}

func mockSummary(t ingest.Ticket) string {
	who := t.Company
	if who == "" {
		who = "A customer"
	}
	s := firstSentence(t.Body)
	if s == "" {
		s = t.Subject
	}
	return fmt.Sprintf("%s (%s): %s", who, t.Channel, s)
}

func firstSentence(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	for i, r := range s {
		if (r == '.' || r == '?' || r == '!') && i > 20 {
			s = s[:i+1]
			break
		}
	}
	if r := []rune(s); len(r) > 180 {
		s = strings.TrimSpace(string(r[:179])) + "…"
	}
	return s
}

func normalizeText(s string) string {
	return strings.NewReplacer("’", "'", "‘", "'", "“", `"`, "”", `"`).Replace(s)
}

func clamp(f, lo, hi float64) float64 {
	if f < lo {
		return lo
	}
	if f > hi {
		return hi
	}
	return f
}
