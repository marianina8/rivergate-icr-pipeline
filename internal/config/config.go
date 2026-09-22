// Package config loads the per-instance configuration: the classification
// prompt and taxonomy, the routing rules, and action-stub settings. These are
// the parts of the ICR pipeline that change per prospect; code does not.
package config

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

// Config is the full instance configuration (see config/rivergate.yaml).
type Config struct {
	Instance   Instance   `yaml:"instance"`
	Classifier Classifier `yaml:"classifier"`
	Taxonomy   Taxonomy   `yaml:"taxonomy"`
	Prompt     Prompt     `yaml:"prompt"`
	Routing    Routing    `yaml:"routing"`
	Actions    Actions    `yaml:"actions"`
}

type Instance struct {
	Company string `yaml:"company"`
	UseCase string `yaml:"use_case"`
}

type Classifier struct {
	Provider string  `yaml:"provider"`
	Bedrock  Bedrock `yaml:"bedrock"`
	Mock     Mock    `yaml:"mock"`
}

type Bedrock struct {
	Region      string  `yaml:"region"`
	ModelID     string  `yaml:"model_id"`
	MaxTokens   int32   `yaml:"max_tokens"`
	Temperature float32 `yaml:"temperature"`
}

// Mock holds keyword lists for the offline keyword classifier.
type Mock struct {
	Categories map[string][]string `yaml:"categories"`
	Priorities map[string][]string `yaml:"priorities"`
}

type Taxonomy struct {
	Categories []Term `yaml:"categories"`
	Priorities []Term `yaml:"priorities"`
}

type Term struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

type Prompt struct {
	System string `yaml:"system"`
	User   string `yaml:"user"`
}

type Routing struct {
	ConfidenceFloor float64 `yaml:"confidence_floor"`
	Rules           []Rule  `yaml:"rules"`
	Default         Default `yaml:"default"`
}

type Rule struct {
	Name                  string   `yaml:"name"`
	Description           string   `yaml:"description"`
	Categories            []string `yaml:"categories"`
	Priorities            []string `yaml:"priorities"`
	MinConfidence         float64  `yaml:"min_confidence"`
	BypassConfidenceFloor bool     `yaml:"bypass_confidence_floor"`
	AlsoReviewBelow       float64  `yaml:"also_review_below"`
	Queue                 string   `yaml:"queue"`
	Actions               []Action `yaml:"actions"`
}

type Action struct {
	Type     string `yaml:"type" json:"type"`
	Target   string `yaml:"target" json:"target,omitempty"`
	Priority string `yaml:"priority" json:"priority,omitempty"`
}

type Default struct {
	Name          string `yaml:"name"`
	Queue         string `yaml:"queue"`
	ApprovedQueue string `yaml:"approved_queue"`
}

type Actions struct {
	KBArticles []KBArticle `yaml:"kb_articles"`
}

type KBArticle struct {
	Title    string   `yaml:"title" json:"title"`
	URL      string   `yaml:"url" json:"url"`
	Keywords []string `yaml:"keywords" json:"-"`
}

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(b)
}

// Parse decodes and validates config YAML.
func Parse(b []byte) (*Config, error) {
	var c Config
	if err := yaml.UnmarshalWithOptions(b, &c, yaml.Strict()); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// CategoryNames returns the allowed category names in config order.
func (c *Config) CategoryNames() []string { return names(c.Taxonomy.Categories) }

// PriorityNames returns the allowed priority names in config order.
func (c *Config) PriorityNames() []string { return names(c.Taxonomy.Priorities) }

func names(ts []Term) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.Name
	}
	return out
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

// Validate checks internal consistency so a typo in a rule fails loudly at
// startup instead of silently never matching.
func (c *Config) Validate() error {
	cats, pris := c.CategoryNames(), c.PriorityNames()
	if len(cats) == 0 || len(pris) == 0 {
		return fmt.Errorf("config: taxonomy needs at least one category and one priority")
	}
	if !contains(cats, "other") {
		return fmt.Errorf("config: taxonomy must include an \"other\" category (used as the safe fallback)")
	}
	if c.Prompt.System == "" || c.Prompt.User == "" {
		return fmt.Errorf("config: prompt.system and prompt.user are required")
	}
	if f := c.Routing.ConfidenceFloor; f < 0 || f > 1 {
		return fmt.Errorf("config: routing.confidence_floor must be within [0,1]")
	}
	if c.Routing.Default.Queue == "" {
		return fmt.Errorf("config: routing.default.queue is required")
	}
	seen := map[string]bool{}
	for _, r := range c.Routing.Rules {
		if r.Name == "" || r.Queue == "" {
			return fmt.Errorf("config: every routing rule needs a name and a queue")
		}
		if seen[r.Name] {
			return fmt.Errorf("config: duplicate routing rule %q", r.Name)
		}
		seen[r.Name] = true
		if len(r.Categories) == 0 && len(r.Priorities) == 0 {
			return fmt.Errorf("config: rule %q matches nothing (no categories or priorities)", r.Name)
		}
		for _, x := range r.Categories {
			if !contains(cats, x) {
				return fmt.Errorf("config: rule %q references unknown category %q", r.Name, x)
			}
		}
		for _, x := range r.Priorities {
			if !contains(pris, x) {
				return fmt.Errorf("config: rule %q references unknown priority %q", r.Name, x)
			}
		}
		if r.MinConfidence < 0 || r.MinConfidence > 1 || r.AlsoReviewBelow < 0 || r.AlsoReviewBelow > 1 {
			return fmt.Errorf("config: rule %q has a confidence threshold outside [0,1]", r.Name)
		}
		for _, a := range r.Actions {
			switch a.Type {
			case "notify", "create_ticket", "suggest_kb":
			default:
				return fmt.Errorf("config: rule %q has unknown action type %q", r.Name, a.Type)
			}
		}
	}
	for cat := range c.Classifier.Mock.Categories {
		if !contains(cats, cat) {
			return fmt.Errorf("config: classifier.mock.categories has unknown category %q", cat)
		}
	}
	for p := range c.Classifier.Mock.Priorities {
		if !contains(pris, p) {
			return fmt.Errorf("config: classifier.mock.priorities has unknown priority %q", p)
		}
	}
	return nil
}
