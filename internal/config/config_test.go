package config_test

import (
	"os"
	"strings"
	"testing"

	"github.com/marianina8/rivergate-icr-pipeline/internal/config"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

func TestRivergateConfigLoads(t *testing.T) {
	c := testutil.Config(t)
	if got := strings.Join(c.CategoryNames(), ","); got != "bug,billing,feature_request,outage,how_to,other" {
		t.Errorf("categories = %s", got)
	}
	if got := strings.Join(c.PriorityNames(), ","); got != "low,medium,high,critical" {
		t.Errorf("priorities = %s", got)
	}
	if c.Classifier.Provider != "mock" {
		t.Errorf("default classifier must be the offline mock, got %q", c.Classifier.Provider)
	}
	if c.Routing.ConfidenceFloor != 0.6 {
		t.Errorf("confidence floor = %v, want 0.6", c.Routing.ConfidenceFloor)
	}
	if c.Routing.Rules[0].Name != "outage-or-critical" || !c.Routing.Rules[0].BypassConfidenceFloor {
		t.Errorf("first rule must be outage-or-critical and bypass the floor")
	}
}

func TestValidateRejectsBadRules(t *testing.T) {
	base, err := os.ReadFile(testutil.ConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct{ from, to, want string }{
		"unknown category in rule": {"categories: [billing]", "categories: [invoices]", "unknown category"},
		"unknown action":           {"type: suggest_kb", "type: send_email", "unknown action type"},
		"threshold out of range":   {"min_confidence: 0.75\n      queue: billing", "min_confidence: 7.5\n      queue: billing", "outside [0,1]"},
		"unknown yaml key":         {"confidence_floor: 0.6", "confidence_floor: 0.6\n  confidence_flor: 0.5", "confidence_flor"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			y := strings.Replace(string(base), tc.from, tc.to, 1)
			if y == string(base) {
				t.Fatalf("test setup: %q not found in config", tc.from)
			}
			_, err := config.Parse([]byte(y))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}
