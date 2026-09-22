package classify_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

func TestParseModelOutput(t *testing.T) {
	tx := classify.TaxonomyFrom(testutil.Config(t))
	good := `{"category":"billing","priority":"high","summary":"Customer was double charged.","confidence":0.873,"rationale":"mentions duplicate charge"}`
	cases := []struct {
		name, in string
		wantErr  bool
		wantConf float64
	}{
		{"plain json", good, false, 0.87},
		{"code fenced", "```json\n" + good + "\n```", false, 0.87},
		{"prose around json", "Here you go:\n" + good + "\nThanks", false, 0.87},
		{"uppercase labels normalized", strings.Replace(good, `"billing"`, `"BILLING"`, 1), false, 0.87},
		{"unknown category", strings.Replace(good, `"billing"`, `"refunds"`, 1), true, 0},
		{"unknown priority", strings.Replace(good, `"high"`, `"urgent"`, 1), true, 0},
		{"confidence > 1", strings.Replace(good, `0.873`, `1.4`, 1), true, 0},
		{"confidence missing", `{"category":"bug","priority":"low","summary":"x"}`, true, 0},
		{"empty summary", strings.Replace(good, `Customer was double charged.`, ``, 1), true, 0},
		{"no json", "I think this is billing.", true, 0},
		{"malformed json", `{"category": "bug",`, true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := classify.ParseModelOutput(tc.in, tx)
			if tc.wantErr {
				if !errors.Is(err, classify.ErrBadOutput) {
					t.Fatalf("err = %v, want ErrBadOutput", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Category != "billing" || c.Priority != "high" || c.Confidence != tc.wantConf {
				t.Errorf("got %+v", c)
			}
		})
	}
}

func TestFallbackAlwaysDefersToHuman(t *testing.T) {
	c := classify.Fallback(errors.New("timeout"), "x", time.Now())
	if c.Category != "other" || c.Confidence != 0 {
		t.Errorf("fallback must be other @ 0, got %s @ %v", c.Category, c.Confidence)
	}
}

func TestKeywordRegexp(t *testing.T) {
	cases := []struct {
		kw, text string
		want     bool
	}{
		{"error", "an error occurred", true},
		{"error", "errors everywhere", false}, // whole word
		{"invoice*", "our invoices", true},    // prefix
		{"how do i", "How do I export?", true},
		{"503", "a 503 page", true},
		{"doesn't work", "it doesn't work", true},
	}
	for _, tc := range cases {
		re, err := classify.KeywordRegexp(tc.kw)
		if err != nil {
			t.Fatal(err)
		}
		if got := re.MatchString(tc.text); got != tc.want {
			t.Errorf("%q in %q = %v, want %v", tc.kw, tc.text, got, tc.want)
		}
	}
}

// Golden expectations for the mock classifier on the demo fixtures. These pin
// the demo storyline: if a keyword list changes, this test says which demo
// ticket moved.
func TestMockClassifierOnFixtures(t *testing.T) {
	cfg := testutil.Config(t)
	m, err := classify.NewMock(cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]struct {
		cat, pri string
		conf     float64
	}{
		"01-webform-outage-503.json":          {"outage", "critical", 0.85},
		"02-email-data-loss-critical.json":    {"bug", "critical", 0.75},
		"03-chat-double-charge.json":          {"billing", "medium", 0.90},
		"04-email-timeline-crash.json":        {"bug", "medium", 0.90},
		"05-webform-recurring-tasks.json":     {"feature_request", "low", 0.90},
		"06-chat-export-csv.json":             {"how_to", "medium", 0.75},
		"07-email-guest-seats-ambiguous.json": {"billing", "medium", 0.55},
		"08-webform-partnership.json":         {"other", "low", 0.90},
		"09-chat-vague-asap.json":             {"other", "high", 0.35},
		"10-email-late-notifications.json":    {"bug", "medium", 0.60},
	}
	fixtures := testutil.Fixtures(t)
	if len(fixtures) != len(want) {
		t.Fatalf("%d fixtures, %d expectations", len(fixtures), len(want))
	}
	for _, f := range fixtures {
		t.Run(f.Name, func(t *testing.T) {
			tk, err := ingest.Normalize(f.Payload, "test", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			c, err := m.Classify(context.Background(), tk)
			if err != nil {
				t.Fatal(err)
			}
			w := want[f.Name]
			if c.Category != w.cat || c.Priority != w.pri || c.Confidence != w.conf {
				t.Errorf("got %s/%s@%.2f, want %s/%s@%.2f (%s)", c.Category, c.Priority, c.Confidence, w.cat, w.pri, w.conf, c.Rationale)
			}
			if c.Summary == "" || c.Rationale == "" {
				t.Error("summary and rationale must be set")
			}
		})
	}
}

func TestPromptRendersTaxonomyAndTicket(t *testing.T) {
	p, err := classify.NewPrompt(testutil.Config(t))
	if err != nil {
		t.Fatal(err)
	}
	sys, err := p.System()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"Rivergate", "- feature_request:", "- critical:", `"confidence"`, "untrusted"} {
		if !strings.Contains(sys, s) {
			t.Errorf("system prompt missing %q", s)
		}
	}
	usr, err := p.User(ingest.Ticket{Channel: "email", CustomerName: "Ada", Company: "Acme", Subject: "Subj", Body: "Body text"})
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"<ticket>", "channel: email", "Ada (Acme)", "subject: Subj", "Body text", "</ticket>"} {
		if !strings.Contains(usr, s) {
			t.Errorf("user prompt missing %q", s)
		}
	}
}

// fakeConverse stands in for Bedrock Runtime. No AWS calls are made.
type fakeConverse struct {
	reply string
	err   error
	got   *bedrockruntime.ConverseInput
}

func (f *fakeConverse) Converse(_ context.Context, in *bedrockruntime.ConverseInput, _ ...func(*bedrockruntime.Options)) (*bedrockruntime.ConverseOutput, error) {
	f.got = in
	if f.err != nil {
		return nil, f.err
	}
	return &bedrockruntime.ConverseOutput{Output: &types.ConverseOutputMemberMessage{Value: types.Message{
		Role:    types.ConversationRoleAssistant,
		Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: f.reply}},
	}}}, nil
}

func TestBedrockClassifierWithFakeClient(t *testing.T) {
	cfg := testutil.Config(t)
	fake := &fakeConverse{reply: `{"category":"outage","priority":"critical","summary":"Workspace down.","confidence":0.92,"rationale":"503 for all users"}`}
	b, err := classify.NewBedrock(fake, cfg)
	if err != nil {
		t.Fatal(err)
	}
	tk := ingest.Ticket{Channel: "webform", Subject: "All boards 503", Body: "Everything is down"}
	c, err := b.Classify(context.Background(), tk)
	if err != nil {
		t.Fatal(err)
	}
	if c.Category != "outage" || c.Confidence != 0.92 || !strings.HasPrefix(c.Classifier, "bedrock:") {
		t.Errorf("got %+v", c)
	}
	if *fake.got.ModelId != cfg.Classifier.Bedrock.ModelID {
		t.Errorf("model id = %s", *fake.got.ModelId)
	}
	if *fake.got.InferenceConfig.Temperature != 0 {
		t.Error("classification should run at temperature 0")
	}
	userText := fake.got.Messages[0].Content[0].(*types.ContentBlockMemberText).Value
	if !strings.Contains(userText, "All boards 503") {
		t.Errorf("ticket not in prompt: %s", userText)
	}

	fake.reply = "Sorry, I can't classify that."
	if _, err := b.Classify(context.Background(), tk); !errors.Is(err, classify.ErrBadOutput) {
		t.Errorf("non-JSON reply: err = %v, want ErrBadOutput", err)
	}
	fake.err = errors.New("throttled")
	if _, err := b.Classify(context.Background(), tk); err == nil {
		t.Error("client error should surface")
	}
}
