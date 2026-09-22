package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

type cli struct {
	t    *testing.T
	base []string
}

func newCLI(t *testing.T) cli {
	return cli{t: t, base: []string{"-config", testutil.ConfigPath(t), "-data", t.TempDir()}}
}

func (c cli) run(stdin string, args ...string) (string, string, int) {
	c.t.Helper()
	var out, errOut bytes.Buffer
	code := run(context.Background(), append(append([]string{}, c.base...), args...), strings.NewReader(stdin), &out, &errOut)
	return out.String(), errOut.String(), code
}

func tickets(t *testing.T) string { return filepath.Join(testutil.RepoRoot(t), "demo", "tickets") }

func TestIngestProcessStatus(t *testing.T) {
	c := newCLI(t)
	out, errOut, code := c.run("", "ingest", "-process", tickets(t))
	if code != 0 {
		t.Fatalf("ingest exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, "processed 10 item(s)") {
		t.Errorf("ingest output: %s", out)
	}
	// re-ingest is idempotent
	out, _, _ = c.run("", "ingest", tickets(t))
	if strings.Count(out, "duplicate") != 10 {
		t.Errorf("second ingest should report 10 duplicates: %s", out)
	}

	out, _, code = c.run("", "-json", "status")
	if code != 0 {
		t.Fatal(code)
	}
	var st struct {
		Stats struct {
			Total       int `json:"total"`
			Automatic   int `json:"automatic"`
			NeedsReview int `json:"needs_review"`
		} `json:"stats"`
	}
	if err := json.Unmarshal([]byte(out), &st); err != nil {
		t.Fatal(err)
	}
	if st.Stats.Total != 10 || st.Stats.NeedsReview != 4 {
		t.Errorf("stats: %+v", st.Stats)
	}

	out, _, _ = c.run("", "status", "-review")
	if strings.Count(out, "YES") != 4 {
		t.Errorf("review filter:\n%s", out)
	}

	out, _, _ = c.run("", "outbox")
	for _, want := range []string{"#rivergate-oncall", "ENG-1001", "ENG-BACKLOG-1003", "why:"} {
		if !strings.Contains(out, want) {
			t.Errorf("outbox missing %q:\n%s", want, out)
		}
	}
}

func TestClassifyPreviewIsDryRun(t *testing.T) {
	c := newCLI(t)
	out, errOut, code := c.run("", "classify", "-file", filepath.Join(tickets(t), "03-chat-double-charge.json"))
	if code != 0 {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	for _, want := range []string{"category    billing", `queue "billing"`, "dry run"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
	out, _, _ = c.run("", "status")
	if !strings.HasPrefix(out, "0 item(s)") {
		t.Errorf("preview must not store anything:\n%s", out)
	}
}

func TestReviewRouteAndItemDetail(t *testing.T) {
	c := newCLI(t)
	out, _, _ := c.run("", "ingest", "-process", filepath.Join(tickets(t), "10-email-late-notifications.json"))
	id := regexp.MustCompile(`RG-[0-9A-F]{8}`).FindString(out)
	if id == "" {
		t.Fatalf("no id in: %s", out)
	}
	out, _, _ = c.run("", "route", "-dry-run", id)
	if !strings.Contains(out, `queue "human-review"`) {
		t.Errorf("dry-run route: %s", out)
	}
	out, errOut, code := c.run("", "review", "override", id, "-reviewer", "sam", "-category", "bug", "-priority", "high", "-note", "reproduced")
	if code != 0 {
		t.Fatalf("override exit %d: %s", code, errOut)
	}
	if !strings.Contains(out, "queue=bug") || !strings.Contains(out, "human-verified") {
		t.Errorf("override output:\n%s", out)
	}
	out, _, _ = c.run("", "status", id)
	for _, want := range []string{"audit trail:", "received", "classified", "routed", "review", "cli:sam", "ENG-BACKLOG"} {
		if !strings.Contains(out, want) {
			t.Errorf("item detail missing %q:\n%s", want, out)
		}
	}
}

func TestMCPOverCLI(t *testing.T) {
	c := newCLI(t)
	if _, errOut, code := c.run("", "ingest", "-process", tickets(t)); code != 0 {
		t.Fatal(errOut)
	}
	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_queue","arguments":{}}}`,
	}, "\n")
	out, errOut, code := c.run(in, "mcp")
	if code != 0 {
		t.Fatalf("mcp exit %d: %s", code, errOut)
	}
	if !strings.Contains(errOut, "none — read-only") {
		t.Errorf("stderr should announce read-only mode: %s", errOut)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || strings.Contains(lines[1], "route_item") || !strings.Contains(lines[2], `\"count\": 4`) {
		t.Errorf("mcp output:\n%s", out)
	}
	if _, errOut, code := c.run("", "mcp", "-allow-write", "all"); code != 2 || !strings.Contains(errOut, "unknown write tool") {
		t.Errorf("-allow-write all should be rejected: %d %s", code, errOut)
	}
}

func TestUsageErrors(t *testing.T) {
	c := newCLI(t)
	for _, args := range [][]string{{}, {"bogus"}, {"route"}, {"ingest"}, {"reset"}} {
		if _, _, code := c.run("", args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
}
