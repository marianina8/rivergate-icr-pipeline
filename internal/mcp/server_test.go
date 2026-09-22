package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/mcp"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

// newService returns a pipeline with all demo fixtures processed.
func newService(t *testing.T) (*pipeline.Service, map[string]string) {
	t.Helper()
	cfg := testutil.Config(t)
	m, _ := classify.NewMock(cfg)
	q, _ := ingest.NewDirQueue(t.TempDir())
	ob, _ := router.NewOutbox(t.TempDir(), cfg.Actions.KBArticles)
	svc := &pipeline.Service{Cfg: cfg, Classifier: m, Store: store.NewMemory(), Router: router.New(cfg), Actions: ob, Queue: q, Now: time.Now}
	ids := map[string]string{}
	ctx := context.Background()
	for _, f := range testutil.Fixtures(t) {
		tk, _, err := svc.Ingest(ctx, f.Payload, "test")
		if err != nil {
			t.Fatal(err)
		}
		ids[f.Name] = tk.ID
	}
	if _, err := svc.RunOnce(ctx, 20); err != nil {
		t.Fatal(err)
	}
	return svc, ids
}

type rpcResp struct {
	ID     int             `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type toolResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	IsError           bool           `json:"isError"`
	StructuredContent map[string]any `json:"structuredContent"`
}

// session drives the server over its real stdio framing.
func session(t *testing.T, srv *mcp.Server, msgs ...string) []rpcResp {
	t.Helper()
	var out bytes.Buffer
	if err := srv.Serve(context.Background(), strings.NewReader(strings.Join(msgs, "\n")+"\n"), &out); err != nil {
		t.Fatal(err)
	}
	var resps []rpcResp
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if line == "" {
			continue
		}
		var r rpcResp
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("bad response line %q: %v", line, err)
		}
		resps = append(resps, r)
	}
	return resps
}

func call(id int, tool string, args any) string {
	a, _ := json.Marshal(args)
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":%q,"arguments":%s}}`, id, tool, a)
}

func decodeTool(t *testing.T, r rpcResp) toolResult {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("rpc error: %+v", r.Error)
	}
	var tr toolResult
	if err := json.Unmarshal(r.Result, &tr); err != nil {
		t.Fatal(err)
	}
	return tr
}

const initMsg = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}`

func TestHandshakeAndReadOnlyToolList(t *testing.T) {
	svc, _ := newService(t)
	srv := &mcp.Server{Svc: svc, Version: "test"}
	resps := session(t, srv,
		initMsg,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"ping"}`,
		`{"jsonrpc":"2.0","id":4,"method":"resources/list"}`,
	)
	if len(resps) != 4 {
		t.Fatalf("got %d responses, want 4 (notification gets none)", len(resps))
	}
	var init struct {
		ProtocolVersion string         `json:"protocolVersion"`
		Capabilities    map[string]any `json:"capabilities"`
	}
	_ = json.Unmarshal(resps[0].Result, &init)
	if init.ProtocolVersion != "2025-06-18" || init.Capabilities["tools"] == nil {
		t.Errorf("initialize = %s", resps[0].Result)
	}
	var list struct {
		Tools []struct {
			Name        string         `json:"name"`
			InputSchema map[string]any `json:"inputSchema"`
		} `json:"tools"`
	}
	_ = json.Unmarshal(resps[1].Result, &list)
	var names []string
	for _, tl := range list.Tools {
		names = append(names, tl.Name)
		if tl.InputSchema["type"] != "object" {
			t.Errorf("%s: inputSchema must be an object schema", tl.Name)
		}
	}
	if got := strings.Join(names, ","); got != "status,get_item,list_queue" {
		t.Errorf("default tools = %s; write tools must be hidden unless enabled", got)
	}
	if resps[3].Error == nil || resps[3].Error.Code != -32601 {
		t.Errorf("unknown method should be -32601: %+v", resps[3])
	}
}

func TestReadTools(t *testing.T) {
	svc, ids := newService(t)
	srv := &mcp.Server{Svc: svc}
	resps := session(t, srv, initMsg,
		call(2, "status", map[string]any{}),
		call(3, "list_queue", map[string]any{}),
		call(4, "list_queue", map[string]any{"queue": "billing"}),
		call(5, "get_item", map[string]any{"id": ids["01-webform-outage-503.json"]}),
		call(6, "get_item", map[string]any{"id": "RG-NOPE0000"}),
		call(7, "get_item", map[string]any{"id": "x", "extra": 1}),
	)
	st := decodeTool(t, resps[1])
	if st.IsError || st.StructuredContent["total"].(float64) != 10 || st.StructuredContent["needs_review"].(float64) != 4 {
		t.Errorf("status = %+v", st.StructuredContent)
	}
	if rq := decodeTool(t, resps[2]); rq.StructuredContent["count"].(float64) != 4 {
		t.Errorf("default list_queue should be the review queue: %v", rq.StructuredContent["count"])
	}
	if bq := decodeTool(t, resps[3]); bq.StructuredContent["count"].(float64) != 1 {
		t.Errorf("billing queue count = %v", bq.StructuredContent["count"])
	}
	item := decodeTool(t, resps[4])
	if item.IsError || item.StructuredContent["queue"] != "incident" {
		t.Errorf("get_item = %v", item.StructuredContent["queue"])
	}
	if nf := decodeTool(t, resps[5]); !nf.IsError {
		t.Error("unknown id should be a tool error")
	}
	if bad := decodeTool(t, resps[6]); !bad.IsError || !strings.Contains(bad.Content[0].Text, "invalid arguments") {
		t.Errorf("unknown argument should be rejected: %+v", bad)
	}
}

func TestWriteToolsAreOptIn(t *testing.T) {
	svc, ids := newService(t)
	id := ids["10-email-late-notifications.json"]

	// Not enabled: calling it anyway is refused and nothing changes.
	srv := &mcp.Server{Svc: svc}
	resps := session(t, srv, initMsg, call(2, "approve", map[string]any{"id": id, "note": "ok"}))
	if r := decodeTool(t, resps[1]); !r.IsError || !strings.Contains(r.Content[0].Text, "not enabled") {
		t.Errorf("disabled write tool should refuse: %+v", r)
	}
	if it, _ := svc.Get(context.Background(), id); it.Review != nil {
		t.Fatal("refused call must not modify the item")
	}

	// Enable only route_item: approve stays unavailable.
	allowed, err := mcp.ParseAllowWrite("route_item")
	if err != nil {
		t.Fatal(err)
	}
	srv = &mcp.Server{Svc: svc, AllowWrite: allowed, Actor: "triage-bot"}
	resps = session(t, srv, initMsg,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		call(3, "route_item", map[string]any{"id": id}),
		call(4, "approve", map[string]any{"id": id, "note": "ok"}),
	)
	if !strings.Contains(string(resps[1].Result), `"route_item"`) || strings.Contains(string(resps[1].Result), `"approve"`) {
		t.Errorf("tools/list should include only the enabled write tool: %s", resps[1].Result)
	}
	routed := decodeTool(t, resps[2])
	// The agent cannot push a low-confidence item past the gate.
	if routed.IsError || routed.StructuredContent["queue"] != "human-review" || routed.StructuredContent["needs_review"] != true {
		t.Errorf("route_item on low-confidence item must stay with a human: %v", routed.StructuredContent["queue"])
	}
	if r := decodeTool(t, resps[3]); !r.IsError {
		t.Error("approve should still be disabled")
	}
	it, _ := svc.Get(context.Background(), id)
	last := it.Events[len(it.Events)-1]
	if last.Actor != "mcp:triage-bot" {
		t.Errorf("write calls must be attributed in the audit trail, got actor %q", last.Actor)
	}
}

func TestApproveToolRequiresNote(t *testing.T) {
	svc, ids := newService(t)
	allowed, _ := mcp.ParseAllowWrite("approve,reclassify")
	srv := &mcp.Server{Svc: svc, AllowWrite: allowed}
	id := ids["07-email-guest-seats-ambiguous.json"]
	resps := session(t, srv, initMsg,
		call(2, "approve", map[string]any{"id": id}),
		call(3, "approve", map[string]any{"id": id, "note": "reviewer said permissions question", "category": "how_to"}),
		call(4, "reclassify", map[string]any{"id": id}),
	)
	if r := decodeTool(t, resps[1]); !r.IsError {
		t.Error("approve without a note should be rejected")
	}
	ov := decodeTool(t, resps[2])
	if ov.IsError || ov.StructuredContent["queue"] != "self-serve" {
		t.Errorf("override via MCP: %v", ov.StructuredContent["queue"])
	}
	if r := decodeTool(t, resps[3]); r.IsError {
		t.Errorf("reclassify: %s", r.Content[0].Text)
	}
}

func TestParseAllowWrite(t *testing.T) {
	if _, err := mcp.ParseAllowWrite("all"); err == nil {
		t.Error(`"all" must not be accepted: write tools are enabled one by one`)
	}
	if m, err := mcp.ParseAllowWrite(""); err != nil || len(m) != 0 {
		t.Error("empty spec means read-only")
	}
	if m, err := mcp.ParseAllowWrite(" route_item , approve "); err != nil || !m["route_item"] || !m["approve"] {
		t.Errorf("got %v %v", m, err)
	}
}

func TestMalformedInput(t *testing.T) {
	svc, _ := newService(t)
	resps := session(t, &mcp.Server{Svc: svc}, `{not json`, `{"jsonrpc":"1.0","id":9,"method":"ping"}`)
	if len(resps) != 2 || resps[0].Error == nil || resps[0].Error.Code != -32700 || resps[1].Error == nil || resps[1].Error.Code != -32600 {
		t.Errorf("got %+v", resps)
	}
}
