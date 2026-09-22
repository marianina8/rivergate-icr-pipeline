// Package mcp exposes the pipeline's CLI operations as MCP (Model Context
// Protocol) tools, so an AI agent can operate the pipeline the way a human
// does with the CLI.
//
// Safety model:
//   - Read tools (status, get_item, list_queue) are always available.
//   - Write/routing tools (route_item, reclassify, approve) are OFF by default.
//     Each must be enabled by name (icr mcp -allow-write route_item,...).
//     There is deliberately no "all" shortcut.
//   - Disabled write tools are not advertised in tools/list, and calling one
//     anyway returns an error — no implicit write access.
//   - Every write call is recorded in the item's audit trail as mcp:<actor>.
//   - Routing through MCP uses the same rules engine: an agent cannot push a
//     low-confidence item past the human-review gate with route_item.
//
// This is a small, dependency-free implementation of the MCP stdio transport
// (newline-delimited JSON-RPC 2.0) covering initialize, ping, tools/list and
// tools/call. It can be swapped for the official Go SDK
// (github.com/modelcontextprotocol/go-sdk) without changing the tool surface.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

// Supported MCP protocol revisions, newest first.
var protocolVersions = []string{"2025-06-18", "2025-03-26", "2024-11-05"}

// Server is an MCP server over the pipeline service.
type Server struct {
	Svc        *pipeline.Service
	AllowWrite map[string]bool // write tools explicitly enabled
	Actor      string          // audit-trail name for write calls
	Version    string

	outMu sync.Mutex
}

// ---- JSON-RPC plumbing -----------------------------------------------------

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	codeParse          = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// Serve reads newline-delimited JSON-RPC messages from in and writes
// responses to out until in is closed or ctx is cancelled.
func (s *Server) Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)
	enc := json.NewEncoder(out)
	for sc.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if resp := s.Handle(ctx, []byte(line)); resp != nil {
			s.outMu.Lock()
			err := enc.Encode(resp)
			s.outMu.Unlock()
			if err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

// Handle processes one JSON-RPC message and returns the response to send
// (nil for notifications).
func (s *Server) Handle(ctx context.Context, msg []byte) *response {
	var req request
	if err := json.Unmarshal(msg, &req); err != nil {
		return &response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{codeParse, "parse error: " + err.Error()}}
	}
	isNotification := len(req.ID) == 0
	if req.JSONRPC != "2.0" || req.Method == "" {
		if isNotification {
			return nil
		}
		return &response{JSONRPC: "2.0", ID: req.ID, Error: &rpcError{codeInvalidRequest, "invalid request"}}
	}
	result, rerr := s.dispatch(ctx, req)
	if isNotification {
		return nil
	}
	resp := &response{JSONRPC: "2.0", ID: req.ID}
	if rerr != nil {
		resp.Error = rerr
	} else {
		resp.Result = result
	}
	return resp
}

func (s *Server) dispatch(ctx context.Context, req request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &p)
		v := protocolVersions[0]
		for _, pv := range protocolVersions {
			if pv == p.ProtocolVersion {
				v = pv
			}
		}
		return map[string]any{
			"protocolVersion": v,
			"capabilities":    map[string]any{"tools": map[string]any{"listChanged": false}},
			"serverInfo":      map[string]any{"name": "rivergate-icr", "version": s.Version},
			"instructions": "Rivergate support-ticket triage pipeline. Read tools are always available. " +
				"Write tools are enabled individually by the operator; routing always follows the configured rules, " +
				"and low-confidence tickets stay with a human.",
		}, nil
	case "ping":
		return map[string]any{}, nil
	case "notifications/initialized", "notifications/cancelled":
		return nil, nil
	case "tools/list":
		return map[string]any{"tools": s.listTools()}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &p); err != nil || p.Name == "" {
			return nil, &rpcError{codeInvalidParams, "tools/call needs a tool name"}
		}
		return s.callTool(ctx, p.Name, p.Arguments)
	default:
		return nil, &rpcError{codeMethodNotFound, "method not found: " + req.Method}
	}
}

// ---- tools -----------------------------------------------------------------

type tool struct {
	Name        string         `json:"name"`
	Title       string         `json:"title,omitempty"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
	Annotations map[string]any `json:"annotations,omitempty"`
	write       bool
	run         func(ctx context.Context, s *Server, args json.RawMessage) (any, error)
}

func obj(props map[string]any, required ...string) map[string]any {
	m := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		m["required"] = required
	}
	return m
}

func str(desc string) map[string]any { return map[string]any{"type": "string", "description": desc} }

var idArg = map[string]any{"id": str("Item ID, e.g. RG-1A2B3C4D")}

var tools = []tool{
	{
		Name: "status", Title: "Pipeline status",
		Description: "Summary counts of items by lifecycle status and queue, and how many are waiting on human review.",
		InputSchema: obj(map[string]any{}),
		Annotations: map[string]any{"readOnlyHint": true},
		run: func(ctx context.Context, s *Server, _ json.RawMessage) (any, error) {
			return s.Svc.Stats(ctx)
		},
	},
	{
		Name: "get_item", Title: "Get item",
		Description: "Full record for one ticket: normalized ticket, classification, routing decision, review and audit trail.",
		InputSchema: obj(idArg, "id"),
		Annotations: map[string]any{"readOnlyHint": true},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ ID string }
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			return s.Svc.Get(ctx, a.ID)
		},
	},
	{
		Name: "list_queue", Title: "List queue",
		Description: "List items, optionally filtered by queue name (e.g. human-review, billing, bug, product, self-serve, incident) or status. Omit both for the human review queue.",
		InputSchema: obj(map[string]any{
			"queue":  str("Queue name to filter by"),
			"status": str("Lifecycle status: received, classified, routed, pending_review, reviewed, failed"),
		}),
		Annotations: map[string]any{"readOnlyHint": true},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ Queue, Status string }
			if err := decode(raw, &a); err != nil {
				return nil, errArgs(err.Error())
			}
			var items []store.Item
			var err error
			if a.Queue == "" && a.Status == "" {
				items, err = s.Svc.ReviewQueue(ctx)
			} else {
				items, err = s.Svc.List(ctx, store.Filter{Queue: a.Queue, Status: store.Status(a.Status)})
			}
			if err != nil {
				return nil, err
			}
			return map[string]any{"count": len(items), "items": summarize(items)}, nil
		},
	},
	{
		Name: "route_item", Title: "Route item", write: true,
		Description: "Re-apply the configured routing rules to a classified item and run the resulting actions. Cannot bypass confidence gates: low-confidence items still go to human review.",
		InputSchema: obj(idArg, "id"),
		Annotations: map[string]any{"readOnlyHint": false, "destructiveHint": false, "idempotentHint": true},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ ID string }
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			return s.Svc.Route(ctx, a.ID, s.actor())
		},
	},
	{
		Name: "reclassify", Title: "Reclassify item", write: true,
		Description: "Re-run the classifier on a stored item and re-route it. Actions already taken for the item are not repeated.",
		InputSchema: obj(idArg, "id"),
		Annotations: map[string]any{"readOnlyHint": false, "destructiveHint": false},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ ID string }
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			return s.Svc.Reclassify(ctx, a.ID, s.actor())
		},
	},
	{
		Name: "approve", Title: "Approve / override classification", write: true,
		Description: "Record a review decision on an item: approve the model's classification, or override category and/or priority. The item is then routed as human-verified. Only enable this for an agent acting on explicit instructions from a human reviewer.",
		InputSchema: obj(map[string]any{
			"id":       idArg["id"],
			"note":     str("Why — recorded in the audit trail"),
			"category": str("Override: corrected category (omit to approve as-is)"),
			"priority": str("Override: corrected priority (omit to approve as-is)"),
		}, "id", "note"),
		Annotations: map[string]any{"readOnlyHint": false, "destructiveHint": false},
		run: func(ctx context.Context, s *Server, raw json.RawMessage) (any, error) {
			var a struct{ ID, Note, Category, Priority string }
			if err := decode(raw, &a); err != nil || a.ID == "" {
				return nil, errArgs("id is required")
			}
			if strings.TrimSpace(a.Note) == "" {
				return nil, errArgs("note is required so the audit trail explains the decision")
			}
			if a.Category == "" && a.Priority == "" {
				return s.Svc.Approve(ctx, a.ID, s.actor(), a.Note)
			}
			return s.Svc.Override(ctx, a.ID, s.actor(), a.Category, a.Priority, a.Note)
		},
	},
}

// WriteToolNames lists the tools that require -allow-write.
func WriteToolNames() []string {
	var out []string
	for _, t := range tools {
		if t.write {
			out = append(out, t.Name)
		}
	}
	return out
}

// ParseAllowWrite validates a comma-separated list of write tool names.
func ParseAllowWrite(spec string) (map[string]bool, error) {
	out := map[string]bool{}
	valid := map[string]bool{}
	for _, n := range WriteToolNames() {
		valid[n] = true
	}
	for _, n := range strings.Split(spec, ",") {
		n = strings.TrimSpace(n)
		if n == "" {
			continue
		}
		if !valid[n] {
			return nil, fmt.Errorf("unknown write tool %q (valid: %s)", n, strings.Join(WriteToolNames(), ", "))
		}
		out[n] = true
	}
	return out, nil
}

func (s *Server) actor() string {
	a := s.Actor
	if a == "" {
		a = "agent"
	}
	return "mcp:" + a
}

func (s *Server) enabled(t tool) bool { return !t.write || s.AllowWrite[t.Name] }

func (s *Server) listTools() []tool {
	var out []tool
	for _, t := range tools {
		if s.enabled(t) {
			out = append(out, t)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return !out[i].write && out[j].write })
	return out
}

type argsError struct{ msg string }

func (e argsError) Error() string { return e.msg }
func errArgs(m string) error      { return argsError{m} }

func decode(raw json.RawMessage, v any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// callTool runs a tool. Per MCP, tool failures are returned as a result with
// isError=true (so the agent sees them), not as JSON-RPC protocol errors.
func (s *Server) callTool(ctx context.Context, name string, args json.RawMessage) (any, *rpcError) {
	var t *tool
	for i := range tools {
		if tools[i].Name == name {
			t = &tools[i]
		}
	}
	if t == nil {
		return nil, &rpcError{codeInvalidParams, "unknown tool: " + name}
	}
	if !s.enabled(*t) {
		return toolError(fmt.Sprintf("tool %q is a write tool and is not enabled on this server. "+
			"An operator must start the server with -allow-write %s.", name, name)), nil
	}
	out, err := t.run(ctx, s, args)
	if err != nil {
		var ae argsError
		switch {
		case errors.As(err, &ae):
			return toolError("invalid arguments: " + err.Error()), nil
		case errors.Is(err, store.ErrNotFound):
			return toolError(err.Error()), nil
		default:
			// Partial success (e.g. routed but one action failed) still
			// returns the item so the agent can see what happened.
			if out != nil {
				r := toolResult(out)
				r["isError"] = true
				r["content"] = append(r["content"].([]map[string]any), map[string]any{"type": "text", "text": "error: " + err.Error()})
				return r, nil
			}
			return toolError(err.Error()), nil
		}
	}
	return toolResult(out), nil
}

func toolResult(v any) map[string]any {
	b, _ := json.MarshalIndent(v, "", "  ")
	r := map[string]any{"content": []map[string]any{{"type": "text", "text": string(b)}}}
	// structuredContent must be a JSON object.
	var m map[string]any
	if json.Unmarshal(b, &m) == nil {
		r["structuredContent"] = m
	}
	return r
}

func toolError(msg string) map[string]any {
	return map[string]any{"content": []map[string]any{{"type": "text", "text": msg}}, "isError": true}
}

// itemSummary is the compact row list_queue returns.
type itemSummary struct {
	ID          string  `json:"id"`
	Subject     string  `json:"subject"`
	Company     string  `json:"company"`
	Channel     string  `json:"channel"`
	Status      string  `json:"status"`
	Queue       string  `json:"queue"`
	NeedsReview bool    `json:"needs_review"`
	Category    string  `json:"category,omitempty"`
	Priority    string  `json:"priority,omitempty"`
	Confidence  float64 `json:"confidence,omitempty"`
}

func summarize(items []store.Item) []itemSummary {
	out := make([]itemSummary, 0, len(items))
	for _, it := range items {
		s := itemSummary{ID: it.ID, Subject: it.Ticket.Subject, Company: it.Ticket.Company, Channel: it.Ticket.Channel,
			Status: string(it.Status), Queue: it.Queue, NeedsReview: it.NeedsReview}
		if c := it.Classification; c != nil {
			s.Category, s.Priority, s.Confidence = c.Category, c.Priority, c.Confidence
		}
		out = append(out, s)
	}
	return out
}
