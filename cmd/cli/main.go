// Command icr is the Rivergate ICR pipeline CLI: ingest, classify, route,
// status — plus review, outbox and an MCP server mode that exposes the same
// operations to AI agents.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/mcp"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

const usage = `icr — Rivergate support-ticket Ingest -> Classify -> Route pipeline

Usage:
  icr [global flags] <command> [flags] [args]

Commands:
  ingest [-process] <file|dir|->...   normalize tickets and enqueue them
  classify -file <ticket.json>        preview classification + routing (no writes)
  classify <id>                       re-run classification on a stored item and re-route it
  route [-dry-run] <id>               (re-)apply routing rules to a classified item
  status [-queue q] [-review] [id]    pipeline summary, a filtered list, or one item's audit trail
  review approve <id> -reviewer NAME [-note TEXT]
  review override <id> -reviewer NAME [-category C] [-priority P] [-note TEXT]
  outbox                              show stubbed Slack posts, tickets and KB suggestions
  mcp [-allow-write tools]            serve the pipeline as MCP tools over stdio
  reset -yes                          delete the local data dir (demo reset)

Global flags:
  -config PATH       instance config (env ICR_CONFIG, default config/rivergate.yaml)
  -data DIR          local data dir (env ICR_DATA_DIR, default .icr)
  -classifier NAME   mock | bedrock (env ICR_CLASSIFIER, default from config)
  -json              machine-readable output where supported
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

type env struct {
	ctx     context.Context
	cfgPath string
	dataDir string
	clName  string
	asJSON  bool
	stdin   io.Reader
	out     io.Writer
	errOut  io.Writer
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("icr", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	e := &env{ctx: ctx, stdin: stdin, out: stdout, errOut: stderr}
	fs.StringVar(&e.cfgPath, "config", pipeline.Env("ICR_CONFIG", pipeline.DefaultConfig), "")
	fs.StringVar(&e.dataDir, "data", pipeline.Env("ICR_DATA_DIR", pipeline.DefaultDataDir), "")
	fs.StringVar(&e.clName, "classifier", pipeline.Env("ICR_CLASSIFIER", ""), "")
	fs.BoolVar(&e.asJSON, "json", false, "")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fs.Usage()
		return 2
	}
	cmd, rest := fs.Arg(0), fs.Args()[1:]
	var err error
	switch cmd {
	case "ingest":
		err = e.ingest(rest)
	case "classify":
		err = e.classify(rest)
	case "route":
		err = e.route(rest)
	case "status":
		err = e.status(rest)
	case "review":
		err = e.review(rest)
	case "outbox":
		err = e.outbox(rest)
	case "mcp":
		err = e.mcp(rest)
	case "reset":
		err = e.reset(rest)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		if errors.Is(err, flag.ErrHelp) || errors.Is(err, errUsage) {
			return 2
		}
		return 1
	}
	return 0
}

var errUsage = errors.New("usage")

func (e *env) open() (*pipeline.Local, error) {
	lg := slog.New(slog.NewTextHandler(e.errOut, &slog.HandlerOptions{Level: slog.LevelWarn}))
	return pipeline.OpenLocal(e.ctx, e.cfgPath, e.dataDir, e.clName, lg)
}

func (e *env) flags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(e.errOut)
	fs.BoolVar(&e.asJSON, "json", e.asJSON, "machine-readable output")
	return fs
}

// parseInterleaved lets flags appear after positional args (icr route ID -dry-run).
func parseInterleaved(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func (e *env) printJSON(v any) error {
	enc := json.NewEncoder(e.out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// ---- ingest ----------------------------------------------------------------

func (e *env) ingest(args []string) error {
	fs := e.flags("ingest")
	process := fs.Bool("process", false, "also drain the queue now (skip running the worker)")
	paths, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("%w: icr ingest [-process] <file|dir|->...", errUsage)
	}
	p, err := e.open()
	if err != nil {
		return err
	}
	type result struct {
		File      string `json:"file"`
		ID        string `json:"id,omitempty"`
		Duplicate bool   `json:"duplicate,omitempty"`
		Error     string `json:"error,omitempty"`
	}
	var results []result
	var failed int
	for _, path := range paths {
		files, err := expand(path)
		if err != nil {
			return err
		}
		for _, f := range files {
			var b []byte
			if f == "-" {
				b, err = io.ReadAll(e.stdin)
			} else {
				b, err = os.ReadFile(f)
			}
			if err != nil {
				return err
			}
			t, dup, err := p.Ingest(e.ctx, b, "cli")
			r := result{File: f, ID: t.ID, Duplicate: dup}
			if err != nil {
				r.Error = err.Error()
				failed++
			}
			results = append(results, r)
		}
	}
	processed := 0
	if *process {
		for {
			n, err := p.RunOnce(e.ctx, 25)
			processed += n
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
		}
	}
	if e.asJSON {
		return e.printJSON(map[string]any{"ingested": results, "processed": processed})
	}
	for _, r := range results {
		switch {
		case r.Error != "":
			fmt.Fprintf(e.out, "REJECTED  %-40s %s\n", r.File, r.Error)
		case r.Duplicate:
			fmt.Fprintf(e.out, "duplicate %-40s %s (already ingested)\n", r.File, r.ID)
		default:
			fmt.Fprintf(e.out, "queued    %-40s %s\n", r.File, r.ID)
		}
	}
	if *process {
		fmt.Fprintf(e.out, "\nprocessed %d item(s). Run `icr status` to see where they went.\n", processed)
	} else {
		fmt.Fprintln(e.out, "\nqueued. Start the worker (go run ./cmd/worker) or re-run with -process.")
	}
	if failed > 0 {
		return fmt.Errorf("%d file(s) rejected", failed)
	}
	return nil
}

func expand(path string) ([]string, error) {
	if path == "-" {
		return []string{"-"}, nil
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return []string{path}, nil
	}
	matches, err := filepath.Glob(filepath.Join(path, "*.json"))
	sort.Strings(matches)
	return matches, err
}

// ---- classify / route ------------------------------------------------------

func (e *env) classify(args []string) error {
	fs := e.flags("classify")
	file := fs.String("file", "", "preview a raw ticket file without storing anything")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	p, err := e.open()
	if err != nil {
		return err
	}
	if *file != "" {
		b, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		t, err := ingest.Normalize(b, "cli-preview", time.Now())
		if err != nil {
			return err
		}
		c, d, err := p.Preview(e.ctx, t)
		if err != nil {
			return err
		}
		if e.asJSON {
			return e.printJSON(map[string]any{"ticket": t, "classification": c, "decision": d, "dry_run": true})
		}
		fmt.Fprintf(e.out, "ticket      %s  (%s) %s\n", t.ID, t.Channel, t.Subject)
		fmt.Fprintf(e.out, "category    %s\npriority    %s\nconfidence  %.2f\nsummary     %s\nrationale   %s\n", c.Category, c.Priority, c.Confidence, c.Summary, c.Rationale)
		fmt.Fprintf(e.out, "\nwould route -> queue %q via rule %q (automatic=%v, needs_review=%v)\n", d.Queue, d.Rule, d.Automatic, d.NeedsReview)
		for _, a := range d.Actions {
			fmt.Fprintf(e.out, "would run   %s %s\n", a.Type, a.Target)
		}
		fmt.Fprintln(e.out, "(dry run: nothing stored, no actions taken)")
		return nil
	}
	if len(pos) != 1 {
		return fmt.Errorf("%w: icr classify <id>  |  icr classify -file <ticket.json>", errUsage)
	}
	it, err := p.Reclassify(e.ctx, pos[0], "cli")
	if err != nil {
		return err
	}
	return e.showItem(it, false)
}

func (e *env) route(args []string) error {
	fs := e.flags("route")
	dry := fs.Bool("dry-run", false, "show the decision without storing it or running actions")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("%w: icr route [-dry-run] <id>", errUsage)
	}
	p, err := e.open()
	if err != nil {
		return err
	}
	if *dry {
		it, err := p.Get(e.ctx, pos[0])
		if err != nil {
			return err
		}
		if it.Classification == nil {
			return pipeline.ErrNotClassified
		}
		d := p.Router.Route(*it.Classification)
		if e.asJSON {
			return e.printJSON(d)
		}
		fmt.Fprintf(e.out, "%s -> queue %q via rule %q (automatic=%v, needs_review=%v)\n%s\n", it.ID, d.Queue, d.Rule, d.Automatic, d.NeedsReview, d.Reason)
		return nil
	}
	it, err := p.Route(e.ctx, pos[0], "cli")
	if err != nil {
		return err
	}
	return e.showItem(it, false)
}

// ---- status ----------------------------------------------------------------

func (e *env) status(args []string) error {
	fs := e.flags("status")
	queue := fs.String("queue", "", "only items in this queue")
	review := fs.Bool("review", false, "only items waiting on human review")
	pos, err := parseInterleaved(fs, args)
	if err != nil {
		return err
	}
	p, err := e.open()
	if err != nil {
		return err
	}
	if len(pos) == 1 {
		it, err := p.Get(e.ctx, pos[0])
		if err != nil {
			return err
		}
		return e.showItem(it, true)
	}
	f := store.Filter{Queue: *queue}
	if *review {
		yes := true
		f.NeedsReview = &yes
	}
	items, err := p.List(e.ctx, f)
	if err != nil {
		return err
	}
	st, err := p.Stats(e.ctx)
	if err != nil {
		return err
	}
	pending, inflight, dead := p.DirQueue.Depth()
	if e.asJSON {
		return e.printJSON(map[string]any{"stats": st, "queue": map[string]int{"pending": pending, "inflight": inflight, "dead": dead}, "items": items})
	}
	fmt.Fprintf(e.out, "%d item(s) | %d routed automatically | %d waiting on human review | queue: %d pending, %d in flight, %d dead-lettered\n\n",
		st.Total, st.Automatic, st.NeedsReview, pending, inflight, dead)
	tw := tabwriter.NewWriter(e.out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tCHANNEL\tCATEGORY\tPRIORITY\tCONF\tSTATUS\tQUEUE\tREVIEW\tSUBJECT")
	for _, it := range items {
		cat, pri, conf := "-", "-", "-"
		if c := it.Classification; c != nil {
			cat, pri, conf = c.Category, c.Priority, fmt.Sprintf("%.2f", c.Confidence)
		}
		rv := ""
		if it.NeedsReview {
			rv = "YES"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", it.ID, it.Ticket.Channel, cat, pri, conf, it.Status, orDash(it.Queue), rv, trunc(it.Ticket.Subject, 48))
	}
	return tw.Flush()
}

func (e *env) showItem(it store.Item, withEvents bool) error {
	if e.asJSON {
		return e.printJSON(it)
	}
	t := it.Ticket
	fmt.Fprintf(e.out, "%s  [%s]  %s\n", it.ID, it.Status, t.Subject)
	fmt.Fprintf(e.out, "from        %s <%s>, %s via %s\n", t.CustomerName, t.CustomerEmail, t.Company, t.Channel)
	if c := it.Classification; c != nil {
		fmt.Fprintf(e.out, "class       %s / %s @ %.2f  (%s", c.Category, c.Priority, c.Confidence, c.Classifier)
		if c.HumanVerified {
			fmt.Fprint(e.out, ", human-verified")
		}
		fmt.Fprintf(e.out, ")\nsummary     %s\n", c.Summary)
	}
	if d := it.Decision; d != nil {
		fmt.Fprintf(e.out, "routed      queue=%s rule=%s automatic=%v needs_review=%v\nreason      %s\n", d.Queue, d.Rule, d.Automatic, it.NeedsReview, d.Reason)
	}
	if withEvents {
		fmt.Fprintln(e.out, "\naudit trail:")
		for _, ev := range it.Events {
			fmt.Fprintf(e.out, "  %s  %-10s %-18s %s\n", ev.At.Format(time.RFC3339), ev.Type, ev.Actor, ev.Detail)
		}
	}
	return nil
}

// ---- review ----------------------------------------------------------------

func (e *env) review(args []string) error {
	if len(args) == 0 || (args[0] != "approve" && args[0] != "override") {
		return fmt.Errorf("%w: icr review approve|override <id> -reviewer NAME ...", errUsage)
	}
	fs := e.flags("review " + args[0])
	reviewer := fs.String("reviewer", os.Getenv("USER"), "who is making this decision (recorded in the audit trail)")
	note := fs.String("note", "", "optional note")
	category := fs.String("category", "", "override: corrected category")
	priority := fs.String("priority", "", "override: corrected priority")
	pos, err := parseInterleaved(fs, args[1:])
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("%w: icr review %s <id> -reviewer NAME", errUsage, args[0])
	}
	if *reviewer == "" {
		return fmt.Errorf("%w: -reviewer is required", errUsage)
	}
	p, err := e.open()
	if err != nil {
		return err
	}
	var it store.Item
	if args[0] == "approve" {
		it, err = p.Approve(e.ctx, pos[0], "cli:"+*reviewer, *note)
	} else {
		it, err = p.Override(e.ctx, pos[0], "cli:"+*reviewer, *category, *priority, *note)
	}
	if err != nil {
		return err
	}
	return e.showItem(it, false)
}

// ---- outbox ----------------------------------------------------------------

func (e *env) outbox(args []string) error {
	fs := e.flags("outbox")
	if _, err := parseInterleaved(fs, args); err != nil {
		return err
	}
	entries, err := router.ReadOutbox(pipeline.OutboxDir(e.dataDir))
	if err != nil {
		return err
	}
	if e.asJSON {
		return e.printJSON(entries)
	}
	if len(entries) == 0 {
		fmt.Fprintln(e.out, "outbox is empty (no automatic actions yet)")
		return nil
	}
	for _, en := range entries {
		ref := en.Ref
		switch en.Kind {
		case "slack":
			ref = en.Target
		case "kb":
			ref = "self-serve"
		}
		fmt.Fprintf(e.out, "%s  %-6s %-18s %s  %s\n", en.At.Format("15:04:05"), en.Kind, ref, en.Request.ItemID, en.Detail)
		fmt.Fprintf(e.out, "          why: %s\n", en.Request.Reason)
	}
	return nil
}

// ---- mcp -------------------------------------------------------------------

func (e *env) mcp(args []string) error {
	fs := e.flags("mcp")
	allow := fs.String("allow-write", "", "comma-separated write tools to enable: "+strings.Join(mcp.WriteToolNames(), ","))
	actor := fs.String("actor", "agent", "name recorded in the audit trail for write calls")
	if _, err := parseInterleaved(fs, args); err != nil {
		return err
	}
	allowed, err := mcp.ParseAllowWrite(*allow)
	if err != nil {
		return fmt.Errorf("%w: %v", errUsage, err)
	}
	p, err := e.open()
	if err != nil {
		return err
	}
	srv := &mcp.Server{Svc: p.Service, AllowWrite: allowed, Actor: *actor, Version: "0.4.0"}
	fmt.Fprintf(e.errOut, "icr mcp: serving on stdio (write tools enabled: %s)\n", describeAllowed(allowed))
	return srv.Serve(e.ctx, bufio.NewReader(e.stdin), e.out)
}

func describeAllowed(m map[string]bool) string {
	if len(m) == 0 {
		return "none — read-only"
	}
	var xs []string
	for k := range m {
		xs = append(xs, k)
	}
	sort.Strings(xs)
	return strings.Join(xs, ", ")
}

// ---- reset -----------------------------------------------------------------

func (e *env) reset(args []string) error {
	fs := e.flags("reset")
	yes := fs.Bool("yes", false, "confirm deletion of the data dir")
	if _, err := parseInterleaved(fs, args); err != nil {
		return err
	}
	if !*yes {
		return fmt.Errorf("%w: this deletes %s; re-run with -yes", errUsage, e.dataDir)
	}
	clean := filepath.Clean(e.dataDir)
	if clean == "." || clean == "/" || clean == ".." {
		return fmt.Errorf("refusing to delete %q", e.dataDir)
	}
	if err := os.RemoveAll(clean); err != nil {
		return err
	}
	fmt.Fprintf(e.out, "removed %s\n", clean)
	return nil
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func trunc(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
