package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

// server is the human-review dashboard: the review queue, item detail with
// the model's classification and reasoning, and approve/override controls.
type server struct {
	svc      *pipeline.Service
	actions  func(ctx context.Context) ([]router.OutboxEntry, error)
	reviewer string // default reviewer name pre-filled in forms
	tpl      *template.Template
}

// fileActions reads the local outbox files (local mode).
func fileActions(dir string) func(context.Context) ([]router.OutboxEntry, error) {
	return func(context.Context) ([]router.OutboxEntry, error) { return router.ReadOutbox(dir) }
}

// auditActions rebuilds the action feed from item audit trails (DynamoDB mode).
func auditActions(svc *pipeline.Service) func(context.Context) ([]router.OutboxEntry, error) {
	return func(ctx context.Context) ([]router.OutboxEntry, error) {
		items, err := svc.List(ctx, store.Filter{})
		if err != nil {
			return nil, err
		}
		return pipeline.ActionsFromItems(items), nil
	}
}

func newServer(svc *pipeline.Service, actions func(context.Context) ([]router.OutboxEntry, error), reviewer string) (*server, error) {
	funcs := template.FuncMap{
		"pct":  func(f float64) string { return fmt.Sprintf("%.0f%%", f*100) },
		"when": func(t time.Time) string { return t.Local().Format("Jan 2 15:04:05") },
		"confClass": func(f float64) string {
			switch {
			case f >= 0.75:
				return "hi"
			case f >= 0.6:
				return "mid"
			default:
				return "lo"
			}
		},
		"join": strings.Join,
	}
	tpl, err := template.New("").Funcs(funcs).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &server{svc: svc, actions: actions, reviewer: reviewer, tpl: tpl}, nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /items/{id}", s.item)
	mux.HandleFunc("POST /items/{id}/approve", s.sameOrigin(s.approve))
	mux.HandleFunc("POST /items/{id}/override", s.sameOrigin(s.override))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	return mux
}

type indexData struct {
	Company string
	Stats   pipeline.Stats
	Review  []store.Item
	All     []store.Item
	Outbox  []router.OutboxEntry
	Flash   string
}

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	review, err := s.svc.ReviewQueue(ctx)
	if err != nil {
		s.fail(w, err)
		return
	}
	all, err := s.svc.List(ctx, store.Filter{})
	if err != nil {
		s.fail(w, err)
		return
	}
	st, err := s.svc.Stats(ctx)
	if err != nil {
		s.fail(w, err)
		return
	}
	ob, err := s.actions(ctx)
	if err != nil {
		s.fail(w, err)
		return
	}
	// newest first for the activity feed
	for i, j := 0, len(ob)-1; i < j; i, j = i+1, j-1 {
		ob[i], ob[j] = ob[j], ob[i]
	}
	s.render(w, "index.html", indexData{Company: s.svc.Cfg.Instance.Company, Stats: st, Review: review, All: all, Outbox: ob, Flash: r.URL.Query().Get("msg")})
}

type itemData struct {
	Company    string
	Item       store.Item
	Categories []string
	Priorities []string
	Reviewer   string
	Flash      string
	Error      string
}

func (s *server) item(w http.ResponseWriter, r *http.Request) {
	it, err := s.svc.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	s.render(w, "item.html", itemData{
		Company: s.svc.Cfg.Instance.Company, Item: it,
		Categories: s.svc.Cfg.CategoryNames(), Priorities: s.svc.Cfg.PriorityNames(),
		Reviewer: s.reviewer, Flash: r.URL.Query().Get("msg"), Error: r.URL.Query().Get("err"),
	})
}

func (s *server) approve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reviewer := strings.TrimSpace(r.FormValue("reviewer"))
	if reviewer == "" {
		s.back(w, r, id, "", "reviewer name is required")
		return
	}
	it, err := s.svc.Approve(r.Context(), id, "dashboard:"+reviewer, r.FormValue("note"))
	s.after(w, r, id, it, err, "Approved")
}

func (s *server) override(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reviewer := strings.TrimSpace(r.FormValue("reviewer"))
	if reviewer == "" {
		s.back(w, r, id, "", "reviewer name is required")
		return
	}
	it, err := s.svc.Override(r.Context(), id, "dashboard:"+reviewer, r.FormValue("category"), r.FormValue("priority"), r.FormValue("note"))
	s.after(w, r, id, it, err, "Overridden")
}

func (s *server) after(w http.ResponseWriter, r *http.Request, id string, it store.Item, err error, verb string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.NotFound(w, r)
	case errors.Is(err, pipeline.ErrInvalidLabel), errors.Is(err, pipeline.ErrNotClassified):
		s.back(w, r, id, "", err.Error())
	case err != nil && it.ID == "":
		s.fail(w, err)
	case err != nil:
		// Review recorded but an action failed; show it on the item page.
		s.back(w, r, id, "", fmt.Sprintf("%s, but an action failed: %v", verb, err))
	default:
		http.Redirect(w, r, "/?msg="+url.QueryEscape(fmt.Sprintf("%s %s → %s queue", verb, id, it.Queue)), http.StatusSeeOther)
	}
}

func (s *server) back(w http.ResponseWriter, r *http.Request, id, msg, errMsg string) {
	q := url.Values{}
	if msg != "" {
		q.Set("msg", msg)
	}
	if errMsg != "" {
		q.Set("err", errMsg)
	}
	http.Redirect(w, r, "/items/"+url.PathEscape(id)+"?"+q.Encode(), http.StatusSeeOther)
}

// sameOrigin rejects cross-site form posts (a minimal CSRF guard; the
// dashboard binds to localhost and has no auth in the local demo).
func (s *server) sameOrigin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" {
			u, err := url.Parse(o)
			if err != nil || u.Host != r.Host {
				http.Error(w, "cross-origin request rejected", http.StatusForbidden)
				return
			}
		}
		h(w, r)
	}
}

func (s *server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) fail(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
