// Package dashboard is the human-in-the-loop web UI: submit a ticket, watch
// it get classified and routed, review the queue, approve or override. The
// same handler runs locally (cmd/dashboard) and hosted on Lambda
// (cmd/lambda/dashboard), optionally behind a shared password.
package dashboard

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
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

// Options configures the dashboard.
type Options struct {
	Svc *pipeline.Service
	// Actions returns the automatic-action feed (outbox files locally,
	// audit-trail derived against DynamoDB).
	Actions func(ctx context.Context) ([]router.OutboxEntry, error)
	// Reviewer pre-fills the reviewer name in forms.
	Reviewer string
	// Password, when set, is required to view any page (shared demo login).
	Password string
	// SecureCookie marks the session cookie Secure (set when served over HTTPS).
	SecureCookie bool
	// ProcessInline, when set, runs right after a submission (local mode:
	// drain the directory queue so there is no separate worker to start).
	ProcessInline func(ctx context.Context) error
	// Examples are offered on the submit page.
	Examples []Example
	// BasePath mounts the UI under a prefix, e.g. "/demos/rivergate" when it
	// is served through marian.online. Empty means the site root.
	BasePath string
	// SiteURL, when set, adds an "About this demo" link to the header
	// (e.g. /demos on marian.online).
	SiteURL string
	// AllowedOrigins lists extra origins (scheme://host) whose form posts are
	// accepted, e.g. the public site that proxies to the dashboard.
	AllowedOrigins []string
}

// Server is the dashboard HTTP handler.
type Server struct {
	opt  Options
	tpl  *template.Template
	auth *auth
}

// FileActions reads the local outbox files (local mode).
func FileActions(dir string) func(context.Context) ([]router.OutboxEntry, error) {
	return func(context.Context) ([]router.OutboxEntry, error) { return router.ReadOutbox(dir) }
}

// AuditActions rebuilds the action feed from item audit trails (DynamoDB mode).
func AuditActions(svc *pipeline.Service) func(context.Context) ([]router.OutboxEntry, error) {
	return func(ctx context.Context) ([]router.OutboxEntry, error) {
		items, err := svc.List(ctx, store.Filter{})
		if err != nil {
			return nil, err
		}
		return pipeline.ActionsFromItems(items), nil
	}
}

// New builds the dashboard.
func New(opt Options) (*Server, error) {
	if opt.Svc == nil || opt.Actions == nil {
		return nil, errors.New("dashboard: Svc and Actions are required")
	}
	if opt.Reviewer == "" {
		opt.Reviewer = "reviewer"
	}
	opt.BasePath = "/" + strings.Trim(opt.BasePath, "/")
	if opt.BasePath == "/" {
		opt.BasePath = ""
	}
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
	s := &Server{opt: opt, tpl: tpl}
	if opt.Password != "" {
		s.auth = newAuth(opt.Password, opt.SecureCookie)
		s.auth.path = opt.BasePath + "/"
	}
	return s, nil
}

// Handler returns the routed, authenticated handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.index)
	mux.HandleFunc("GET /items/{id}", s.item)
	mux.HandleFunc("POST /items/{id}/approve", s.sameOrigin(s.approve))
	mux.HandleFunc("POST /items/{id}/override", s.sameOrigin(s.override))
	mux.HandleFunc("GET /submit", s.submitForm)
	mux.HandleFunc("POST /submit", s.sameOrigin(s.submit))
	mux.HandleFunc("GET /login", s.loginForm)
	mux.HandleFunc("POST /login", s.sameOrigin(s.login))
	mux.HandleFunc("POST /logout", s.sameOrigin(s.logout))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	h := securityHeaders(s.requireAuth(mux))
	if s.opt.BasePath == "" {
		return h
	}
	outer := http.NewServeMux()
	outer.Handle(s.opt.BasePath+"/", http.StripPrefix(s.opt.BasePath, h))
	outer.HandleFunc(s.opt.BasePath, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, s.opt.BasePath+"/", http.StatusMovedPermanently)
	})
	return outer
}

// url builds a link/redirect target under the base path.
func (s *Server) url(p string) string { return s.opt.BasePath + p }

// page is the data every template's header needs.
type page struct {
	Base    string
	SiteURL string
	Company string
	Title   string
	Refresh int
	Nav     bool
	Auth    bool
}

func (s *Server) page(title string) page {
	return page{Base: s.opt.BasePath, SiteURL: s.opt.SiteURL, Company: s.opt.Svc.Cfg.Instance.Company, Title: title, Nav: true, Auth: s.auth != nil}
}

type indexData struct {
	Page   page
	Stats  pipeline.Stats
	Review []store.Item
	All    []store.Item
	Outbox []router.OutboxEntry
	Flash  string
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	all, err := s.opt.Svc.List(ctx, store.Filter{})
	if err != nil {
		s.fail(w, err)
		return
	}
	var review []store.Item
	for _, it := range all {
		if it.NeedsReview {
			review = append(review, it)
		}
	}
	st, err := s.opt.Svc.Stats(ctx)
	if err != nil {
		s.fail(w, err)
		return
	}
	ob, err := s.opt.Actions(ctx)
	if err != nil {
		s.fail(w, err)
		return
	}
	for i, j := 0, len(ob)-1; i < j; i, j = i+1, j-1 { // newest first
		ob[i], ob[j] = ob[j], ob[i]
	}
	// newest tickets first in the "All" table
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	s.render(w, "index.html", indexData{Page: s.page("Review queue"), Stats: st, Review: review, All: all, Outbox: ob, Flash: r.URL.Query().Get("msg")})
}

type itemData struct {
	Page       page
	Item       store.Item
	Categories []string
	Priorities []string
	Reviewer   string
	Flash      string
	Error      string
}

func (s *Server) item(w http.ResponseWriter, r *http.Request) {
	it, err := s.opt.Svc.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	p := s.page(it.ID)
	if it.Status == store.StatusReceived && time.Since(it.CreatedAt) < 5*time.Minute {
		p.Refresh = 2 // still being classified: poll until the worker finishes
	}
	s.render(w, "item.html", itemData{
		Page: p, Item: it,
		Categories: s.opt.Svc.Cfg.CategoryNames(), Priorities: s.opt.Svc.Cfg.PriorityNames(),
		Reviewer: s.opt.Reviewer, Flash: r.URL.Query().Get("msg"), Error: r.URL.Query().Get("err"),
	})
}

func (s *Server) approve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reviewer := strings.TrimSpace(r.FormValue("reviewer"))
	if reviewer == "" {
		s.back(w, r, id, "", "reviewer name is required")
		return
	}
	it, err := s.opt.Svc.Approve(r.Context(), id, "dashboard:"+reviewer, r.FormValue("note"))
	s.after(w, r, id, it, err, "Approved")
}

func (s *Server) override(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reviewer := strings.TrimSpace(r.FormValue("reviewer"))
	if reviewer == "" {
		s.back(w, r, id, "", "reviewer name is required")
		return
	}
	it, err := s.opt.Svc.Override(r.Context(), id, "dashboard:"+reviewer, r.FormValue("category"), r.FormValue("priority"), r.FormValue("note"))
	s.after(w, r, id, it, err, "Overridden")
}

func (s *Server) after(w http.ResponseWriter, r *http.Request, id string, it store.Item, err error, verb string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		http.NotFound(w, r)
	case errors.Is(err, pipeline.ErrInvalidLabel), errors.Is(err, pipeline.ErrNotClassified):
		s.back(w, r, id, "", err.Error())
	case err != nil && it.ID == "":
		s.fail(w, err)
	case err != nil:
		s.back(w, r, id, "", fmt.Sprintf("%s, but an action failed: %v", verb, err))
	default:
		http.Redirect(w, r, s.url("/?msg=")+url.QueryEscape(fmt.Sprintf("%s %s → %s queue", verb, id, it.Queue)), http.StatusSeeOther)
	}
}

func (s *Server) back(w http.ResponseWriter, r *http.Request, id, msg, errMsg string) {
	q := url.Values{}
	if msg != "" {
		q.Set("msg", msg)
	}
	if errMsg != "" {
		q.Set("err", errMsg)
	}
	http.Redirect(w, r, s.url("/items/"+url.PathEscape(id)+"?"+q.Encode()), http.StatusSeeOther)
}

// sameOrigin rejects cross-site form posts (CSRF guard, alongside the
// SameSite=Lax session cookie).
func (s *Server) sameOrigin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" && !s.originAllowed(o, r.Host) {
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
		h(w, r)
	}
}

func securityHeaders(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	s.renderStatus(w, http.StatusOK, name, data)
}

// renderStatus sets the Content-Type *before* the status line; headers set
// after WriteHeader are dropped (API Gateway then shows the HTML as text).
func (s *Server) renderStatus(w http.ResponseWriter, code int, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	if err := s.tpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	slog.Error("dashboard request failed", "err", err) // details go to logs, not to the browser
	http.Error(w, "Something went wrong. Please try again.", http.StatusInternalServerError)
}

func (s *Server) originAllowed(origin, host string) bool {
	if u, err := url.Parse(origin); err == nil && u.Host == host {
		return true
	}
	for _, a := range s.opt.AllowedOrigins {
		if strings.EqualFold(strings.TrimRight(a, "/"), origin) {
			return true
		}
	}
	return false
}
