package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

func setup(t *testing.T) (*server, map[string]string) {
	t.Helper()
	data := t.TempDir()
	p, err := pipeline.OpenLocal(context.Background(), testutil.ConfigPath(t), data, "mock", nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	for _, f := range testutil.Fixtures(t) {
		tk, _, err := p.Ingest(context.Background(), f.Payload, "test")
		if err != nil {
			t.Fatal(err)
		}
		ids[f.Name] = tk.ID
	}
	if _, err := p.RunOnce(context.Background(), 20); err != nil {
		t.Fatal(err)
	}
	s, err := newServer(p.Service, fileActions(pipeline.OutboxDir(data)), "tester")
	if err != nil {
		t.Fatal(err)
	}
	return s, ids
}

func TestIndexShowsReviewQueueAndActions(t *testing.T) {
	s, ids := setup(t)
	rec := httptest.NewRecorder()
	s.routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 200 {
		t.Fatalf("GET / = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Human review queue (4)", ids["09-chat-vague-asap.json"], "#rivergate-oncall", "ENG-BACKLOG", "Rivergate Technologies"} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q", want)
		}
	}
}

func TestItemPageAndApprove(t *testing.T) {
	s, ids := setup(t)
	h := s.routes()
	id := ids["10-email-late-notifications.json"]

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/items/"+id, nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Approve") || !strings.Contains(rec.Body.String(), "Audit trail") {
		t.Fatalf("item page = %d", rec.Code)
	}

	form := url.Values{"reviewer": {"sam"}, "note": {"confirmed"}}
	req := httptest.NewRequest(http.MethodPost, "/items/"+id+"/approve", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "bug") {
		t.Fatalf("approve = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	it, _ := s.svc.Get(context.Background(), id)
	if it.NeedsReview || it.Review == nil || it.Review.Reviewer != "dashboard:sam" {
		t.Errorf("after approve: %+v", it.Review)
	}
}

func TestOverrideValidationAndCSRF(t *testing.T) {
	s, ids := setup(t)
	h := s.routes()
	id := ids["08-webform-partnership.json"]

	post := func(path string, form url.Values, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	if rec := post("/items/"+id+"/override", url.Values{"reviewer": {"sam"}, "category": {"refunds"}}, ""); !strings.Contains(rec.Header().Get("Location"), "err=") {
		t.Errorf("invalid category should bounce back with an error: %s", rec.Header().Get("Location"))
	}
	if rec := post("/items/"+id+"/override", url.Values{"category": {"other"}}, ""); !strings.Contains(rec.Header().Get("Location"), "reviewer") {
		t.Errorf("missing reviewer should bounce back: %s", rec.Header().Get("Location"))
	}
	if rec := post("/items/"+id+"/approve", url.Values{"reviewer": {"x"}}, "https://evil.example"); rec.Code != http.StatusForbidden {
		t.Errorf("cross-origin post = %d, want 403", rec.Code)
	}
	if rec := post("/items/"+id+"/override", url.Values{"reviewer": {"sam"}, "category": {"feature_request"}, "priority": {"low"}}, "http://example.com"); rec.Code != http.StatusSeeOther {
		t.Errorf("same-origin override = %d", rec.Code)
	}
	it, _ := s.svc.Get(context.Background(), id)
	if it.Queue != "product" || it.Review.Outcome != "overridden" {
		t.Errorf("after override: queue=%s", it.Queue)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/items/RG-NOPE0000", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown item = %d", rec.Code)
	}
}
