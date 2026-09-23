package dashboard

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

type harness struct {
	s   *Server
	h   http.Handler
	p   *pipeline.Local
	ids map[string]string
}

func setup(t *testing.T, password string, seed bool) harness {
	t.Helper()
	data := t.TempDir()
	ctx := context.Background()
	p, err := pipeline.OpenLocal(ctx, testutil.ConfigPath(t), data, "mock", nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{}
	if seed {
		for _, f := range testutil.Fixtures(t) {
			tk, _, err := p.Ingest(ctx, f.Payload, "test")
			if err != nil {
				t.Fatal(err)
			}
			ids[f.Name] = tk.ID
		}
		if _, err := p.RunOnce(ctx, 20); err != nil {
			t.Fatal(err)
		}
	}
	ex, err := LoadExamples(filepath.Join(testutil.RepoRoot(t), "demo", "tickets"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Options{
		Svc: p.Service, Actions: FileActions(pipeline.OutboxDir(data)), Reviewer: "tester", Password: password,
		Examples: ex,
		ProcessInline: func(ctx context.Context) error {
			_, err := p.RunOnce(ctx, 10)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return harness{s: s, h: s.Handler(), p: p, ids: ids}
}

func (h harness) do(req *http.Request, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	for _, c := range cookies {
		req.AddCookie(c)
	}
	rec := httptest.NewRecorder()
	h.h.ServeHTTP(rec, req)
	return rec
}

func postForm(path string, form url.Values, origin string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	return req
}

func TestIndexShowsReviewQueueAndActions(t *testing.T) {
	h := setup(t, "", true)
	rec := h.do(httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 200 {
		t.Fatalf("GET / = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"Human review queue (4)", h.ids["09-chat-vague-asap.json"], "#rivergate-oncall", "ENG-BACKLOG", "Rivergate Technologies", "Submit a ticket"} {
		if !strings.Contains(body, want) {
			t.Errorf("index missing %q", want)
		}
	}
	if rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Error("security headers missing")
	}
}

func TestItemPageAndApprove(t *testing.T) {
	h := setup(t, "", true)
	id := h.ids["10-email-late-notifications.json"]
	rec := h.do(httptest.NewRequest(http.MethodGet, "/items/"+id, nil))
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "Approve") || !strings.Contains(rec.Body.String(), "Audit trail") {
		t.Fatalf("item page = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), `http-equiv="refresh"`) {
		t.Error("a classified item must not auto-refresh")
	}
	rec = h.do(postForm("/items/"+id+"/approve", url.Values{"reviewer": {"sam"}, "note": {"confirmed"}}, ""))
	if rec.Code != http.StatusSeeOther || !strings.Contains(rec.Header().Get("Location"), "bug") {
		t.Fatalf("approve = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	it, _ := h.p.Get(context.Background(), id)
	if it.NeedsReview || it.Review == nil || it.Review.Reviewer != "dashboard:sam" {
		t.Errorf("after approve: %+v", it.Review)
	}
}

func TestOverrideValidationAndCSRF(t *testing.T) {
	h := setup(t, "", true)
	id := h.ids["08-webform-partnership.json"]
	if rec := h.do(postForm("/items/"+id+"/override", url.Values{"reviewer": {"sam"}, "category": {"refunds"}}, "")); !strings.Contains(rec.Header().Get("Location"), "err=") {
		t.Errorf("invalid category should bounce back with an error: %s", rec.Header().Get("Location"))
	}
	if rec := h.do(postForm("/items/"+id+"/approve", url.Values{"reviewer": {"x"}}, "https://evil.example")); rec.Code != http.StatusForbidden {
		t.Errorf("cross-origin post = %d, want 403", rec.Code)
	}
	if rec := h.do(postForm("/items/"+id+"/override", url.Values{"reviewer": {"sam"}, "category": {"feature_request"}, "priority": {"low"}}, "http://example.com")); rec.Code != http.StatusSeeOther {
		t.Errorf("same-origin override = %d", rec.Code)
	}
	it, _ := h.p.Get(context.Background(), id)
	if it.Queue != "product" || it.Review.Outcome != "overridden" {
		t.Errorf("after override: queue=%s", it.Queue)
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/items/RG-NOPE0000", nil)); rec.Code != http.StatusNotFound {
		t.Errorf("unknown item = %d", rec.Code)
	}
}

// ---- submit ----

func TestSubmitFormFieldsEachChannel(t *testing.T) {
	h := setup(t, "", false)
	for _, ch := range []string{"webform", "email", "chat"} {
		form := url.Values{"mode": {"form"}, "channel": {ch}, "name": {"Alex Example"}, "email": {"alex@fictional.example"},
			"company": {"Fictional Co."}, "subject": {"Invoice question " + ch}, "message": {"We were charged twice on our invoice, please refund the duplicate."}}
		rec := h.do(postForm("/submit", form, ""))
		loc := rec.Header().Get("Location")
		if rec.Code != http.StatusSeeOther || !strings.HasPrefix(loc, "/items/RG-") {
			t.Fatalf("%s: submit = %d %s %s", ch, rec.Code, loc, rec.Body)
		}
		id := strings.TrimPrefix(strings.SplitN(loc, "?", 2)[0], "/items/")
		it, err := h.p.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if it.Ticket.Channel != ch || it.Ticket.Source != "dashboard" || it.Status == store.StatusReceived {
			t.Errorf("%s: got channel=%s source=%s status=%s", ch, it.Ticket.Channel, it.Ticket.Source, it.Status)
		}
		if it.Classification == nil || it.Classification.Category != "billing" {
			t.Errorf("%s: expected processed billing ticket, got %+v", ch, it.Classification)
		}
	}
}

func TestSubmitPastedAndUploadedJSON(t *testing.T) {
	h := setup(t, "", false)
	ex := h.s.opt.Examples[0] // 01 outage
	rec := h.do(postForm("/submit", url.Values{"mode": {"json"}, "json": {string(ex.Payload)}}, ""))
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("paste: %d %s", rec.Code, rec.Body)
	}
	// Same ticket again -> duplicate message, no second item.
	rec = h.do(postForm("/submit", url.Values{"mode": {"json"}, "json": {string(ex.Payload)}}, ""))
	if !strings.Contains(rec.Header().Get("Location"), "already+submitted") {
		t.Errorf("duplicate: %s", rec.Header().Get("Location"))
	}
	// Multipart upload of another example.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("mode", "json")
	fw, _ := mw.CreateFormFile("file", h.s.opt.Examples[5].Name)
	_, _ = fw.Write(h.s.opt.Examples[5].Payload)
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/submit", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if rec := h.do(req); rec.Code != http.StatusSeeOther {
		t.Fatalf("upload: %d %s", rec.Code, rec.Body)
	}
	items, _ := h.p.List(context.Background(), store.Filter{})
	if len(items) != 2 {
		t.Errorf("want 2 items, got %d", len(items))
	}
}

func TestSubmitRejectsBadInput(t *testing.T) {
	h := setup(t, "", false)
	for name, form := range map[string]url.Values{
		"bad json":      {"mode": {"json"}, "json": {"{nope"}},
		"wrong channel": {"mode": {"json"}, "json": {`{"channel":"fax","body":"x"}`}},
		"empty message": {"mode": {"form"}, "channel": {"webform"}, "message": {"  "}},
		"empty json":    {"mode": {"json"}, "json": {""}},
	} {
		rec := h.do(postForm("/submit", form, ""))
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "Couldn&#39;t submit") {
			t.Errorf("%s: %d", name, rec.Code)
		}
	}
	if rec := h.do(postForm("/submit", url.Values{"mode": {"json"}, "json": {strings.Repeat("x", MaxSubmitBytes*2)}}, "")); rec.Code != http.StatusBadRequest {
		t.Errorf("oversized submission = %d", rec.Code)
	}
}

func TestSubmitPageExamplesAndPendingRefresh(t *testing.T) {
	h := setup(t, "", false)
	rec := h.do(httptest.NewRequest(http.MethodGet, "/submit?example=03-chat-double-charge.json", nil))
	body := rec.Body.String()
	if rec.Code != 200 || !strings.Contains(body, "charged twice") || !strings.Contains(body, "10-email-late-notifications.json") {
		t.Errorf("submit page should list examples and prefill the chosen one")
	}
	// An item still waiting on the worker auto-refreshes.
	h.s.opt.ProcessInline = nil
	tk, _, err := h.p.Ingest(context.Background(), h.s.opt.Examples[1].Payload, "dashboard")
	if err != nil {
		t.Fatal(err)
	}
	rec = h.do(httptest.NewRequest(http.MethodGet, "/items/"+tk.ID, nil))
	if !strings.Contains(rec.Body.String(), `http-equiv="refresh"`) || !strings.Contains(rec.Body.String(), "Classifying") {
		t.Error("pending item should show the classifying banner and auto-refresh")
	}
}

// ---- auth ----

func TestPasswordProtectsEveryPage(t *testing.T) {
	h := setup(t, "open-sesame", true)
	for _, path := range []string{"/", "/submit", "/items/" + h.ids["01-webform-outage-503.json"]} {
		rec := h.do(httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/login?next=") {
			t.Errorf("GET %s without login = %d %s", path, rec.Code, rec.Header().Get("Location"))
		}
	}
	if rec := h.do(postForm("/submit", url.Values{"mode": {"form"}, "message": {"x"}}, "")); rec.Code != http.StatusUnauthorized {
		t.Errorf("POST without login = %d", rec.Code)
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/healthz", nil)); rec.Code != 200 {
		t.Errorf("healthz should stay open: %d", rec.Code)
	}

	rec := h.do(postForm("/login", url.Values{"password": {"wrong"}, "next": {"/submit"}}, ""))
	if rec.Code != http.StatusUnauthorized || len(rec.Result().Cookies()) != 0 {
		t.Fatalf("wrong password = %d", rec.Code)
	}
	rec = h.do(postForm("/login", url.Values{"password": {"open-sesame"}, "next": {"/submit"}}, ""))
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/submit" {
		t.Fatalf("login = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	cookie := rec.Result().Cookies()[0]
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode {
		t.Error("session cookie must be HttpOnly and SameSite=Lax")
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/", nil), cookie); rec.Code != 200 || !strings.Contains(rec.Body.String(), "Log out") {
		t.Errorf("authed GET / = %d", rec.Code)
	}
	forged := &http.Cookie{Name: sessionCookie, Value: "99999999999.deadbeef"}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/", nil), forged); rec.Code != http.StatusSeeOther {
		t.Error("forged cookie accepted")
	}
	// Open redirect is refused.
	rec = h.do(postForm("/login", url.Values{"password": {"open-sesame"}, "next": {"//evil.example/x"}}, ""))
	if rec.Header().Get("Location") != "/" {
		t.Errorf("open redirect: %s", rec.Header().Get("Location"))
	}
	// Logout clears the cookie.
	rec = h.do(postForm("/logout", nil, ""), cookie)
	if c := rec.Result().Cookies(); len(c) != 1 || c[0].MaxAge >= 0 {
		t.Error("logout should expire the cookie")
	}
}

func TestSessionExpiry(t *testing.T) {
	a := newAuth("pw", true)
	tok, _ := a.token("abc123")
	if !a.valid(tok) {
		t.Fatal("fresh token invalid")
	}
	a.now = func() time.Time { return time.Now().Add(sessionTTL + time.Minute) }
	if a.valid(tok) {
		t.Error("expired token accepted")
	}
	if newAuth("other", true).valid(tok) {
		t.Error("token valid under a different password")
	}
}

// Served through marian.online at /demos/rivergate via a proxy rewrite.
func TestBasePathBehindProxy(t *testing.T) {
	base := setup(t, "", true)
	s, err := New(Options{Svc: base.s.opt.Svc, Actions: base.s.opt.Actions, Password: "pw", SecureCookie: true,
		BasePath: "/demos/rivergate/", AllowedOrigins: []string{"https://marian.online"}, Examples: base.s.opt.Examples,
		ProcessInline: base.s.opt.ProcessInline})
	if err != nil {
		t.Fatal(err)
	}
	h := harness{s: s, h: s.Handler(), p: base.p, ids: base.ids}

	if rec := h.do(httptest.NewRequest(http.MethodGet, "/demos/rivergate", nil)); rec.Header().Get("Location") != "/demos/rivergate/" {
		t.Errorf("bare base path should redirect to trailing slash: %s", rec.Header().Get("Location"))
	}
	rec := h.do(httptest.NewRequest(http.MethodGet, "/demos/rivergate/submit", nil))
	if loc := rec.Header().Get("Location"); loc != "/demos/rivergate/login?next=%2Fsubmit" {
		t.Errorf("login redirect = %s", loc)
	}
	rec = h.do(postForm("/demos/rivergate/login", url.Values{"password": {"pw"}, "next": {"/submit"}}, "https://marian.online"))
	if rec.Header().Get("Location") != "/demos/rivergate/submit" {
		t.Fatalf("post-login redirect = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	c := rec.Result().Cookies()[0]
	if c.Path != "/demos/rivergate/" || !c.Secure {
		t.Errorf("cookie path=%s secure=%v", c.Path, c.Secure)
	}
	rec = h.do(httptest.NewRequest(http.MethodGet, "/demos/rivergate/", nil), c)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `href="/demos/rivergate/submit"`) || !strings.Contains(rec.Body.String(), `href="/demos/rivergate/items/`) {
		t.Errorf("links should carry the base path (code %d)", rec.Code)
	}
	form := url.Values{"mode": {"form"}, "channel": {"webform"}, "message": {"How do I export a board to CSV?"}, "subject": {"CSV export"}}
	rec = h.do(postForm("/demos/rivergate/submit", form, "https://marian.online"), c)
	if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/demos/rivergate/items/RG-") {
		t.Errorf("submit via proxy origin = %d %s", rec.Code, rec.Header().Get("Location"))
	}
	if rec := h.do(postForm("/demos/rivergate/submit", form, "https://evil.example"), c); rec.Code != http.StatusForbidden {
		t.Errorf("foreign origin = %d", rec.Code)
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/somewhere-else", nil)); rec.Code != http.StatusNotFound {
		t.Errorf("paths outside the base should 404: %d", rec.Code)
	}
}

// ---- sandboxes ----

func login(t *testing.T, h harness, pw string) *http.Cookie {
	t.Helper()
	rec := h.do(postForm("/login", url.Values{"password": {pw}}, ""))
	if rec.Code != http.StatusSeeOther || len(rec.Result().Cookies()) != 1 {
		t.Fatalf("login = %d", rec.Code)
	}
	return rec.Result().Cookies()[0]
}

func sandboxHarness(t *testing.T) harness {
	t.Helper()
	base := setup(t, "", false)
	s, err := New(Options{Svc: base.s.opt.Svc, Actions: AuditActions(base.s.opt.Svc), Password: "pw", Sandboxes: true,
		Examples: base.s.opt.Examples, ProcessInline: base.s.opt.ProcessInline})
	if err != nil {
		t.Fatal(err)
	}
	return harness{s: s, h: s.Handler(), p: base.p}
}

func TestSandboxesIsolateVisitors(t *testing.T) {
	h := sandboxHarness(t)
	alice, bob := login(t, h, "pw"), login(t, h, "pw")

	// Each sandbox starts with its own 10 classified example tickets.
	for name, c := range map[string]*http.Cookie{"alice": alice, "bob": bob} {
		rec := h.do(httptest.NewRequest(http.MethodGet, "/", nil), c)
		body := rec.Body.String()
		if rec.Code != 200 || !strings.Contains(body, "Human review queue (4)") || !strings.Contains(body, "private sandbox") {
			t.Errorf("%s: sandbox not seeded/labelled (code %d)", name, rec.Code)
		}
	}
	all, _ := h.p.List(context.Background(), store.Filter{})
	if len(all) != 20 {
		t.Fatalf("want 10 seeded items per sandbox (20), got %d", len(all))
	}

	// Alice submits a ticket; Bob can't see, open or act on it.
	form := url.Values{"mode": {"form"}, "channel": {"webform"}, "subject": {"Alice only"}, "message": {"Private question about our invoice and a refund."}}
	rec := h.do(postForm("/submit", form, ""), alice)
	loc := rec.Header().Get("Location")
	id := strings.TrimPrefix(strings.SplitN(loc, "?", 2)[0], "/items/")
	if rec.Code != http.StatusSeeOther || !strings.HasPrefix(id, "RG-") {
		t.Fatalf("alice submit = %d %s", rec.Code, loc)
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/items/"+id, nil), alice); rec.Code != 200 {
		t.Errorf("alice can't open her own ticket: %d", rec.Code)
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/items/"+id, nil), bob); rec.Code != http.StatusNotFound {
		t.Errorf("bob opened alice's ticket: %d", rec.Code)
	}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/", nil), bob); strings.Contains(rec.Body.String(), "Alice only") || strings.Contains(rec.Body.String(), id) {
		t.Error("alice's ticket visible in bob's queue")
	}
	if rec := h.do(postForm("/items/"+id+"/approve", url.Values{"reviewer": {"bob"}}, ""), bob); rec.Code != http.StatusNotFound {
		t.Errorf("bob approved alice's ticket: %d", rec.Code)
	}
	it, _ := h.p.Get(context.Background(), id)
	if it.Review != nil || it.ExpiresAt == nil || time.Until(*it.ExpiresAt) > 25*time.Hour {
		t.Errorf("item should be untouched and expire within 24h: review=%v exp=%v", it.Review, it.ExpiresAt)
	}

	// A forged cookie that swaps in bob's workspace fails the signature check.
	aws, _ := h.s.auth.parse(alice.Value)
	bws, _ := h.s.auth.parse(bob.Value)
	forged := &http.Cookie{Name: sessionCookie, Value: strings.Replace(bob.Value, "."+bws+".", "."+aws+".", 1)}
	if rec := h.do(httptest.NewRequest(http.MethodGet, "/items/"+id, nil), forged); rec.Code != http.StatusSeeOther {
		t.Errorf("forged workspace cookie accepted: %d", rec.Code)
	}
}

func TestSandboxesRequirePassword(t *testing.T) {
	base := setup(t, "", false)
	if _, err := New(Options{Svc: base.s.opt.Svc, Actions: base.s.opt.Actions, Sandboxes: true}); err == nil {
		t.Error("sandboxes without a password should be refused")
	}
}
