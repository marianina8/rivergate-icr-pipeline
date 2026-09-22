package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	sessionCookie = "icr_session"
	sessionTTL    = 12 * time.Hour
)

// auth is a single shared-password login for demo access. Sessions are
// stateless HMAC-signed cookies ("<expiry>.<sig>"), so they work across
// Lambda instances; the signing key is derived from the password, so
// changing the password logs everyone out.
type auth struct {
	password []byte
	key      []byte
	secure   bool
	path     string // cookie path (the dashboard's base path)
	now      func() time.Time
}

func newAuth(password string, secure bool) *auth {
	k := sha256.Sum256([]byte("rivergate-icr-dashboard-session\x00" + password))
	return &auth{password: []byte(password), key: k[:], secure: secure, path: "/", now: time.Now}
}

func (a *auth) sign(exp int64) string {
	m := hmac.New(sha256.New, a.key)
	m.Write([]byte("v1|" + strconv.FormatInt(exp, 10)))
	return hex.EncodeToString(m.Sum(nil))
}

func (a *auth) token() (string, time.Time) {
	exp := a.now().Add(sessionTTL)
	return strconv.FormatInt(exp.Unix(), 10) + "." + a.sign(exp.Unix()), exp
}

func (a *auth) valid(tok string) bool {
	expS, sig, ok := strings.Cut(tok, ".")
	if !ok {
		return false
	}
	exp, err := strconv.ParseInt(expS, 10, 64)
	if err != nil || a.now().Unix() > exp {
		return false
	}
	return hmac.Equal([]byte(sig), []byte(a.sign(exp)))
}

func (a *auth) check(password string) bool {
	// Compare fixed-size digests so timing doesn't leak the length.
	x, y := sha256.Sum256([]byte(password)), sha256.Sum256(a.password)
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

func (a *auth) authed(r *http.Request) bool {
	c, err := r.Cookie(sessionCookie)
	return err == nil && a.valid(c.Value)
}

// requireAuth redirects unauthenticated requests to the login page.
func (s *Server) requireAuth(h http.Handler) http.Handler {
	if s.auth == nil {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/login", "/healthz":
			h.ServeHTTP(w, r)
			return
		}
		if s.auth.authed(r) {
			h.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "sign in required", http.StatusUnauthorized)
			return
		}
		http.Redirect(w, r, s.url("/login?next=")+url.QueryEscape(r.URL.RequestURI()), http.StatusSeeOther)
	})
}

type loginData struct {
	Page  page
	Next  string
	Error string
}

// safeNext only allows local paths, so the login form can't be used as an
// open redirect.
func safeNext(n string) string {
	if !strings.HasPrefix(n, "/") || strings.HasPrefix(n, "//") || strings.HasPrefix(n, "/\\") {
		return "/"
	}
	return n
}

func (s *Server) loginForm(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil || s.auth.authed(r) {
		http.Redirect(w, r, s.url("/"), http.StatusSeeOther)
		return
	}
	p := s.page("Sign in")
	p.Nav = false
	s.render(w, "login.html", loginData{Page: p, Next: safeNext(r.URL.Query().Get("next"))})
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		http.Redirect(w, r, s.url("/"), http.StatusSeeOther)
		return
	}
	next := safeNext(r.FormValue("next"))
	if !s.auth.check(r.FormValue("password")) {
		time.Sleep(400 * time.Millisecond) // slow down guessing
		p := s.page("Sign in")
		p.Nav = false
		w.WriteHeader(http.StatusUnauthorized)
		s.render(w, "login.html", loginData{Page: p, Next: next, Error: "That password isn't right."})
		return
	}
	tok, exp := s.auth.token()
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: tok, Path: s.auth.path, Expires: exp,
		HttpOnly: true, Secure: s.auth.secure, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, s.url(next), http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	secure, path := false, "/"
	if s.auth != nil {
		secure, path = s.auth.secure, s.auth.path
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: "", Path: path, MaxAge: -1, HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode})
	http.Redirect(w, r, s.url("/login"), http.StatusSeeOther)
}
