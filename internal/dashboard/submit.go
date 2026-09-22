package dashboard

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
)

// MaxSubmitBytes caps a submission (form, pasted JSON or uploaded file).
const MaxSubmitBytes = 64 << 10

// Example is a ready-made ticket offered on the submit page.
type Example struct {
	Name    string // file name, used as the form value
	Label   string // "03 · chat · Hi, we were charged twice…"
	Payload []byte
}

// LoadExamples reads *.json ticket fixtures from dir (e.g. demo/tickets).
// A missing dir yields no examples rather than an error.
func LoadExamples(dir string) ([]Example, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	var out []Example
	for _, m := range matches {
		b, err := os.ReadFile(m)
		if err != nil {
			return nil, err
		}
		name := filepath.Base(m)
		label := strings.TrimSuffix(name, ".json")
		if t, err := ingest.Normalize(b, "example", zeroTime); err == nil {
			label = fmt.Sprintf("%s · %s · %s", strings.SplitN(name, "-", 2)[0], t.Channel, t.Subject)
		}
		out = append(out, Example{Name: name, Label: label, Payload: b})
	}
	return out, nil
}

type submitData struct {
	Page     page
	Examples []Example
	Selected string
	JSON     string
	Error    string
}

func (s *Server) submitForm(w http.ResponseWriter, r *http.Request) {
	d := submitData{Page: s.page("Submit a ticket"), Examples: s.opt.Examples, Selected: r.URL.Query().Get("example")}
	for _, ex := range s.opt.Examples {
		if ex.Name == d.Selected {
			d.JSON = string(ex.Payload)
		}
	}
	if d.JSON == "" {
		d.JSON = exampleSkeleton
	}
	s.render(w, "submit.html", d)
}

const exampleSkeleton = `{
  "channel": "webform",
  "name": "Alex Example",
  "email": "alex@fictional-co.example",
  "company": "Fictional Co.",
  "topic": "Short summary of the problem",
  "message": "Describe the issue here."
}`

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	if s.opt.Svc.Queue == nil {
		s.submitError(w, r, errors.New("this dashboard has no queue configured (start it with -queue-url)"))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxSubmitBytes+4096)
	payload, err := submissionPayload(r)
	if err != nil {
		s.submitError(w, r, err)
		return
	}
	ctx := r.Context()
	t, dup, err := s.opt.Svc.Ingest(ctx, payload, "dashboard")
	if err != nil {
		if errors.Is(err, ingest.ErrInvalid) {
			s.submitError(w, r, err)
			return
		}
		s.fail(w, err)
		return
	}
	msg := "Submitted " + t.ID
	if dup {
		msg = "This exact ticket was already submitted; showing the existing result."
	} else if s.opt.ProcessInline != nil {
		if err := s.opt.ProcessInline(ctx); err != nil {
			s.fail(w, err)
			return
		}
	}
	http.Redirect(w, r, s.url("/items/"+url.PathEscape(t.ID)+"?msg="+url.QueryEscape(msg)), http.StatusSeeOther)
}

func (s *Server) submitError(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(http.StatusBadRequest)
	d := submitData{Page: s.page("Submit a ticket"), Examples: s.opt.Examples, JSON: r.FormValue("json"),
		Error: "Couldn't submit that ticket: " + err.Error()}
	if d.JSON == "" {
		d.JSON = exampleSkeleton
	}
	s.render(w, "submit.html", d)
}

// submissionPayload turns the form into a raw channel payload: an uploaded
// file, pasted JSON, or the structured form fields.
func submissionPayload(r *http.Request) ([]byte, error) {
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "multipart/form-data") {
		if err := r.ParseMultipartForm(MaxSubmitBytes); err != nil {
			return nil, fmt.Errorf("%w: form too large or malformed", ingest.ErrInvalid)
		}
	} else if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("%w: form too large or malformed", ingest.ErrInvalid)
	}
	if r.FormValue("mode") == "json" {
		if f, _, err := r.FormFile("file"); err == nil {
			defer f.Close()
			b, err := io.ReadAll(io.LimitReader(f, MaxSubmitBytes+1))
			if err != nil {
				return nil, err
			}
			if len(b) > MaxSubmitBytes {
				return nil, fmt.Errorf("%w: file larger than %d KB", ingest.ErrInvalid, MaxSubmitBytes>>10)
			}
			if len(strings.TrimSpace(string(b))) > 0 {
				return b, nil
			}
		}
		j := strings.TrimSpace(r.FormValue("json"))
		if j == "" {
			return nil, fmt.Errorf("%w: paste ticket JSON or choose a file", ingest.ErrInvalid)
		}
		return []byte(j), nil
	}
	f := func(k string) string { return strings.TrimSpace(r.FormValue(k)) }
	msg := f("message")
	if msg == "" {
		return nil, fmt.Errorf("%w: the message is empty", ingest.ErrInvalid)
	}
	name, email, company, subject := f("name"), f("email"), f("company"), f("subject")
	var v any
	switch f("channel") {
	case "email":
		from := email
		if name != "" && email != "" {
			from = fmt.Sprintf("%q <%s>", name, email)
		}
		v = map[string]any{"channel": "email", "from": from, "company": company, "subject": subject, "body": msg}
	case "chat":
		lines := []map[string]string{}
		for _, l := range strings.Split(msg, "\n") {
			if l = strings.TrimSpace(l); l != "" {
				lines = append(lines, map[string]string{"from": "customer", "text": l})
			}
		}
		v = map[string]any{"channel": "chat", "customer": map[string]string{"name": name, "email": email, "company": company}, "transcript": lines}
	default:
		v = map[string]any{"channel": "webform", "name": name, "email": email, "company": company, "topic": subject, "message": msg}
	}
	return json.Marshal(v)
}

var zeroTime = time.Time{}
