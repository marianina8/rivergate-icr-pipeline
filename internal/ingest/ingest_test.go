package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

func TestNormalizeChannels(t *testing.T) {
	cases := []struct {
		name, payload                    string
		channel, subject, email, company string
		bodyHas                          string
	}{
		{
			name:    "email",
			payload: `{"channel":"email","from":"Ada Park <ada@example.test>","company":"Acme Fictional","subject":"Hi","body":"Line one.","received_at":"2026-09-21T10:00:00Z"}`,
			channel: "email", subject: "Hi", email: "ada@example.test", company: "Acme Fictional", bodyHas: "Line one.",
		},
		{
			name: "chat keeps only customer lines",
			payload: `{"channel":"chat","customer":{"name":"B","email":"b@example.test","company":"Beta Co"},
				"transcript":[{"from":"bot","text":"How can we help?"},{"from":"customer","text":"How do I export? Thanks."},{"from":"customer","text":"Second line"}]}`,
			channel: "chat", subject: "How do I export?", email: "b@example.test", company: "Beta Co", bodyHas: "Second line",
		},
		{
			name:    "webform",
			payload: `{"channel":"WebForm","name":"C","email":"c@example.test","company":"Gamma","topic":"Billing q","message":"Charged twice"}`,
			channel: "webform", subject: "Billing q", email: "c@example.test", company: "Gamma", bodyHas: "Charged twice",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk, err := Normalize([]byte(tc.payload), "cli", now)
			if err != nil {
				t.Fatal(err)
			}
			if tk.Channel != tc.channel || tk.Subject != tc.subject || tk.CustomerEmail != tc.email || tk.Company != tc.company {
				t.Errorf("got %+v", tk)
			}
			if !strings.Contains(tk.Body, tc.bodyHas) {
				t.Errorf("body %q missing %q", tk.Body, tc.bodyHas)
			}
			if strings.Contains(tk.Body, "How can we help") {
				t.Error("bot lines must not be part of the ticket body")
			}
			if !strings.HasPrefix(tk.ID, "RG-") || len(tk.ID) != 11 {
				t.Errorf("id = %q", tk.ID)
			}
		})
	}
}

func TestNormalizeUsesPayloadTimestamp(t *testing.T) {
	tk, err := Normalize([]byte(`{"channel":"email","subject":"x","received_at":"2026-09-21T10:00:00Z"}`), "cli", now)
	if err != nil {
		t.Fatal(err)
	}
	if !tk.ReceivedAt.Equal(time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("received_at = %v", tk.ReceivedAt)
	}
}

func TestNormalizeRejectsInvalid(t *testing.T) {
	for name, p := range map[string]string{
		"bad json":        `{`,
		"unknown channel": `{"channel":"fax","body":"x"}`,
		"empty":           `{"channel":"email"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Normalize([]byte(p), "cli", now); !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestTicketIDStableAndContentBased(t *testing.T) {
	p := []byte(`{"channel":"email","from":"a@example.test","subject":"s","body":"b"}`)
	a, _ := Normalize(p, "cli", now)
	b, _ := Normalize(p, "webhook", now.Add(time.Hour))
	if a.ID != b.ID {
		t.Errorf("same content should give same ID: %s vs %s", a.ID, b.ID)
	}
	c, _ := Normalize([]byte(`{"channel":"email","from":"a@example.test","subject":"s","body":"different"}`), "cli", now)
	if a.ID == c.ID {
		t.Error("different content should give different IDs")
	}
}

func TestDirQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	q, err := NewDirQueue(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"RG-1", "RG-2", "RG-3"} {
		if err := q.Send(ctx, Ticket{ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	msgs, err := q.Receive(ctx, 2)
	if err != nil || len(msgs) != 2 {
		t.Fatalf("receive: %v %d", err, len(msgs))
	}
	if msgs[0].Ticket.ID != "RG-1" || msgs[1].Ticket.ID != "RG-2" {
		t.Errorf("not FIFO: %s %s", msgs[0].Ticket.ID, msgs[1].Ticket.ID)
	}
	if p, in, _ := q.Depth(); p != 1 || in != 2 {
		t.Errorf("depth pending=%d inflight=%d", p, in)
	}
	if err := q.Ack(ctx, msgs[0]); err != nil {
		t.Fatal(err)
	}
	if err := q.Nack(ctx, msgs[1], errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	if p, in, _ := q.Depth(); p != 2 || in != 0 {
		t.Errorf("after nack: pending=%d inflight=%d, want 2/0", p, in)
	}
}

func TestDirQueueDeadLetters(t *testing.T) {
	ctx := context.Background()
	q, _ := NewDirQueue(t.TempDir())
	_ = q.Send(ctx, Ticket{ID: "RG-1"})
	for i := 0; i < MaxAttempts; i++ {
		ms := mustReceive(t, q, 1)
		if len(ms) != 1 {
			t.Fatalf("attempt %d: no message", i+1)
		}
		if ms[0].Attempts != i {
			t.Errorf("attempts = %d, want %d", ms[0].Attempts, i)
		}
		_ = q.Nack(ctx, ms[0], errors.New("boom"))
	}
	if p, _, dead := q.Depth(); p != 0 || dead != 1 {
		t.Errorf("pending=%d dead=%d, want 0/1", p, dead)
	}
}

func mustReceive(t *testing.T, q *DirQueue, n int) []Message {
	t.Helper()
	ms, err := q.Receive(context.Background(), n)
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func TestDirQueueRecover(t *testing.T) {
	ctx := context.Background()
	q, _ := NewDirQueue(t.TempDir())
	_ = q.Send(ctx, Ticket{ID: "RG-1"})
	mustReceive(t, q, 1) // claimed, never acked (worker "crashed")
	if n, err := q.Recover(); err != nil || n != 1 {
		t.Fatalf("recover = %d, %v", n, err)
	}
	if len(mustReceive(t, q, 1)) != 1 {
		t.Error("recovered message should be receivable again")
	}
}

func fakeSink(calls *[]string) Sink {
	return func(_ context.Context, payload []byte, source string) (Ticket, bool, error) {
		tk, err := Normalize(payload, source, now)
		if err != nil {
			return Ticket{}, false, err
		}
		*calls = append(*calls, source+":"+tk.ID)
		return tk, false, nil
	}
}

func TestDropZoneScan(t *testing.T) {
	root := t.TempDir()
	in := filepath.Join(root, "incoming")
	_ = os.MkdirAll(in, 0o755)
	_ = os.WriteFile(filepath.Join(in, "a.json"), []byte(`{"channel":"email","subject":"hello","body":"x"}`), 0o644)
	_ = os.WriteFile(filepath.Join(in, "b.json"), []byte(`not json`), 0o644)
	_ = os.WriteFile(filepath.Join(in, "ignore.txt"), []byte(`x`), 0o644)
	var calls []string
	ids, rejected, err := DropZone{Root: root, Sink: fakeSink(&calls)}.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || len(rejected) != 1 || rejected[0] != "b.json" {
		t.Fatalf("ids=%v rejected=%v", ids, rejected)
	}
	if !strings.HasPrefix(calls[0], "dropzone:") {
		t.Errorf("source should be dropzone: %v", calls)
	}
	for _, p := range []string{"processed/a.json", "rejected/b.json", "incoming/ignore.txt"} {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Errorf("expected %s: %v", p, err)
		}
	}
}

func TestWebhookHandler(t *testing.T) {
	var calls []string
	h := WebhookHandler(fakeSink(&calls))
	cases := []struct {
		body string
		code int
	}{
		{`{"channel":"webform","email":"x@example.test","topic":"Help","message":"hi"}`, http.StatusAccepted},
		{`{"channel":"pigeon"}`, http.StatusBadRequest},
		{`nope`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/tickets", strings.NewReader(tc.body)))
		if rec.Code != tc.code {
			t.Errorf("%s -> %d, want %d (%s)", tc.body, rec.Code, tc.code, rec.Body)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/tickets", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET /tickets -> %d", rec.Code)
	}
	if len(calls) != 1 || !strings.HasPrefix(calls[0], "webhook:") {
		t.Errorf("calls = %v", calls)
	}
}
