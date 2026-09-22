// Package ingest normalizes raw support tickets from every channel (email,
// chat, web form) into one Ticket shape, and provides the local stand-ins for
// the AWS ingestion path: a directory-backed queue (SQS), a drop-zone folder
// (S3 bucket + event notification) and an HTTP webhook receiver
// (API Gateway + Lambda).
package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"
)

// Channels a ticket can arrive on.
const (
	ChannelEmail   = "email"
	ChannelChat    = "chat"
	ChannelWebform = "webform"
)

// Ticket is the normalized message shape every entry point produces and the
// worker consumes. In AWS this is the SQS message body.
type Ticket struct {
	ID            string    `json:"id"`
	Channel       string    `json:"channel"`
	Source        string    `json:"source"` // cli | dropzone | webhook
	ReceivedAt    time.Time `json:"received_at"`
	CustomerName  string    `json:"customer_name"`
	CustomerEmail string    `json:"customer_email"`
	Company       string    `json:"company"`
	Subject       string    `json:"subject"`
	Body          string    `json:"body"`
}

// raw is the union of the three channel payload shapes.
type raw struct {
	Channel string `json:"channel"`

	// email
	From       string `json:"from"`
	Subject    string `json:"subject"`
	Body       string `json:"body"`
	ReceivedAt string `json:"received_at"`

	// chat
	Customer struct {
		Name    string `json:"name"`
		Email   string `json:"email"`
		Company string `json:"company"`
	} `json:"customer"`
	StartedAt  string `json:"started_at"`
	Transcript []struct {
		From string `json:"from"`
		Text string `json:"text"`
	} `json:"transcript"`

	// webform
	Name        string `json:"name"`
	Email       string `json:"email"`
	Company     string `json:"company"`
	Topic       string `json:"topic"`
	Message     string `json:"message"`
	SubmittedAt string `json:"submitted_at"`
}

// ErrInvalid marks payloads that can never be ingested (bad JSON, unknown
// channel, no text). Entry points map it to a 4xx / rejected file.
var ErrInvalid = errors.New("invalid ticket payload")

// Normalize converts a raw channel payload (JSON) into a Ticket.
// source records which entry point received it.
func Normalize(payload []byte, source string, now time.Time) (Ticket, error) {
	var r raw
	if err := json.Unmarshal(payload, &r); err != nil {
		return Ticket{}, fmt.Errorf("%w: decode: %v", ErrInvalid, err)
	}
	t := Ticket{Channel: strings.ToLower(strings.TrimSpace(r.Channel)), Source: source}
	var ts string
	switch t.Channel {
	case ChannelEmail:
		t.CustomerName, t.CustomerEmail = parseFrom(r.From)
		t.Company = r.Company
		t.Subject = r.Subject
		t.Body = r.Body
		ts = r.ReceivedAt
	case ChannelChat:
		t.CustomerName, t.CustomerEmail, t.Company = r.Customer.Name, r.Customer.Email, r.Customer.Company
		var lines []string
		for _, m := range r.Transcript {
			if strings.EqualFold(m.From, "customer") {
				lines = append(lines, strings.TrimSpace(m.Text))
			}
		}
		t.Body = strings.Join(lines, "\n")
		t.Subject = firstLine(t.Body, 80)
		ts = r.StartedAt
	case ChannelWebform:
		t.CustomerName, t.CustomerEmail, t.Company = r.Name, r.Email, r.Company
		t.Subject = r.Topic
		t.Body = r.Message
		ts = r.SubmittedAt
	default:
		return Ticket{}, fmt.Errorf("%w: unknown channel %q (want email, chat or webform)", ErrInvalid, r.Channel)
	}
	t.Subject = strings.TrimSpace(t.Subject)
	t.Body = strings.TrimSpace(t.Body)
	if t.Subject == "" && t.Body == "" {
		return Ticket{}, fmt.Errorf("%w: no subject or body", ErrInvalid)
	}
	t.ReceivedAt = now.UTC()
	if ts != "" {
		if parsed, err := time.Parse(time.RFC3339, ts); err == nil {
			t.ReceivedAt = parsed.UTC()
		}
	}
	t.ID = TicketID(t)
	return t, nil
}

// TicketID is a stable content hash, so re-ingesting the same ticket (a retry,
// a duplicate webhook delivery) is idempotent.
func TicketID(t Ticket) string {
	h := sha256.Sum256([]byte(strings.Join([]string{t.Channel, strings.ToLower(t.CustomerEmail), t.Subject, t.Body}, "\x1f")))
	return "RG-" + strings.ToUpper(hex.EncodeToString(h[:4]))
}

func parseFrom(from string) (name, email string) {
	if a, err := mail.ParseAddress(from); err == nil {
		return a.Name, a.Address
	}
	return "", strings.TrimSpace(from)
}

func firstLine(s string, max int) string {
	if i := strings.IndexAny(s, "\n"); i >= 0 {
		s = s[:i]
	}
	if i := strings.IndexAny(s, ".?!"); i >= 0 && i+1 < len(s) {
		s = s[:i+1]
	}
	r := []rune(s)
	if len(r) > max {
		return strings.TrimSpace(string(r[:max-1])) + "…"
	}
	return s
}
