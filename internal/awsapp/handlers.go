package awsapp

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
)

// S3GetAPI is the one S3 call the worker needs.
type S3GetAPI interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// MaxObjectBytes caps how much of a drop-zone object is read.
const MaxObjectBytes = ingest.MaxWebhookBody

// ---- worker ----------------------------------------------------------------

// Worker is the SQS-triggered Lambda. Each SQS record is either
//   - a normalized ingest.Ticket (sent by the webhook Lambda or the CLI), or
//   - an S3 event notification for a new object in the drop-zone bucket.
//
// It returns partial batch failures so only failed records are retried
// (the template enables ReportBatchItemFailures; 3 receives -> DLQ).
type Worker struct {
	Svc *pipeline.Service
	S3  S3GetAPI
	Log *slog.Logger
}

type s3Notification struct {
	Event   string `json:"Event"` // "s3:TestEvent" when the notification is first configured
	Records []struct {
		EventSource string `json:"eventSource"`
		S3          struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func (w *Worker) log() *slog.Logger {
	if w.Log != nil {
		return w.Log
	}
	return slog.Default()
}

// Handle processes one SQS batch.
func (w *Worker) Handle(ctx context.Context, ev events.SQSEvent) (events.SQSEventResponse, error) {
	var resp events.SQSEventResponse
	for _, rec := range ev.Records {
		if err := w.handleRecord(ctx, rec.Body); err != nil {
			w.log().Error("record failed; will be retried", "message_id", rec.MessageId, "err", err)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.SQSBatchItemFailure{ItemIdentifier: rec.MessageId})
		}
	}
	return resp, nil
}

func (w *Worker) handleRecord(ctx context.Context, body string) error {
	var n s3Notification
	if err := json.Unmarshal([]byte(body), &n); err == nil && (n.Event == "s3:TestEvent" || len(n.Records) > 0) {
		if n.Event == "s3:TestEvent" {
			return nil
		}
		for _, r := range n.Records {
			if r.EventSource != "aws:s3" {
				continue
			}
			if err := w.handleObject(ctx, r.S3.Bucket.Name, r.S3.Object.Key); err != nil {
				return err
			}
		}
		return nil
	}
	var t ingest.Ticket
	if err := json.Unmarshal([]byte(body), &t); err != nil || t.ID == "" {
		// Poison message: retrying cannot fix it. Log and drop.
		w.log().Warn("dropping unrecognized queue message", "body_prefix", prefix(body, 120))
		return nil
	}
	_, err := w.Svc.Process(ctx, t, "worker")
	return err
}

func (w *Worker) handleObject(ctx context.Context, bucket, rawKey string) error {
	key, err := url.QueryUnescape(rawKey) // S3 event keys are URL-encoded ('+' for spaces)
	if err != nil {
		key = rawKey
	}
	out, err := w.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return fmt.Errorf("get s3://%s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(out.Body, MaxObjectBytes))
	if err != nil {
		return err
	}
	t, _, err := w.Svc.Accept(ctx, payload, "dropzone")
	if errors.Is(err, ingest.ErrInvalid) {
		w.log().Warn("rejected drop-zone object", "key", key, "err", err)
		return nil // not retryable
	}
	if err != nil {
		return err
	}
	_, err = w.Svc.Process(ctx, t, "worker")
	return err
}

func prefix(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// ---- webhook ---------------------------------------------------------------

// TokenHeader carries the shared secret webhook callers must send.
const TokenHeader = "x-rivergate-token"

// Webhook is the API Gateway (HTTP API) Lambda: authenticate, normalize,
// record as received, enqueue to SQS, answer 202.
type Webhook struct {
	Svc   *pipeline.Service
	Token string // shared secret; empty means every request is rejected
	Log   *slog.Logger
}

// Handle serves POST /tickets.
func (h *Webhook) Handle(ctx context.Context, req events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	if h.Token == "" {
		return jsonResp(500, map[string]string{"error": "webhook secret not configured"}), nil
	}
	got := ""
	for k, v := range req.Headers {
		if strings.EqualFold(k, TokenHeader) {
			got = v
		}
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(h.Token)) != 1 {
		return jsonResp(401, map[string]string{"error": "missing or invalid " + TokenHeader + " header"}), nil
	}
	body := []byte(req.Body)
	if req.IsBase64Encoded {
		b, err := base64.StdEncoding.DecodeString(req.Body)
		if err != nil {
			return jsonResp(400, map[string]string{"error": "invalid base64 body"}), nil
		}
		body = b
	}
	if len(body) > ingest.MaxWebhookBody {
		return jsonResp(413, map[string]string{"error": "payload too large"}), nil
	}
	t, dup, err := h.Svc.Ingest(ctx, body, "webhook")
	switch {
	case errors.Is(err, ingest.ErrInvalid):
		return jsonResp(400, map[string]string{"error": err.Error()}), nil
	case err != nil:
		if h.Log != nil {
			h.Log.Error("ingest failed", "err", err)
		}
		return jsonResp(500, map[string]string{"error": "internal error"}), nil
	}
	return jsonResp(202, map[string]any{"id": t.ID, "duplicate": dup, "status": "queued"}), nil
}

func jsonResp(code int, v any) events.APIGatewayV2HTTPResponse {
	b, _ := json.Marshal(v)
	return events.APIGatewayV2HTTPResponse{StatusCode: code, Headers: map[string]string{"Content-Type": "application/json"}, Body: string(b)}
}

// SSMGetAPI is the one SSM call needed to read the webhook secret.
type SSMGetAPI interface {
	GetParameter(ctx context.Context, in *ssm.GetParameterInput, opts ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
}

// FetchSecret reads a SecureString parameter (decrypted).
func FetchSecret(ctx context.Context, c SSMGetAPI, name string) (string, error) {
	out, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(name), WithDecryption: aws.Bool(true)})
	if err != nil {
		return "", fmt.Errorf("read SSM parameter %s: %w", name, err)
	}
	return aws.ToString(out.Parameter.Value), nil
}
