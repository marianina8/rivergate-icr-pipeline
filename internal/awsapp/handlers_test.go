package awsapp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/pipeline"
	"github.com/marianina8/rivergate-icr-pipeline/internal/router"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
	"github.com/marianina8/rivergate-icr-pipeline/internal/testutil"
)

// ---- fakes (no AWS calls) ----

type fakeSQS struct{ bodies []string }

func (f *fakeSQS) SendMessage(_ context.Context, in *sqs.SendMessageInput, _ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	f.bodies = append(f.bodies, aws.ToString(in.MessageBody))
	return &sqs.SendMessageOutput{MessageId: aws.String("m-1")}, nil
}

type fakeS3 struct {
	objects map[string][]byte
	gotKeys []string
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	k := aws.ToString(in.Key)
	f.gotKeys = append(f.gotKeys, k)
	b, ok := f.objects[aws.ToString(in.Bucket)+"/"+k]
	if !ok {
		return nil, errors.New("NoSuchKey")
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b))}, nil
}

type fakeSSM struct{ value string }

func (f fakeSSM) GetParameter(_ context.Context, in *ssm.GetParameterInput, _ ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
	if !aws.ToBool(in.WithDecryption) {
		return nil, errors.New("must decrypt")
	}
	return &ssm.GetParameterOutput{Parameter: &ssmtypes.Parameter{Value: aws.String(f.value)}}, nil
}

type env struct {
	svc     *pipeline.Service
	sqs     *fakeSQS
	actions *bytes.Buffer
}

func newEnv(t *testing.T) env {
	t.Helper()
	cfg := testutil.Config(t)
	m, _ := classify.NewMock(cfg)
	q := &fakeSQS{}
	buf := &bytes.Buffer{}
	svc := &pipeline.Service{Cfg: cfg, Classifier: m, Store: store.NewMemory(), Router: router.New(cfg),
		Actions: &router.LogSink{W: buf, Articles: cfg.Actions.KBArticles},
		Queue:   &ingest.SQSQueue{Client: q, QueueURL: "https://sqs.example/q"}}
	return env{svc: svc, sqs: q, actions: buf}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testutil.RepoRoot(t), "demo", "tickets", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ---- webhook ----

func TestWebhookAuthAndIngest(t *testing.T) {
	e := newEnv(t)
	h := &Webhook{Svc: e.svc, Token: "s3cret"}
	ctx := context.Background()
	body := string(fixture(t, "01-webform-outage-503.json"))

	resp, _ := h.Handle(ctx, events.APIGatewayV2HTTPRequest{Body: body})
	if resp.StatusCode != 401 {
		t.Errorf("missing token = %d, want 401", resp.StatusCode)
	}
	resp, _ = h.Handle(ctx, events.APIGatewayV2HTTPRequest{Body: body, Headers: map[string]string{"x-rivergate-token": "wrong"}})
	if resp.StatusCode != 401 {
		t.Errorf("wrong token = %d, want 401", resp.StatusCode)
	}
	if len(e.sqs.bodies) != 0 {
		t.Fatal("rejected requests must not enqueue anything")
	}

	resp, _ = h.Handle(ctx, events.APIGatewayV2HTTPRequest{Body: body, Headers: map[string]string{"X-Rivergate-Token": "s3cret"}})
	if resp.StatusCode != 202 {
		t.Fatalf("valid request = %d %s", resp.StatusCode, resp.Body)
	}
	var out struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	_ = json.Unmarshal([]byte(resp.Body), &out)
	it, err := e.svc.Get(ctx, out.ID)
	if err != nil || it.Status != store.StatusReceived || it.Ticket.Source != "webhook" {
		t.Errorf("item not recorded as received: %+v %v", it.Status, err)
	}
	var queued ingest.Ticket
	if len(e.sqs.bodies) != 1 || json.Unmarshal([]byte(e.sqs.bodies[0]), &queued) != nil || queued.ID != out.ID {
		t.Errorf("SQS message should be the normalized ticket: %v", e.sqs.bodies)
	}

	// base64 body + duplicate delivery
	resp, _ = h.Handle(ctx, events.APIGatewayV2HTTPRequest{Body: base64.StdEncoding.EncodeToString([]byte(body)), IsBase64Encoded: true,
		Headers: map[string]string{"x-rivergate-token": "s3cret"}})
	if resp.StatusCode != 202 || !strings.Contains(resp.Body, `"duplicate":true`) || len(e.sqs.bodies) != 1 {
		t.Errorf("duplicate delivery: %d %s (sqs sends=%d)", resp.StatusCode, resp.Body, len(e.sqs.bodies))
	}

	resp, _ = h.Handle(ctx, events.APIGatewayV2HTTPRequest{Body: `{"channel":"fax"}`, Headers: map[string]string{"x-rivergate-token": "s3cret"}})
	if resp.StatusCode != 400 {
		t.Errorf("invalid payload = %d", resp.StatusCode)
	}
}

func TestWebhookFailsClosedWithoutSecret(t *testing.T) {
	e := newEnv(t)
	resp, _ := (&Webhook{Svc: e.svc}).Handle(context.Background(), events.APIGatewayV2HTTPRequest{Body: "{}", Headers: map[string]string{"x-rivergate-token": ""}})
	if resp.StatusCode != 500 || len(e.sqs.bodies) != 0 {
		t.Errorf("no configured secret must reject everything: %d", resp.StatusCode)
	}
}

func TestFetchSecret(t *testing.T) {
	v, err := FetchSecret(context.Background(), fakeSSM{value: "abc"}, "/rivergate-icr/webhook-shared-secret")
	if err != nil || v != "abc" {
		t.Errorf("got %q %v", v, err)
	}
}

// ---- worker ----

func TestWorkerProcessesQueuedTickets(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	h := &Webhook{Svc: e.svc, Token: "t"}
	for _, f := range []string{"01-webform-outage-503.json", "07-email-guest-seats-ambiguous.json"} {
		if r, _ := h.Handle(ctx, events.APIGatewayV2HTTPRequest{Body: string(fixture(t, f)), Headers: map[string]string{"x-rivergate-token": "t"}}); r.StatusCode != 202 {
			t.Fatal(r.Body)
		}
	}
	var recs []events.SQSMessage
	for i, b := range e.sqs.bodies {
		recs = append(recs, events.SQSMessage{MessageId: string(rune('a' + i)), Body: b})
	}
	w := &Worker{Svc: e.svc}
	resp, err := w.Handle(ctx, events.SQSEvent{Records: recs})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("failures: %+v %v", resp.BatchItemFailures, err)
	}
	st, _ := e.svc.Stats(ctx)
	if st.Automatic != 1 || st.NeedsReview != 1 {
		t.Errorf("stats = %+v", st)
	}
	logs := e.actions.String()
	if strings.Count(logs, `"msg":"icr_action"`) != 2 || !strings.Contains(logs, `"reason"`) {
		t.Errorf("outage should log 2 actions with reasons:\n%s", logs)
	}
	// Redelivery (SQS is at-least-once) does not repeat actions.
	_, _ = w.Handle(ctx, events.SQSEvent{Records: recs})
	if strings.Count(e.actions.String(), `"msg":"icr_action"`) != 2 {
		t.Error("redelivered messages re-ran actions")
	}
}

func s3Event(bucket, key string) string {
	return `{"Records":[{"eventSource":"aws:s3","eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"` + bucket + `"},"object":{"key":"` + key + `"}}}]}`
}

func TestWorkerHandlesS3DropZone(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	s3c := &fakeS3{objects: map[string][]byte{
		"drop/incoming/03 double charge.json": fixture(t, "03-chat-double-charge.json"),
		"drop/incoming/bad.json":              []byte(`not json`),
	}}
	w := &Worker{Svc: e.svc, S3: s3c}
	resp, _ := w.Handle(ctx, events.SQSEvent{Records: []events.SQSMessage{
		{MessageId: "1", Body: s3Event("drop", "incoming/03+double+charge.json")}, // URL-encoded key
		{MessageId: "2", Body: s3Event("drop", "incoming/bad.json")},              // rejected, not retried
		{MessageId: "3", Body: `{"Service":"Amazon S3","Event":"s3:TestEvent"}`},  // ignored
		{MessageId: "4", Body: `{"hello":"world"}`},                               // poison, dropped
		{MessageId: "5", Body: s3Event("drop", "incoming/missing.json")},          // S3 error -> retry
	}})
	if len(resp.BatchItemFailures) != 1 || resp.BatchItemFailures[0].ItemIdentifier != "5" {
		t.Errorf("only the S3 read failure should be retried: %+v", resp.BatchItemFailures)
	}
	if s3c.gotKeys[0] != "incoming/03 double charge.json" {
		t.Errorf("key not URL-decoded: %q", s3c.gotKeys[0])
	}
	items, _ := e.svc.List(ctx, store.Filter{})
	if len(items) != 1 || items[0].Queue != "billing" || items[0].Ticket.Source != "dropzone" {
		t.Fatalf("drop-zone ticket should be classified and routed to billing: %+v", items)
	}
	if len(e.sqs.bodies) != 0 {
		t.Error("drop-zone objects are processed in place, not re-enqueued")
	}
}

// ---- config ----

func TestLoadConfigAppliesDeploymentOverrides(t *testing.T) {
	t.Setenv("ICR_BEDROCK_MODEL_ID", "us.anthropic.some-other-model")
	t.Setenv("AWS_REGION", "us-east-1")
	cfg, err := LoadConfig(testutil.ConfigPath(t))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Classifier.Bedrock.ModelID != "us.anthropic.some-other-model" || cfg.Classifier.Bedrock.Region != "us-east-1" {
		t.Errorf("overrides not applied: %+v", cfg.Classifier.Bedrock)
	}
	t.Setenv("LAMBDA_TASK_ROOT", "/var/task")
	if got := ResolveConfigPath("config/rivergate.yaml"); got != "/var/task/config/rivergate.yaml" {
		t.Errorf("ResolveConfigPath = %s", got)
	}
}

func TestNewRequiresTable(t *testing.T) {
	t.Setenv("ICR_ITEMS_TABLE", "")
	if _, err := New(context.Background(), Options{ConfigPath: testutil.ConfigPath(t)}); err == nil || !strings.Contains(err.Error(), "ICR_ITEMS_TABLE") {
		t.Errorf("err = %v", err)
	}
}

// The sample events used with `sam local invoke` must parse and run.
func TestSampleEventsForSamLocal(t *testing.T) {
	root := filepath.Join(testutil.RepoRoot(t), "infra", "events")
	read := func(name string, v any) {
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	e := newEnv(t)
	ctx := context.Background()

	var wh events.APIGatewayV2HTTPRequest
	read("webhook-post.json", &wh)
	if r, _ := (&Webhook{Svc: e.svc, Token: "local-dev-token"}).Handle(ctx, wh); r.StatusCode != 202 {
		t.Errorf("webhook-post.json -> %d %s", r.StatusCode, r.Body)
	}

	var sq events.SQSEvent
	read("sqs-ticket.json", &sq)
	if r, _ := (&Worker{Svc: e.svc}).Handle(ctx, sq); len(r.BatchItemFailures) != 0 {
		t.Errorf("sqs-ticket.json failed: %+v", r)
	}
	if it, err := e.svc.Get(ctx, "RG-LOCAL001"); err != nil || it.Queue != "bug" {
		t.Errorf("sqs-ticket.json item: %v %v", it.Queue, err)
	}

	var s3ev events.SQSEvent
	read("sqs-s3-dropzone.json", &s3ev)
	s3c := &fakeS3{objects: map[string][]byte{"REPLACE-with-DropZoneBucketName/incoming/03-chat-double-charge.json": fixture(t, "03-chat-double-charge.json")}}
	if r, _ := (&Worker{Svc: e.svc, S3: s3c}).Handle(ctx, s3ev); len(r.BatchItemFailures) != 0 {
		t.Errorf("sqs-s3-dropzone.json failed: %+v", r)
	}
}
