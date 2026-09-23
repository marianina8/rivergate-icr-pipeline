package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/marianina8/rivergate-icr-pipeline/internal/classify"
	"github.com/marianina8/rivergate-icr-pipeline/internal/ingest"
	"github.com/marianina8/rivergate-icr-pipeline/internal/store"
)

// conformance runs the same behavioural checks against any Store. The phase 5
// DynamoDB store runs it against an in-memory fake of the DynamoDB API.
func conformance(t *testing.T, s store.Store) {
	ctx := context.Background()
	t0 := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)

	if _, err := s.Get(ctx, "RG-MISSING"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing item: err = %v", err)
	}
	a := store.Item{ID: "RG-A", Ticket: ingest.Ticket{ID: "RG-A", ReceivedAt: t0.Add(time.Minute)}, Status: store.StatusReceived}
	a.AddEvent(t0, "received", "test", "hello", map[string]any{"k": "v"})
	b := store.Item{ID: "RG-B", Ticket: ingest.Ticket{ID: "RG-B", ReceivedAt: t0}, Status: store.StatusPendingReview, Queue: "human-review", NeedsReview: true,
		Classification: &classify.Classification{Category: "other", Confidence: 0.3}}
	for _, it := range []store.Item{a, b} {
		if err := s.Put(ctx, it); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Get(ctx, "RG-A")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || got.Events[0].Data["k"] != "v" {
		t.Errorf("events not round-tripped: %+v", got.Events)
	}
	// Mutating a returned copy must not change the stored item.
	got.Status = store.StatusFailed
	if again, _ := s.Get(ctx, "RG-A"); again.Status != store.StatusReceived {
		t.Error("store returned a shared reference")
	}

	all, _ := s.List(ctx, store.Filter{})
	if len(all) != 2 || all[0].ID != "RG-B" {
		t.Errorf("list should be ordered by received time: %v", ids(all))
	}
	yes := true
	rq, _ := s.List(ctx, store.Filter{NeedsReview: &yes})
	if len(rq) != 1 || rq[0].ID != "RG-B" || rq[0].Classification.Confidence != 0.3 {
		t.Errorf("review filter: %v", ids(rq))
	}
	byQueue, _ := s.List(ctx, store.Filter{Queue: "human-review", Status: store.StatusPendingReview})
	if len(byQueue) != 1 {
		t.Errorf("queue+status filter: %v", ids(byQueue))
	}

	// Sandbox items: scoped by workspace, invisible once expired.
	future, past := time.Now().Add(time.Hour), time.Now().Add(-time.Minute)
	live := store.Item{ID: "RG-WS1", Ticket: ingest.Ticket{ID: "RG-WS1", Workspace: "aa11", ReceivedAt: t0}, Status: store.StatusReceived, ExpiresAt: &future}
	gone := store.Item{ID: "RG-WS2", Ticket: ingest.Ticket{ID: "RG-WS2", Workspace: "aa11", ReceivedAt: t0}, Status: store.StatusReceived, ExpiresAt: &past}
	other := store.Item{ID: "RG-WS3", Ticket: ingest.Ticket{ID: "RG-WS3", Workspace: "bb22", ReceivedAt: t0}, Status: store.StatusReceived, ExpiresAt: &future}
	for _, it := range []store.Item{live, gone, other} {
		if err := s.Put(ctx, it); err != nil {
			t.Fatal(err)
		}
	}
	ws, _ := s.List(ctx, store.Filter{Workspace: "aa11"})
	if len(ws) != 1 || ws[0].ID != "RG-WS1" {
		t.Errorf("workspace filter / expiry: %v", ids(ws))
	}
	if _, err := s.Get(ctx, "RG-WS2"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("expired item still readable: %v", err)
	}
	if all, _ := s.List(ctx, store.Filter{}); len(all) != 4 {
		t.Errorf("unfiltered list should include every live workspace: %v", ids(all))
	}
}

func ids(items []store.Item) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.ID)
	}
	return out
}

func TestMemoryStore(t *testing.T) { conformance(t, store.NewMemory()) }

func TestFileStore(t *testing.T) {
	s, err := store.NewFile(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, s)
}

func TestFileStoreRejectsPathTraversal(t *testing.T) {
	s, _ := store.NewFile(t.TempDir())
	if _, err := s.Get(context.Background(), "../../etc/passwd"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("err = %v", err)
	}
	if err := s.Put(context.Background(), store.Item{ID: "../x"}); err == nil {
		t.Error("put with traversal id should fail")
	}
}

// fakeDynamo is an in-memory stand-in for the DynamoDB API: enough of
// GetItem/PutItem/Query/Scan (with one-item pages, to exercise pagination)
// for the conformance suite. No AWS calls.
type fakeDynamo struct {
	items map[string]map[string]types.AttributeValue
	order []string
}

func newFakeDynamo() *fakeDynamo {
	return &fakeDynamo{items: map[string]map[string]types.AttributeValue{}}
}

func (f *fakeDynamo) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	id := in.Key["id"].(*types.AttributeValueMemberS).Value
	return &dynamodb.GetItemOutput{Item: f.items[id]}, nil
}

func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	id := in.Item["id"].(*types.AttributeValueMemberS).Value
	if q, ok := in.Item["queue"].(*types.AttributeValueMemberS); ok && q.Value == "" {
		return nil, errors.New("ValidationException: empty string for index key")
	}
	if _, ok := f.items[id]; !ok {
		f.order = append(f.order, id)
	}
	f.items[id] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDynamo) page(match func(map[string]types.AttributeValue) bool, start map[string]types.AttributeValue) ([]map[string]types.AttributeValue, map[string]types.AttributeValue) {
	begin := 0
	if start != nil {
		sid := start["id"].(*types.AttributeValueMemberS).Value
		for i, id := range f.order {
			if id == sid {
				begin = i + 1
			}
		}
	}
	for i := begin; i < len(f.order); i++ {
		av := f.items[f.order[i]]
		if match(av) {
			var next map[string]types.AttributeValue
			if i < len(f.order)-1 {
				next = map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: f.order[i]}}
			}
			return []map[string]types.AttributeValue{av}, next
		}
	}
	return nil, nil
}

func (f *fakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if aws.ToString(in.IndexName) != store.QueueIndex {
		return nil, errors.New("unexpected index")
	}
	want := in.ExpressionAttributeValues[":q"].(*types.AttributeValueMemberS).Value
	items, next := f.page(func(av map[string]types.AttributeValue) bool {
		q, ok := av["queue"].(*types.AttributeValueMemberS)
		return ok && q.Value == want
	}, in.ExclusiveStartKey)
	return &dynamodb.QueryOutput{Items: items, LastEvaluatedKey: next}, nil
}

func (f *fakeDynamo) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	items, next := f.page(func(map[string]types.AttributeValue) bool { return true }, in.ExclusiveStartKey)
	return &dynamodb.ScanOutput{Items: items, LastEvaluatedKey: next}, nil
}

func TestDynamoStore(t *testing.T) {
	conformance(t, &store.Dynamo{Client: newFakeDynamo(), Table: "items"})
}

func TestDynamoStoreOmitsEmptyQueueKey(t *testing.T) {
	fd := newFakeDynamo()
	s := &store.Dynamo{Client: fd, Table: "items"}
	if err := s.Put(context.Background(), store.Item{ID: "RG-1", Status: store.StatusReceived}); err != nil {
		t.Fatalf("a received item (no queue yet) must be storable: %v", err)
	}
	if _, ok := fd.items["RG-1"]["queue"]; ok {
		t.Error("empty queue must not be written as a GSI key")
	}
}
