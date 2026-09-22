package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoAPI is the subset of the DynamoDB client the store uses (fakeable).
type DynamoAPI interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// QueueIndex is the GSI defined in infra/template.yaml.
const QueueIndex = "queue-received_at"

// Dynamo stores items in the DynamoDB table from infra/template.yaml.
//
// Layout: the full Item is kept as one JSON document in the "doc" attribute
// (so the schema can evolve without migrations), and the fields we query on
// are copied to top-level attributes: id (hash key), queue + received_at
// (GSI), status, needs_review. Items stay far below the 400 KB item limit —
// a demo ticket with its full audit trail is a few KB.
type Dynamo struct {
	Client DynamoAPI
	Table  string
}

func (d *Dynamo) Get(ctx context.Context, id string) (Item, error) {
	out, err := d.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(d.Table),
		Key:            map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: id}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return Item{}, fmt.Errorf("dynamodb get %s: %w", id, err)
	}
	if len(out.Item) == 0 {
		return Item{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return decodeDoc(out.Item)
}

func (d *Dynamo) Put(ctx context.Context, it Item) error {
	if it.ID == "" {
		return fmt.Errorf("dynamodb put: empty id")
	}
	b, err := json.Marshal(it)
	if err != nil {
		return err
	}
	av := map[string]types.AttributeValue{
		"id":           &types.AttributeValueMemberS{Value: it.ID},
		"doc":          &types.AttributeValueMemberS{Value: string(b)},
		"status":       &types.AttributeValueMemberS{Value: string(it.Status)},
		"needs_review": &types.AttributeValueMemberBOOL{Value: it.NeedsReview},
		"received_at":  &types.AttributeValueMemberS{Value: it.Ticket.ReceivedAt.UTC().Format(time.RFC3339Nano)},
	}
	// GSI key attributes may not be empty strings; omit until routed.
	if it.Queue != "" {
		av["queue"] = &types.AttributeValueMemberS{Value: it.Queue}
	}
	_, err = d.Client.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(d.Table), Item: av})
	if err != nil {
		return fmt.Errorf("dynamodb put %s: %w", it.ID, err)
	}
	return nil
}

// List queries the queue GSI when a queue is given, otherwise scans (fine at
// demo volume). Results are always re-filtered and sorted client-side so the
// behaviour matches the local stores exactly.
func (d *Dynamo) List(ctx context.Context, f Filter) ([]Item, error) {
	var out []Item
	var start map[string]types.AttributeValue
	for {
		var page []map[string]types.AttributeValue
		var next map[string]types.AttributeValue
		if f.Queue != "" {
			res, err := d.Client.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String(d.Table),
				IndexName:                 aws.String(QueueIndex),
				KeyConditionExpression:    aws.String("#q = :q"),
				ExpressionAttributeNames:  map[string]string{"#q": "queue"},
				ExpressionAttributeValues: map[string]types.AttributeValue{":q": &types.AttributeValueMemberS{Value: f.Queue}},
				ExclusiveStartKey:         start,
			})
			if err != nil {
				return nil, fmt.Errorf("dynamodb query: %w", err)
			}
			page, next = res.Items, res.LastEvaluatedKey
		} else {
			in := &dynamodb.ScanInput{TableName: aws.String(d.Table), ExclusiveStartKey: start}
			if f.NeedsReview != nil {
				in.FilterExpression = aws.String("needs_review = :r")
				in.ExpressionAttributeValues = map[string]types.AttributeValue{":r": &types.AttributeValueMemberBOOL{Value: *f.NeedsReview}}
			}
			res, err := d.Client.Scan(ctx, in)
			if err != nil {
				return nil, fmt.Errorf("dynamodb scan: %w", err)
			}
			page, next = res.Items, res.LastEvaluatedKey
		}
		for _, av := range page {
			it, err := decodeDoc(av)
			if err != nil {
				return nil, err
			}
			if f.match(it) {
				out = append(out, it)
			}
		}
		if len(next) == 0 {
			break
		}
		start = next
	}
	sortItems(out)
	return out, nil
}

func decodeDoc(av map[string]types.AttributeValue) (Item, error) {
	doc, ok := av["doc"].(*types.AttributeValueMemberS)
	if !ok {
		return Item{}, fmt.Errorf("dynamodb item missing doc attribute")
	}
	var it Item
	if err := json.Unmarshal([]byte(doc.Value), &it); err != nil {
		return Item{}, fmt.Errorf("decode item: %w", err)
	}
	return it, nil
}
