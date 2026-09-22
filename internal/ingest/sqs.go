package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// ErrReceiveUnsupported is returned by SQSQueue's consumer methods: in AWS the
// Lambda event source mapping receives and deletes messages, not our code.
var ErrReceiveUnsupported = errors.New("this queue is consumed by the worker Lambda; receive is not supported here")

// SQSSendAPI is the one SQS call the producer side needs (fakeable in tests).
type SQSSendAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSQueue is the producer side of the real ticket queue. The message body is
// the normalized Ticket as JSON — the same shape DirQueue stores locally.
type SQSQueue struct {
	Client   SQSSendAPI
	QueueURL string
}

func (q *SQSQueue) Send(ctx context.Context, t Ticket) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = q.Client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(q.QueueURL),
		MessageBody: aws.String(string(b)),
	})
	if err != nil {
		return fmt.Errorf("sqs send: %w", err)
	}
	return nil
}

func (q *SQSQueue) Receive(context.Context, int) ([]Message, error) {
	return nil, ErrReceiveUnsupported
}
func (q *SQSQueue) Ack(context.Context, Message) error         { return ErrReceiveUnsupported }
func (q *SQSQueue) Nack(context.Context, Message, error) error { return ErrReceiveUnsupported }
