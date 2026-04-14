// extensions/stream/sqs/source.go
package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/sopranoworks/gekka/stream"
)

// Message is a single received SQS message.
type Message struct {
	MessageID     string
	Body          string
	ReceiptHandle string
	Attributes    map[string]string
}

// SourceConfig configures an SQS Source.
type SourceConfig struct {
	Client          *sqs.Client
	QueueURL        string
	MaxMessages     int32 // 1–10; default 10
	WaitTimeSeconds int32 // long-poll; default 20
	AutoDelete      bool  // delete after successful receive
}

// Source long-polls an SQS queue and emits each message individually.
// Returns (zero, false, nil) when a poll returns empty — end of available messages.
//
// NOTE: connection is not closed on mid-stream abandonment.
func Source(cfg SourceConfig) stream.Source[Message, stream.NotUsed] {
	maxMsg := cfg.MaxMessages
	if maxMsg <= 0 || maxMsg > 10 {
		maxMsg = 10
	}
	wait := cfg.WaitTimeSeconds
	if wait <= 0 {
		wait = 20
	}
	var buf []sqstypes.Message
	return stream.FromIteratorFunc(func() (Message, bool, error) {
		if len(buf) == 0 {
			out, err := cfg.Client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
				QueueUrl:            aws.String(cfg.QueueURL),
				MaxNumberOfMessages: maxMsg,
				WaitTimeSeconds:     wait,
				AttributeNames:      []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
			})
			if err != nil {
				return Message{}, false, fmt.Errorf("sqs source: ReceiveMessage: %w", err)
			}
			buf = append(buf, out.Messages...)
			if len(buf) == 0 {
				return Message{}, false, nil
			}
		}
		raw := buf[0]
		buf = buf[1:]
		msg := Message{
			MessageID:     aws.ToString(raw.MessageId),
			Body:          aws.ToString(raw.Body),
			ReceiptHandle: aws.ToString(raw.ReceiptHandle),
			Attributes:    make(map[string]string),
		}
		for k, v := range raw.Attributes {
			msg.Attributes[string(k)] = v
		}
		if cfg.AutoDelete {
			_, _ = cfg.Client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(cfg.QueueURL),
				ReceiptHandle: raw.ReceiptHandle,
			})
		}
		return msg, true, nil
	})
}
