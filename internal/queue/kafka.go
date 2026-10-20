package queue

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

// KafkaTaskQueue is an alternative TaskQueue transport. MySQL remains the
// durable source of truth, so redelivery and out-of-order completion are safe:
// task execution is still guarded by the database state transition/lease.
type KafkaTaskQueue struct {
	topic  string
	writer *kafka.Writer
	reader *kafka.Reader
}

func NewKafkaTaskQueue(brokers []string, topic, group string) *KafkaTaskQueue {
	return &KafkaTaskQueue{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        group,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			CommitInterval: 0,
		}),
	}
}

func encodeKafkaMessage(message Message) string {
	return strconv.FormatUint(message.TaskID, 10) + ":" + strconv.FormatUint(message.Generation, 10)
}

func decodeKafkaMessage(value string) (Message, error) {
	parts := strings.Split(value, ":")
	if len(parts) == 1 {
		taskID, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return Message{}, err
		}
		return Message{TaskID: taskID}, nil
	}
	if len(parts) != 2 {
		return Message{}, fmt.Errorf("invalid kafka task message %q", value)
	}
	taskID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return Message{}, err
	}
	generation, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return Message{}, err
	}
	return Message{TaskID: taskID, Generation: generation}, nil
}

func (q *KafkaTaskQueue) Push(ctx context.Context, message Message) error {
	raw := encodeKafkaMessage(message)
	return q.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.FormatUint(message.TaskID, 10)),
		Value: []byte(raw),
	})
}

func (q *KafkaTaskQueue) PushBatch(ctx context.Context, taskMessages []Message) error {
	if len(taskMessages) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(taskMessages))
	for _, taskMessage := range taskMessages {
		raw := encodeKafkaMessage(taskMessage)
		messages = append(messages, kafka.Message{Key: []byte(strconv.FormatUint(taskMessage.TaskID, 10)), Value: []byte(raw)})
	}
	return q.writer.WriteMessages(ctx, messages...)
}

func (q *KafkaTaskQueue) Pop(ctx context.Context) (Delivery, error) {
	msg, err := q.reader.FetchMessage(ctx)
	if err != nil {
		return Delivery{}, err
	}
	taskMessage, err := decodeKafkaMessage(string(msg.Value))
	if err != nil {
		deliveryID := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
		return Delivery{}, &PoisonDeliveryError{
			Backend: "kafka", Source: q.topic, DeliveryID: deliveryID,
			RawEnvelope: string(msg.Value), FailureClass: "decode", Reason: fmt.Sprintf("parse kafka task message: %v", err),
		}
	}
	return Delivery{
		ID:          fmt.Sprintf("%d:%d", msg.Partition, msg.Offset),
		TaskID:      taskMessage.TaskID,
		Generation:  taskMessage.Generation,
		Backend:     "kafka",
		Source:      q.topic,
		RawEnvelope: string(msg.Value),
	}, nil
}

func (q *KafkaTaskQueue) Ack(ctx context.Context, deliveryID string) error {
	parts := strings.Split(deliveryID, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid kafka delivery id %q", deliveryID)
	}
	partition, err := strconv.Atoi(parts[0])
	if err != nil {
		return fmt.Errorf("parse kafka partition: %w", err)
	}
	offset, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("parse kafka offset: %w", err)
	}
	return q.reader.CommitMessages(ctx, kafka.Message{
		Topic: q.topic, Partition: partition, Offset: offset,
	})
}

func (q *KafkaTaskQueue) Close() error {
	readerErr := q.reader.Close()
	writerErr := q.writer.Close()
	if readerErr != nil {
		return readerErr
	}
	return writerErr
}
