package queue

import (
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisTaskQueueUsesConfiguredKey(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	q := NewRedisTaskQueue(client, "custom:queue")
	if q.key != "custom:queue" {
		t.Fatalf("queue key = %q", q.key)
	}
	if q.group != "taskflow-workers" || q.consumer == "" {
		t.Fatalf("unexpected stream consumer configuration: group=%q consumer=%q", q.group, q.consumer)
	}
	_ = client.Close()
}

func TestIsNoGroup(t *testing.T) {
	if !isNoGroup(errors.New("NOGROUP No such key or consumer group")) {
		t.Fatal("expected NOGROUP to be detected")
	}
	if isNoGroup(errors.New("connection refused")) {
		t.Fatal("unexpected NOGROUP classification")
	}
}

func TestInvalidateGroup(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	q := NewRedisTaskQueue(client, "custom:queue")
	q.groupReady = true
	q.invalidateGroup()
	if q.groupReady {
		t.Fatal("group should be marked not ready after invalidation")
	}
	_ = client.Close()
}

func TestParseDeliveryCarriesGeneration(t *testing.T) {
	delivery, err := parseDelivery(redis.XMessage{
		ID: "1-0",
		Values: map[string]any{
			"task_id":            "17",
			"enqueue_generation": "4",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if delivery.TaskID != 17 || delivery.Generation != 4 {
		t.Fatalf("unexpected delivery: %+v", delivery)
	}
}

func TestParseDeliveryLegacyMessageHasZeroGeneration(t *testing.T) {
	delivery, err := parseDelivery(redis.XMessage{ID: "1-0", Values: map[string]any{"task_id": "17"}})
	if err != nil {
		t.Fatal(err)
	}
	if delivery.TaskID != 17 || delivery.Generation != 0 {
		t.Fatalf("unexpected legacy delivery: %+v", delivery)
	}
}

func TestParseDeliveryMalformedTaskIDCarriesPoisonMetadata(t *testing.T) {
	q := &RedisTaskQueue{key: "phase18:queue"}
	_, err := q.parseRedisDelivery(redis.XMessage{
		ID: "7-0",
		Values: map[string]any{
			"task_id":            "broken",
			"enqueue_generation": "2",
			"marker":             "phase18",
		},
	})
	var poison *PoisonDeliveryError
	if !errors.As(err, &poison) {
		t.Fatalf("error=%v, want PoisonDeliveryError", err)
	}
	if poison.Backend != "redis" || poison.Source != "phase18:queue" || poison.DeliveryID != "7-0" {
		t.Fatalf("unexpected poison metadata: %+v", poison)
	}
	if poison.FailureClass != "decode" || poison.RawEnvelope == "" {
		t.Fatalf("unexpected poison payload: %+v", poison)
	}
}

func TestParseDeliveryMalformedGenerationSalvagesTaskID(t *testing.T) {
	q := &RedisTaskQueue{key: "phase18:queue"}
	_, err := q.parseRedisDelivery(redis.XMessage{
		ID: "8-0",
		Values: map[string]any{
			"task_id":            "42",
			"enqueue_generation": "broken",
		},
	})
	var poison *PoisonDeliveryError
	if !errors.As(err, &poison) {
		t.Fatalf("error=%v, want PoisonDeliveryError", err)
	}
	if poison.TaskID == nil || *poison.TaskID != 42 || poison.Generation != nil {
		t.Fatalf("unexpected salvaged metadata: %+v", poison)
	}
}

func TestKafkaMessageRoundTripGeneration(t *testing.T) {
	want := Message{TaskID: 99, Generation: 7}
	got, err := decodeKafkaMessage(encodeKafkaMessage(want))
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip=%+v, want %+v", got, want)
	}
}
