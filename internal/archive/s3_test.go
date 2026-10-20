package archive

import "testing"

func TestValidS3ObjectKey(t *testing.T) {
	valid := []string{
		"dead-letters/v1/1/abc.json",
		"dead-letters/v1/999/orphan.json",
	}
	for _, key := range valid {
		if !validS3ObjectKey(key) {
			t.Fatalf("expected valid S3 key %q", key)
		}
	}
	invalid := []string{"", "/absolute", "../escape", "dead-letters/../escape", "dead-letters/v1/"}
	for _, key := range invalid {
		if validS3ObjectKey(key) {
			t.Fatalf("expected invalid S3 key %q", key)
		}
	}
}

func TestNewS3SinkValidation(t *testing.T) {
	if _, err := NewS3Sink("", "a", "b", "bucket", "us-east-1", false); err == nil {
		t.Fatal("expected empty endpoint validation error")
	}
	if _, err := NewS3Sink("http://localhost:9000/path", "a", "b", "bucket", "us-east-1", false); err == nil {
		t.Fatal("expected endpoint path validation error")
	}
	if _, err := NewS3Sink("localhost:9000", "", "b", "bucket", "us-east-1", false); err == nil {
		t.Fatal("expected credential validation error")
	}
	if _, err := NewS3Sink("localhost:9000", "a", "b", "", "us-east-1", false); err == nil {
		t.Fatal("expected bucket validation error")
	}
	if _, err := NewS3Sink("http://localhost:9000", "a", "b", "bucket", "us-east-1", true); err != nil {
		t.Fatalf("http endpoint should be accepted: %v", err)
	}
}
