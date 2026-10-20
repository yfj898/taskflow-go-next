package archive

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFileSinkPutGetAndTraversalGuard(t *testing.T) {
	sink, err := NewFileSink(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`{"ok":true}`)
	if err := sink.Put(context.Background(), "dead-letters/v1/1/test.json", want); err != nil {
		t.Fatal(err)
	}
	got, err := sink.Get(context.Background(), "dead-letters/v1/1/test.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("Get() = %q, want %q", got, want)
	}
	if err := sink.Put(context.Background(), "../escape", want); !errors.Is(err, ErrInvalidObjectKey) {
		t.Fatalf("Put traversal error = %v, want %v", err, ErrInvalidObjectKey)
	}
	objects, err := sink.List(context.Background(), "dead-letters/v1")
	if err != nil || len(objects) != 1 || objects[0].Key != "dead-letters/v1/1/test.json" {
		t.Fatalf("List() = %+v, %v", objects, err)
	}
	if err := sink.Delete(context.Background(), "dead-letters/v1/1/test.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Get(context.Background(), "dead-letters/v1/1/test.json"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("Get after Delete error = %v, want %v", err, ErrObjectNotFound)
	}
	named := WithName("replica:file", sink)
	if named.Name() != "replica:file" {
		t.Fatalf("named sink = %q", named.Name())
	}
	if _, ok := named.(Lister); !ok {
		t.Fatal("named file sink lost Lister capability")
	}
}

func TestPackageHashRoundTrip(t *testing.T) {
	data := []byte("phase20")
	if got := SHA256Hex(data); len(got) != 64 {
		t.Fatalf("SHA256Hex length = %d, want 64", len(got))
	}
}

func TestHTTPObjectSinkPutGet(t *testing.T) {
	const token = "phase20-token"
	objects := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			objects[r.URL.Path] = data
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			data, ok := objects[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		case http.MethodDelete:
			delete(objects, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	sink, err := NewHTTPObjectSink(server.URL+"/bucket", token, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`{"phase":20}`)
	if err := sink.Put(context.Background(), "dead-letters/v1/20/test.json", want); err != nil {
		t.Fatal(err)
	}
	got, err := sink.Get(context.Background(), "dead-letters/v1/20/test.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("HTTP Get() = %q, want %q", got, want)
	}
	if err := sink.Delete(context.Background(), "dead-letters/v1/20/test.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.Get(context.Background(), "dead-letters/v1/20/test.json"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("HTTP Get after Delete error = %v", err)
	}
}
