package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Sink stores immutable DLQ archive packages in an Amazon S3-compatible
// object store. MinIO is used by the release-evidence integration, while the
// client contract remains portable to other S3-compatible implementations.
type S3Sink struct {
	client *minio.Client
	bucket string
}

func NewS3Sink(endpoint, accessKey, secretKey, bucket, region string, secure bool) (*S3Sink, error) {
	endpoint = strings.TrimSpace(endpoint)
	bucket = strings.TrimSpace(bucket)
	if endpoint == "" {
		return nil, errors.New("S3 archive endpoint is required")
	}
	if accessKey == "" || secretKey == "" {
		return nil, errors.New("S3 archive credentials are required")
	}
	if bucket == "" {
		return nil, errors.New("S3 archive bucket is required")
	}

	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("invalid S3 archive endpoint %q", endpoint)
		}
		if u.Path != "" && u.Path != "/" {
			return nil, fmt.Errorf("S3 archive endpoint must not contain a path: %q", endpoint)
		}
		secure = u.Scheme == "https"
		endpoint = u.Host
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
		Region: strings.TrimSpace(region),
	})
	if err != nil {
		return nil, err
	}
	return &S3Sink{client: client, bucket: bucket}, nil
}

func (s *S3Sink) Name() string { return "s3" }

func validS3ObjectKey(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return false
	}
	clean := path.Clean(key)
	return clean == key && clean != "." && clean != ".." && !strings.HasPrefix(clean, "../")
}

func (s *S3Sink) Put(ctx context.Context, key string, data []byte) error {
	if !validS3ObjectKey(key) {
		return ErrInvalidObjectKey
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}

func (s *S3Sink) Get(ctx context.Context, key string) ([]byte, error) {
	if !validS3ObjectKey(key) {
		return nil, ErrInvalidObjectKey
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, classifyS3Error(err)
	}
	defer obj.Close()
	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, classifyS3Error(err)
	}
	return data, nil
}

func (s *S3Sink) Delete(ctx context.Context, key string) error {
	if !validS3ObjectKey(key) {
		return ErrInvalidObjectKey
	}
	return classifyS3Error(s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}))
}

func (s *S3Sink) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		prefix = strings.TrimSuffix(prefix, "/") + "/"
		if !validS3ObjectKey(strings.TrimSuffix(prefix, "/")) {
			return nil, ErrInvalidObjectKey
		}
	}
	result := make([]ObjectInfo, 0)
	for item := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if item.Err != nil {
			return nil, classifyS3Error(item.Err)
		}
		result = append(result, ObjectInfo{Key: item.Key, SizeBytes: item.Size, ModifiedAt: item.LastModified})
	}
	return result, nil
}

func classifyS3Error(err error) error {
	if err == nil {
		return nil
	}
	response := minio.ToErrorResponse(err)
	switch response.Code {
	case "NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound":
		return fmt.Errorf("%w: %v", ErrObjectNotFound, err)
	default:
		return err
	}
}
