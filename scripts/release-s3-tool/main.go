package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func mustEnv(name string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", name)
		os.Exit(2)
	}
	return value
}

func client() (*minio.Client, string) {
	endpoint := mustEnv("S3_ENDPOINT")
	accessKey := mustEnv("S3_ACCESS_KEY")
	secretKey := mustEnv("S3_SECRET_KEY")
	bucket := mustEnv("S3_BUCKET")
	secure, _ := strconv.ParseBool(os.Getenv("S3_SECURE"))
	region := strings.TrimSpace(os.Getenv("S3_REGION"))
	c, err := minio.New(endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure, Region: region,
	})
	if err != nil {
		panic(err)
	}
	return c, bucket
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: release-s3-tool ensure-bucket|put|exists|get|remove-bucket [key] [data]")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, bucket := client()

	switch os.Args[1] {
	case "ensure-bucket":
		exists, err := c.BucketExists(ctx, bucket)
		if err != nil {
			panic(err)
		}
		if !exists {
			if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: strings.TrimSpace(os.Getenv("S3_REGION"))}); err != nil {
				panic(err)
			}
		}
	case "put":
		if len(os.Args) < 4 {
			os.Exit(2)
		}
		data := []byte(os.Args[3])
		_, err := c.PutObject(ctx, bucket, os.Args[2], bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			panic(err)
		}
	case "exists":
		if len(os.Args) < 3 {
			os.Exit(2)
		}
		if _, err := c.StatObject(ctx, bucket, os.Args[2], minio.StatObjectOptions{}); err != nil {
			os.Exit(3)
		}
	case "get":
		if len(os.Args) < 3 {
			os.Exit(2)
		}
		obj, err := c.GetObject(ctx, bucket, os.Args[2], minio.GetObjectOptions{})
		if err != nil {
			panic(err)
		}
		defer obj.Close()
		data, err := io.ReadAll(obj)
		if err != nil {
			panic(err)
		}
		_, _ = os.Stdout.Write(data)
	case "remove-bucket":
		for item := range c.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			if item.Err != nil {
				panic(item.Err)
			}
			if err := c.RemoveObject(ctx, bucket, item.Key, minio.RemoveObjectOptions{}); err != nil {
				panic(err)
			}
		}
		if err := c.RemoveBucket(ctx, bucket); err != nil {
			panic(err)
		}
	default:
		os.Exit(2)
	}
}
