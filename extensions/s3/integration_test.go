//go:build integration

package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"os"
	"testing"
	"time"
)

func TestRealS3Roundtrip(t *testing.T) {
	endpoint := os.Getenv("TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Fatal("TEST_S3_ENDPOINT is required for integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := sdk.New(sdk.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Credentials: aws.AnonymousCredentials{}})
	bucket := "hocuspocus-go-test"
	if _, err := client.CreateBucket(ctx, &sdk.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = client.DeleteBucket(context.Background(), &sdk.DeleteBucketInput{Bucket: &bucket}) })
	storage := &S3{Client: client, Bucket: bucket, Prefix: "documents/"}
	if err := storage.Store(ctx, "note", []byte{0, 1, 2, 3, 255}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		key := storage.Key("note")
		_, _ = client.DeleteObject(context.Background(), &sdk.DeleteObjectInput{Bucket: &bucket, Key: &key})
	})
	got, err := storage.Fetch(ctx, "note")
	if err != nil || string(got) != string([]byte{0, 1, 2, 3, 255}) {
		t.Fatal(got, err)
	}
	missing, err := storage.Fetch(ctx, "missing")
	if err != nil || missing != nil {
		t.Fatal(missing, err)
	}
}
