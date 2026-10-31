package s3

import (
	"context"
	aws "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"io"
	"strings"
	"testing"
)

type fake struct {
	body    string
	missing bool
	key     string
}

func (f *fake) GetObject(context.Context, *aws.GetObjectInput, ...func(*aws.Options)) (*aws.GetObjectOutput, error) {
	if f.missing {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &aws.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f.body))}, nil
}
func (f *fake) PutObject(_ context.Context, in *aws.PutObjectInput, _ ...func(*aws.Options)) (*aws.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	f.body = string(body)
	f.key = *in.Key
	return &aws.PutObjectOutput{}, err
}
func TestS3StorageSemantics(t *testing.T) {
	f := &fake{}
	s := &S3{Client: f, Bucket: "test", Prefix: "documents/"}
	ctx := context.Background()
	if err := s.Store(ctx, "note", []byte("state")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Fetch(ctx, "note")
	if err != nil || string(got) != "state" || f.key != "documents/note.bin" {
		t.Fatal(string(got), f.key, err)
	}
	f.missing = true
	if got, err = s.Fetch(ctx, "absent"); err != nil || got != nil {
		t.Fatal(got, err)
	}
}
func TestS3ReadLimit(t *testing.T) {
	s := &S3{Client: &fake{body: "too large"}, MaxDocumentBytes: 2}
	if _, err := s.Fetch(context.Background(), "note"); err == nil {
		t.Fatal("read limit ignored")
	}
}
