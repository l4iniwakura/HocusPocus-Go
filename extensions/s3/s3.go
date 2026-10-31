// Package s3 persists binary documents through the AWS SDK v2, including
// S3-compatible endpoints configured on the injected client.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/extensions/database"
)

type Client interface {
	GetObject(context.Context, *awss3.GetObjectInput, ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	PutObject(context.Context, *awss3.PutObjectInput, ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
}
type S3 struct {
	Client           Client
	Bucket, Prefix   string
	MaxDocumentBytes int64
}

func New(client Client, bucket string) *S3 {
	return &S3{Client: client, Bucket: bucket, Prefix: "hocuspocus-documents/"}
}
func (*S3) Priority() int { return 100 }
func (*S3) Handles(h hp.Hook) bool {
	return h == hp.OnConfigure || h == hp.OnLoadDocument || h == hp.OnStoreDocument
}
func (s *S3) Handle(ctx context.Context, h hp.Hook, p *hp.Payload) error {
	if h == hp.OnConfigure {
		if s.Client == nil || s.Bucket == "" {
			return errors.New("S3 client and bucket required")
		}
		return nil
	}
	return (&database.Database{Storage: s}).Handle(ctx, h, p)
}
func (s *S3) Key(name string) string { return s.Prefix + name + ".bin" }
func (s *S3) Fetch(ctx context.Context, name string) ([]byte, error) {
	out, err := s.Client.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.Key(name))})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return nil, nil
		}
		return nil, err
	}
	if out.Body == nil {
		return nil, nil
	}
	defer out.Body.Close()
	limit := s.MaxDocumentBytes
	if limit <= 0 {
		limit = 64 << 20
	}
	data, err := io.ReadAll(io.LimitReader(out.Body, limit+1))
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("S3 document exceeds %d bytes", limit)
	}
	return data, err
}
func (s *S3) Store(ctx context.Context, name string, state []byte) error {
	_, err := s.Client.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(s.Key(name)), Body: bytes.NewReader(state), ContentType: aws.String("application/octet-stream")})
	return err
}
