package commonblobgo

import (
	"bytes"
	"context"
	"io"

	commonblobgo "github.com/AccelByte/common-blob-go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOCloudStorage struct {
	client     *minio.Client
	bucketName string
}

// Attributes implements CloudStorage.
func (*MinIOCloudStorage) Attributes(ctx context.Context, key string) (*commonblobgo.Attributes, error) {
	panic("unimplemented")
}

// Close implements CloudStorage.
func (*MinIOCloudStorage) Close() {
	panic("unimplemented")
}

// CreateBucket implements CloudStorage.
func (*MinIOCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	panic("unimplemented")
}

// Exists implements CloudStorage.
func (*MinIOCloudStorage) Exists(ctx context.Context, key string) (bool, error) {
	panic("unimplemented")
}

// GetRangeReader implements CloudStorage.
func (*MinIOCloudStorage) GetRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	panic("unimplemented")
}

// GetReader implements CloudStorage.
func (*MinIOCloudStorage) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	panic("unimplemented")
}

// GetSignedURL implements CloudStorage.
func (*MinIOCloudStorage) GetSignedURL(ctx context.Context, key string, opts *commonblobgo.SignedURLOption) (string, error) {
	panic("unimplemented")
}

// GetWriter implements CloudStorage.
func (*MinIOCloudStorage) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	panic("unimplemented")
}

// ListWithOptions implements CloudStorage.
func (*MinIOCloudStorage) ListWithOptions(ctx context.Context, options *commonblobgo.ListOptions) *commonblobgo.ListIterator {
	panic("unimplemented")
}

func NewMinIOCloudStorage(endpoint, accessKeyID, secretAccessKey, bucketName string, secure bool) (*MinIOCloudStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
		if exists, errBucketExists := client.BucketExists(ctx, bucketName); !exists || errBucketExists != nil {
			return nil, err
		}
	}

	return &MinIOCloudStorage{
		client:     client,
		bucketName: bucketName,
	}, nil
}

func (mc *MinIOCloudStorage) List(ctx context.Context, prefix string) *ListIterator {
	objectsCh := mc.client.ListObjects(ctx, mc.bucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	return newListIterator(func() (*ListObject, error) {
		obj, ok := <-objectsCh
		if !ok {
			return nil, io.EOF
		}

		return &ListObject{
			Key:     obj.Key,
			ModTime: obj.LastModified,
			Size:    obj.Size,
			MD5:     nil,
			IsDir:   false,
		}, nil
	})
}

func (mc *MinIOCloudStorage) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := mc.client.GetObject(ctx, mc.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, obj)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (mc *MinIOCloudStorage) Write(ctx context.Context, key string, body []byte, contentType *string) error {
	_, err := mc.client.PutObject(ctx, mc.bucketName, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{
		ContentType: *contentType,
	})
	return err
}

func (mc *MinIOCloudStorage) Delete(ctx context.Context, key string) error {
	return mc.client.RemoveObject(ctx, mc.bucketName, key, minio.RemoveObjectOptions{})
}
