package commonblobgo

import (
	"bytes"
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOCloudStorage struct {
	client     *minio.Client
	bucketName string
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
