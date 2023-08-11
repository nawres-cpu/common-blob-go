package commonblobgo

import (
	"bytes"
	"context"
	"io"
	"net/url"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOCloudStorage struct {
	client     *minio.Client
	bucketName string
}

// Attributes implements CloudStorage.
func (mc *MinIOCloudStorage) Attributes(ctx context.Context, key string) (*Attributes, error) {
	objInfo, err := mc.client.StatObject(ctx, mc.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	attrs := &Attributes{
		CacheControl:       "",
		ContentDisposition: "",
		ContentEncoding:    "",
		ContentLanguage:    "",
		ContentType:        objInfo.ContentType,
		ModTime:            objInfo.LastModified,
		Size:               objInfo.Size,
		MD5:                nil, // MinIO doesn't provide MD5 directly
	}

	return attrs, nil
}

// Close implements CloudStorage.
func (mc *MinIOCloudStorage) Close() {
	mc.Close()
}

// CreateBucket implements CloudStorage.
func (mc *MinIOCloudStorage) CreateBucket(ctx context.Context, bucketPrefix string, expirationTimeDays int64) error {
	bucketName := bucketPrefix + "-" + uuid.New().String() // Generate a unique bucket name

	// Create the bucket using MinIO client
	err := mc.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	return nil
}

// Exists implements CloudStorage.
func (mc *MinIOCloudStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, err := mc.client.StatObject(ctx, mc.bucketName, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetRangeReader implements CloudStorage.
func (mc *MinIOCloudStorage) GetRangeReader(ctx context.Context, key string, offset int64, length int64) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	opts.SetRange(offset, offset+length-1)
	obj, err := mc.client.GetObject(ctx, mc.bucketName, key, opts)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// GetReader implements CloudStorage.
func (mc *MinIOCloudStorage) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := mc.client.GetObject(ctx, mc.bucketName, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

// GetSignedURL implements CloudStorage.
func (mc *MinIOCloudStorage) GetSignedURL(ctx context.Context, key string, opts *SignedURLOption) (string, error) {
	reqParams := url.Values{}
	if opts != nil {
		if opts.Method != "" {
			reqParams.Set("response-content-disposition", "attachment; filename=\""+key+"\"")
		}
	}

	url, err := mc.client.PresignedGetObject(ctx, mc.bucketName, key, opts.Expiry, reqParams)
	if err != nil {
		return "", err
	}

	return url.String(), nil
}

// GetWriter implements CloudStorage.
func (mc *MinIOCloudStorage) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	buffer := &bytes.Buffer{}
	writer := &minIOWriteCloser{
		buffer: buffer,
	}
	return writer, nil
}

// ListWithOptions implements CloudStorage.
func (mc *MinIOCloudStorage) ListWithOptions(ctx context.Context, options *ListOptions) *ListIterator {
	objectsCh := mc.client.ListObjects(ctx, mc.bucketName, minio.ListObjectsOptions{
		Prefix:    options.Prefix,
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
			MD5:     nil, // MinIO doesn't provide MD5 directly
			IsDir:   false,
		}, nil
	})
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

type minIOWriteCloser struct {
	buffer *bytes.Buffer
}

func (wc *minIOWriteCloser) Write(p []byte) (n int, err error) {
	return wc.buffer.Write(p)
}
func (wc *minIOWriteCloser) Close() error {
	return nil
}
