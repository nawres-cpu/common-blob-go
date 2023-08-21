package commonblobgo

import (
	"bytes"
	"context"
	"io"

	"github.com/Azure/azure-sdk-for-go/storage/azblob"
)

type AzureCloudStorage struct {
	containerURL azblob.ContainerURL
}

func NewAzureCloudStorage(
	ctx context.Context,
	accountName string,
	SASToken string,
	containerName string,
) (*AzureCloudStorage, error) {
	// Create a BlobServiceURL using the provided credentials
	credential, err := azblob.NewSharedKeyCredential(accountName, SASToken)
	if err != nil {
		return nil, err
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	serviceURL := azblob.NewServiceURL(azblob.NewBlobURLParts(accountName, azblob.BlobPipelineOptions{}), pipeline)
	containerURL := serviceURL.NewContainerURL(containerName)

	return &AzureCloudStorage{
		containerURL: containerURL,
	}, nil
}

func (acs *AzureCloudStorage) Attributes(ctx context.Context, key string) (*Attributes, error) {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	props, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return &Attributes{
		CacheControl:       props.CacheControl(),
		ContentDisposition: props.ContentDisposition(),
		ContentEncoding:    props.ContentEncoding(),
		ContentLanguage:    props.ContentLanguage(),
		ContentType:        props.ContentType(),
		ModTime:            props.LastModified(),
		Size:               props.ContentLength(),
		MD5:                nil, // Azure doesn't provide MD5 directly
	}, nil
}

func (acs *AzureCloudStorage) List(ctx context.Context, prefix string) *ListIterator {
	return newListIterator(func() (*ListObject, error) {
		resp, err := acs.containerURL.ListBlobsFlatSegment(ctx, azblob.Marker{}, azblob.ListBlobsSegmentOptions{
			Prefix: prefix,
		})
		if err != nil {
			return nil, err
		}

		if resp.NextMarker.NotDone() {
			return &ListObject{
				Key:   resp.Segment.BlobItems[resp.Segment.BlobItems[:1].Len()-1].Name,
				IsDir: false, // Azure Blob Storage doesn't have the concept of directories
			}, nil
		}

		return nil, io.EOF
	})
}

func (acs *AzureCloudStorage) ListWithOptions(ctx context.Context, options *ListOptions) *ListIterator {
	delimiter := ""
	if options != nil {
		delimiter = options.Delimiter
	}

	return newListIterator(func() (*ListObject, error) {
		resp, err := acs.containerURL.ListBlobsHierarchySegment(ctx, azblob.Marker{}, delimiter, azblob.ListBlobsSegmentOptions{
			Prefix: options.Prefix,
		})
		if err != nil {
			return nil, err
		}

		if resp.NextMarker.NotDone() {
			return &ListObject{
				Key:     resp.Segment.BlobItems[resp.Segment.BlobItems[:1].Len()-1].Name,
				IsDir:   resp.Segment.BlobPrefixes[resp.Segment.BlobPrefixes[:1].Len()-1].Name != "", // Check if it's a blob or a directory
				ModTime: resp.Segment.BlobItems[resp.Segment.BlobItems[:1].Len()-1].Properties.LastModified,
				Size:    resp.Segment.BlobItems[resp.Segment.BlobItems[:1].Len()-1].Properties.ContentLength,
			}, nil
		}

		return nil, io.EOF
	})
}

func (acs *AzureCloudStorage) Get(ctx context.Context, key string) ([]byte, error) {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	resp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, err
	}

	body := make([]byte, resp.ContentLength())
	_, err = resp.Body().Read(body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (acs *AzureCloudStorage) Delete(ctx context.Context, key string) error {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	_, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionNone, azblob.BlobAccessConditions{})
	return err
}

func (acs *AzureCloudStorage) GetSignedURL(ctx context.Context, key string, opts *SignedURLOption) (string, error) {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	sasQueryParams, err := azblob.BlobSASSignatureValues{
		Protocol:           azblob.SASProtocolHTTPS,
		ExpiryTime:         opts.Expiry,
		Permissions:        azblob.BlobSASPermissions{Read: true}.String(),
		ContentType:        opts.ContentType,
		ContentDisposition: azblob.DefaultDownloadResponseContentDisposition,
	}.NewSASQueryParameters(acs.containerURL.Credential)
	if err != nil {
		return "", err
	}
	signedURL := blobURL.URL().String() + "?" + sasQueryParams.Encode()
	return signedURL, nil
}

func (acs *AzureCloudStorage) Write(ctx context.Context, key string, body []byte, contentType *string) error {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	_, err := azblob.UploadBufferToBlockBlob(ctx, body, blobURL, azblob.UploadToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{
			ContentType: *contentType,
		},
	})
	return err
}

func (acs *AzureCloudStorage) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	resp, err := blobURL.Download(ctx, 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return resp.Body(azblob.RetryReaderOptions{}), nil
}

func (acs *AzureCloudStorage) GetRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	resp, err := blobURL.Download(ctx, offset, length, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return resp.Body(azblob.RetryReaderOptions{}), nil
}

func (acs *AzureCloudStorage) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	writer := &azureWriteCloser{
		ctx:     ctx,
		blobURL: acs.containerURL.NewBlockBlobURL(key),
		buffer:  &bytes.Buffer{},
	}
	return writer, nil
}

func (acs *AzureCloudStorage) Exists(ctx context.Context, key string) (bool, error) {
	blobURL := acs.containerURL.NewBlockBlobURL(key)
	_, err := blobURL.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok && serr.Response().StatusCode == 404 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

type azureWriteCloser struct {
	ctx     context.Context
	blobURL azblob.BlockBlobURL
	buffer  *bytes.Buffer
}

func (w *azureWriteCloser) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *azureWriteCloser) Close() error {
	_, err := azblob.UploadBufferToBlockBlob(w.ctx, w.buffer.Bytes(), w.blobURL, azblob.UploadToBlockBlobOptions{
		BlobHTTPHeaders: azblob.BlobHTTPHeaders{
			ContentType: "application/octet-stream",
		},
	})
	return err
}
