package commonblobgo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/suite"
)

func TestMinIOAPISuite(t *testing.T) {
	// Skip this test suite if required environment variables are not set
	if testing.Short() {
		t.Skip("Skipping MinIO API Suite in short mode.")
	}

	err := godotenv.Load()
	if err != nil {
		t.Fatalf("Error while loading the .env file: %v", err)
	}

	minIOEndpoint := os.Getenv("localhost:9000")
	minIOAccessKeyID := os.Getenv("MINIO_ACCESS_KEY")
	minIOSecretAccessKey := os.Getenv("MINIO_SECRET_KEY")

	if minIOEndpoint == "" || minIOAccessKeyID == "" || minIOSecretAccessKey == "" {
		t.Skip("Skipped. Required ENV variables for MinIO are not set.")
		return
	}

	suite.Run(t, &MinIOSuite{
		minIOEndpoint:        minIOEndpoint,
		minIOAccessKeyID:     minIOAccessKeyID,
		minIOSecretAccessKey: minIOSecretAccessKey,
		bucketName:           "common-blob-go",
	})
}

type MinIOSuite struct {
	suite.Suite

	storage              CloudStorage
	ctx                  context.Context
	minIOEndpoint        string
	minIOAccessKeyID     string
	minIOSecretAccessKey string
	bucketName           string
}

func (s *MinIOSuite) SetupSuite() {
	ctx := context.Background()

	storage, err := NewMinIOCloudStorage(
		s.minIOEndpoint,
		s.minIOAccessKeyID,
		s.minIOSecretAccessKey,
		s.bucketName,
		false,
	)
	s.Require().NoError(err)
	s.Require().NotNil(storage)

	s.ctx = ctx
}

func (s *MinIOSuite) generateFileName() string {
	return fmt.Sprintf("%s.json", uuid.New().String())
}

func (s *MinIOSuite) TestAttributes() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	attrs, err := s.storage.Attributes(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().NotNil(attrs)

	s.Require().Equal(attrs.ContentType, contentType)

	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)
}
func (s *MinIOSuite) TestExists() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	exists, err := s.storage.Exists(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().False(exists)

	err = s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	exists, err = s.storage.Exists(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().True(exists)

	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)

	exists, err = s.storage.Exists(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().False(exists)
}
func (s *MinIOSuite) TestGetRangeReader() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	// Get a range reader
	offset := int64(5)
	length := int64(5)
	reader, err := s.storage.GetRangeReader(s.ctx, fileName, offset, length)
	s.Require().NoError(err)
	s.Require().NotNil(reader)

	// Read the data and compare
	buf := make([]byte, length)
	n, err := reader.Read(buf)
	s.Require().Equal(int(length), n)
	s.Require().NoError(err)
	s.Require().Equal([]byte(`{"key":`), buf)

	// Cleanup
	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)
}

func (s *MinIOSuite) TestGetReader() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	// Get a reader
	reader, err := s.storage.GetReader(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().NotNil(reader)

	// Read the data and compare
	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	s.Require().NoError(err)
	s.Require().Equal([]byte(`{"key": "value"}`), buf.Bytes())

	// Cleanup
	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)
}
func (s *MinIOSuite) TestGetSignedURL() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	// Set the duration for which the signed URL will be valid (e.g., 1 hour)
	expirationDuration := time.Hour

	opts := &SignedURLOption{
		Expiry: expirationDuration,
		Method: "GET",
	}
	url, err := s.storage.GetSignedURL(s.ctx, fileName, opts)
	s.Require().NoError(err)
	s.Require().NotEmpty(url)
	// Cleanup
	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)
}

func (s *MinIOSuite) TestGetWriter() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)

	writer, err := s.storage.GetWriter(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().NotNil(writer)

	_, err = writer.Write(body)
	s.Require().NoError(err)

	err = writer.Close()
	s.Require().NoError(err)

	storedBody, err := s.storage.Get(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().Equal(body, storedBody)

	// Cleanup
	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)
}

func (s *MinIOSuite) TestWriteAndGet() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	storedBody, err := s.storage.Get(s.ctx, fileName)
	s.Require().NoError(err)
	s.Require().NotEmpty(storedBody)

	s.Require().JSONEq(string(body), string(storedBody))
}
func (s *MinIOSuite) TestListWithOptions() {
	fileName1 := s.generateFileName()
	fileName2 := s.generateFileName()
	contentType := "application/json"

	// Create and write files
	err := s.storage.Write(s.ctx, fileName1, []byte(`{"key": "value1"}`), &contentType)
	s.Require().NoError(err)
	err = s.storage.Write(s.ctx, fileName2, []byte(`{"key": "value2"}`), &contentType)
	s.Require().NoError(err)

	// List objects with options
	listOptions := &ListOptions{
		Prefix:    "",
		Delimiter: "/",
	}
	list := s.storage.ListWithOptions(s.ctx, listOptions)
	var fileCount int
	for {
		item, err := list.Next(s.ctx)
		if err == io.EOF {
			break
		}
		s.Require().NoError(err)
		if item.Key == fileName1 || item.Key == fileName2 {
			fileCount++
		}
	}
	s.Require().Equal(2, fileCount)

	// Cleanup
	err = s.storage.Delete(s.ctx, fileName1)
	s.Require().NoError(err)
	err = s.storage.Delete(s.ctx, fileName2)
	s.Require().NoError(err)
}

func (s *MinIOSuite) TestList() {
	fileName1 := s.generateFileName()
	fileName2 := s.generateFileName()
	contentType := "application/json"

	// Create and write files
	err := s.storage.Write(s.ctx, fileName1, []byte(`{"key": "value1"}`), &contentType)
	s.Require().NoError(err)
	err = s.storage.Write(s.ctx, fileName2, []byte(`{"key": "value2"}`), &contentType)
	s.Require().NoError(err)

	// List objects
	list := s.storage.List(s.ctx, "")
	var fileCount int
	for {
		item, err := list.Next(s.ctx)
		if err == io.EOF {
			break
		}
		s.Require().NoError(err)
		if item.Key == fileName1 || item.Key == fileName2 {
			fileCount++
		}
	}
	s.Require().Equal(2, fileCount)
}

func (s *MinIOSuite) TestDelete() {
	fileName := s.generateFileName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, fileName, body, &contentType)
	s.Require().NoError(err)

	err = s.storage.Delete(s.ctx, fileName)
	s.Require().NoError(err)

	_, err = s.storage.Get(s.ctx, fileName)
	s.Require().Error(err)
}
