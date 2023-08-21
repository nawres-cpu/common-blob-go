package commonblobgo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

func TestAzureAPISuite(t *testing.T) {
	// Skip this test suite if required environment variables are not set
	if testing.Short() {
		t.Skip("Skipping Azure API Suite in short mode.")
	}

	suite.Run(t, &AzureSuite{
		azureAccountName:   "n987a654w321",
		azureSASToken:      "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-08-17T17:48:59Z&st=2023-08-17T09:48:59Z&spr=https&sig=8vXEqpt8L9EHDYlwhRxxDDX09%2BbHQMYvDiN2wlaRhoQ%3D",
		azureContainerName: "first",
	})
}

type AzureSuite struct {
	suite.Suite

	storage            CloudStorage
	ctx                context.Context
	azureAccountName   string
	azureSASToken      string
	azureContainerName string
}

func (s *AzureSuite) SetupSuite() {
	ctx := context.Background()

	storage, err := NewAzureCloudStorage(
		ctx,
		s.azureAccountName,
		s.azureSASToken,
		s.azureContainerName,
	)
	s.Require().NoError(err)
	s.Require().NotNil(storage)

	s.ctx = ctx
}

func (s *AzureSuite) generateBlobName() string {
	return fmt.Sprintf("%s.json", uuid.New().String())
}

func (s *AzureSuite) TestAttributes() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	attrs, err := s.storage.Attributes(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().NotNil(attrs)

	s.Require().Equal(attrs.ContentType, contentType)

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)
}

func (s *AzureSuite) TestDelete() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)

	exists, err := s.storage.Exists(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().False(exists)
}
func (s *AzureSuite) TestExists() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	exists, err := s.storage.Exists(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().False(exists)

	err = s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	exists, err = s.storage.Exists(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().True(exists)

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)

	exists, err = s.storage.Exists(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().False(exists)
}

func (s *AzureSuite) TestGetRangeReader() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	offset := int64(5)
	length := int64(5)
	reader, err := s.storage.GetRangeReader(s.ctx, blobName, offset, length)
	s.Require().NoError(err)
	s.Require().NotNil(reader)

	buf := make([]byte, length)
	n, err := reader.Read(buf)
	s.Require().Equal(int(length), n)
	s.Require().NoError(err)
	s.Require().Equal([]byte(`{"key":`), buf)

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)
}

func (s *AzureSuite) TestGetReader() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	reader, err := s.storage.GetReader(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().NotNil(reader)

	var buf bytes.Buffer
	_, err = io.Copy(&buf, reader)
	s.Require().NoError(err)
	s.Require().Equal([]byte(`{"key": "value"}`), buf.Bytes())

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)
}

func (s *AzureSuite) TestGetSignedURL() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	expirationDuration := time.Hour
	opts := &SignedURLOption{
		Expiry: expirationDuration,
		Method: "GET",
	}
	url, err := s.storage.GetSignedURL(s.ctx, blobName, opts)
	s.Require().NoError(err)
	s.Require().NotEmpty(url)

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)
}

func (s *AzureSuite) TestGetWriter() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)

	writer, err := s.storage.GetWriter(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().NotNil(writer)

	_, err = writer.Write(body)
	s.Require().NoError(err)

	err = writer.Close()
	s.Require().NoError(err)

	storedBody, err := s.storage.Get(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().Equal(body, storedBody)

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)
}

func (s *AzureSuite) TestWriteAndGet() {
	blobName := s.generateBlobName()
	body := []byte(`{"key": "value"}`)
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName, body, &contentType)
	s.Require().NoError(err)

	storedBody, err := s.storage.Get(s.ctx, blobName)
	s.Require().NoError(err)
	s.Require().NotEmpty(storedBody)

	s.Require().JSONEq(string(body), string(storedBody))

	err = s.storage.Delete(s.ctx, blobName)
	s.Require().NoError(err)
}

func (s *AzureSuite) TestListWithOptions() {
	blobName1 := s.generateBlobName()
	blobName2 := s.generateBlobName()
	contentType := "application/json"

	err := s.storage.Write(s.ctx, blobName1, []byte(`{"key": "value1"}`), &contentType)
	s.Require().NoError(err)
	err = s.storage.Write(s.ctx, blobName2, []byte(`{"key": "value2"}`), &contentType)
	s.Require().NoError(err)

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
		if item.Key == blobName1 || item.Key == blobName2 {
			fileCount++
		}
	}
	s.Require().Equal(2, fileCount)

	err = s.storage.Delete(s.ctx, blobName1)
	s.Require().NoError(err)
	err = s.storage.Delete(s.ctx, blobName2)
	s.Require().NoError(err)
}
