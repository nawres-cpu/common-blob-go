package main

import (
	"context"
	"fmt"
	"log"

	blob "github.com/AccelByte/common-blob-go"
)

func main() {
	cloudStorageOpts := blob.CloudStorageOption{
		MinIOEndpoint:        "127.0.0.1:9000",
		MinIOAccessKeyID:     "minioadmin",
		MinIOSecretAccessKey: "minioadmin",
		MinIOBucketName:      "blob_test",
		MinIOSecure:          false,
	}

	storage, err := blob.NewCloudStorageWithOption(context.Background(), true, "minio", "blob_test", cloudStorageOpts)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	storage.Exists(context.Background(), "buck")
	storage.Get(context.Background(), "pk")
	ctx := context.Background()
	attrs, err := storage.Attributes(ctx, "go.mod")
	if err != nil {
		log.Fatal("Error getting attributes:", err)
	}

	log.Println("File Size:", attrs.Size)
}
