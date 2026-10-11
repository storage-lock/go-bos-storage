package bos_storage

import (
	"context"
	"os"
	"testing"

	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

func TestBOSStorage(t *testing.T) {
	envName := "STORAGE_LOCK_BOS_ENDPOINT"
	endpoint := os.Getenv(envName)
	if endpoint == "" {
		t.Skip("STORAGE_LOCK_BOS_ENDPOINT not set, skipping BOS test")
	}

	accessKeyID := os.Getenv("STORAGE_LOCK_BOS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("STORAGE_LOCK_BOS_SECRET_ACCESS_KEY")
	bucketName := os.Getenv("STORAGE_LOCK_BOS_BUCKET")

	ctx := context.Background()
	cm := NewBOSConnectionManagerFromConfig(accessKeyID, secretAccessKey, endpoint, bucketName)
	s, err := NewBOSStorage(ctx, &BOSStorageOptions{
		ConnectionManager: cm,
		BucketName:        bucketName,
		KeyPrefix:         storage_test_helper.TestTableName,
		TimeProvider:      &LocalTimeProvider{},
	})
	assert.Nil(t, err)
	storage_test_helper.TestStorage(t, s)
}
