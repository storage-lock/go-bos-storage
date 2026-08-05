package bos_storage

import (
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/storage-lock/go-storage"
)

// BOSStorageOptions 百度云 BOS 存储的选项
type BOSStorageOptions struct {
	ConnectionManager storage.ConnectionManager[*bos.Client]
	BucketName        string
	KeyPrefix         string
	TimeProvider      storage.TimeProvider
}

func NewBOSStorageOptions() *BOSStorageOptions {
	return &BOSStorageOptions{
		KeyPrefix: storage.DefaultStorageTableName,
	}
}

func (x *BOSStorageOptions) SetConnectionManager(cm storage.ConnectionManager[*bos.Client]) *BOSStorageOptions {
	x.ConnectionManager = cm
	return x
}

func (x *BOSStorageOptions) SetBucketName(bucketName string) *BOSStorageOptions {
	x.BucketName = bucketName
	return x
}

func (x *BOSStorageOptions) SetKeyPrefix(keyPrefix string) *BOSStorageOptions {
	x.KeyPrefix = keyPrefix
	return x
}

func (x *BOSStorageOptions) SetTimeProvider(tp storage.TimeProvider) *BOSStorageOptions {
	x.TimeProvider = tp
	return x
}

func (x *BOSStorageOptions) Check() error {
	if x.KeyPrefix == "" {
		return ErrKeyPrefixEmpty
	}
	if x.BucketName == "" {
		return ErrBucketNameEmpty
	}
	if x.ConnectionManager == nil {
		return ErrConnectionManagerNil
	}
	return nil
}