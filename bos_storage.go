package bos_storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

const bosValueSeparator = "\x00"

// BOSStorage 基于百度云 BOS 的存储引擎实现
type BOSStorage struct {
	options      *BOSStorageOptions
	client       *bos.Client
	timeProvider storage.TimeProvider
}

var _ storage.Storage = &BOSStorage{}

func NewBOSStorage(ctx context.Context, options *BOSStorageOptions) (*BOSStorage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	s := &BOSStorage{options: options}
	if err := s.Init(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

const BOSStorageName = "bos-storage"

func (x *BOSStorage) GetName() string {
	return BOSStorageName
}

func (x *BOSStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{
		storage.CapabilityCAS,
		storage.CapabilityReliableTime,
	}
}

func (x *BOSStorage) Init(ctx context.Context) error {
	client, err := x.options.ConnectionManager.Take(ctx)
	if err != nil {
		return err
	}
	x.client = client

	if x.options.TimeProvider != nil {
		x.timeProvider = x.options.TimeProvider
	} else {
		x.timeProvider = &LocalTimeProvider{}
	}
	return nil
}

type LocalTimeProvider struct{}

func (p *LocalTimeProvider) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}

func (x *BOSStorage) objectKey(lockId string) string {
	return fmt.Sprintf("%s/lock/%s", x.options.KeyPrefix, lockId)
}

func bosEncodeValue(version storage.Version, jsonStr string) []byte {
	return []byte(strconv.FormatUint(uint64(version), 10) + bosValueSeparator + jsonStr)
}

func bosDecodeValue(data []byte) (storage.Version, string, error) {
	parts := strings.SplitN(string(data), bosValueSeparator, 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("invalid bos value format")
	}
	version, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, "", err
	}
	return storage.Version(version), parts[1], nil
}

// CreateWithVersion 条件创建
func (x *BOSStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)
	value := bosEncodeValue(version, lockInformation.ToJsonString())

	_, err := x.client.PutObjectFromBytes(x.options.BucketName, key, value, nil)
	if err != nil {
		if isBosPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// UpdateWithVersion 条件更新
func (x *BOSStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	result, err := x.client.GetObject(x.options.BucketName, key, nil)
	if err != nil {
		if isBosNotFoundError(err) {
			return storage_lock.ErrLockNotFound
		}
		return err
	}
	data, err := io.ReadAll(result.Body)
	result.Body.Close()
	if err != nil {
		return err
	}

	currentVersion, _, err := bosDecodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	newValue := bosEncodeValue(newVersion, lockInformation.ToJsonString())
	_, err = x.client.PutObjectFromBytes(x.options.BucketName, key, newValue, nil)
	if err != nil {
		if isBosPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	return nil
}

// DeleteWithVersion 降级删除
func (x *BOSStorage) DeleteWithVersion(ctx context.Context, lockId string, expectedVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	result, err := x.client.GetObject(x.options.BucketName, key, nil)
	if err != nil {
		if isBosNotFoundError(err) {
			return storage_lock.ErrLockNotFound
		}
		return err
	}
	data, err := io.ReadAll(result.Body)
	result.Body.Close()
	if err != nil {
		return err
	}

	currentVersion, _, err := bosDecodeValue(data)
	if err != nil {
		return err
	}
	if currentVersion != expectedVersion {
		return storage_lock.ErrVersionMiss
	}

	err = x.client.DeleteObject(x.options.BucketName, key)
	return err
}

func (x *BOSStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.objectKey(lockId)
	result, err := x.client.GetObject(x.options.BucketName, key, nil)
	if err != nil {
		if isBosNotFoundError(err) {
			return "", storage_lock.ErrLockNotFound
		}
		return "", err
	}
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", storage_lock.ErrLockNotFound
	}
	_, jsonStr, err := bosDecodeValue(data)
	if err != nil {
		return "", err
	}
	return jsonStr, nil
}

func (x *BOSStorage) GetTime(ctx context.Context) (time.Time, error) {
	return x.timeProvider.GetTime(ctx)
}

func (x *BOSStorage) Close(ctx context.Context) error {
	x.client = nil
	return nil
}

func (x *BOSStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	prefix := fmt.Sprintf("%s/lock/", x.options.KeyPrefix)
	slice := make([]*storage.LockInformation, 0)

	marker := ""
	for {
		result, err := x.client.ListObjects(x.options.BucketName, &api.ListObjectsArgs{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Contents {
			getResult, err := x.client.GetObject(x.options.BucketName, obj.Key, nil)
			if err != nil {
				continue
			}
			data, err := io.ReadAll(getResult.Body)
			getResult.Body.Close()
			if err != nil || len(data) == 0 {
				continue
			}
			_, jsonStr, err := bosDecodeValue(data)
			if err != nil {
				continue
			}
			lockInfo, err := storage.LockInformationFromJsonString(jsonStr)
			if err != nil {
				continue
			}
			slice = append(slice, lockInfo)
		}

		if !result.IsTruncated {
			break
		}
		marker = result.NextMarker
	}

	return go_iterator.FromSlice(slice), nil
}