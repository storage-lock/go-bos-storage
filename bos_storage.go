package bos_storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/baidubce/bce-sdk-go/bce"
	bce_http "github.com/baidubce/bce-sdk-go/http"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
)

const bosValueSeparator = "\x00"

// 对象操作默认走真实客户端。SDK 方法是值接收者且响应体未导出，测试通过替换这些函数覆盖分支。
var (
	bosSendRequest = func(client *bos.Client, req *bce.BceRequest, resp *bce.BceResponse) error {
		return client.SendRequest(req, resp)
	}
	bosGetObject = func(client *bos.Client, bucket, key string) (*api.GetObjectResult, error) {
		return client.GetObject(bucket, key, nil)
	}
	bosPutObjectFromBytes = func(client *bos.Client, bucket, key string, value []byte) (string, error) {
		return client.PutObjectFromBytes(bucket, key, value, nil)
	}
	bosDeleteObject = func(client *bos.Client, bucket, key string) error {
		return client.DeleteObject(bucket, key)
	}
	bosListObjects = func(client *bos.Client, bucket string, args *api.ListObjectsArgs) (*api.ListObjectsResult, error) {
		return client.ListObjects(bucket, args)
	}
)

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
	// 注意：BOS 只满足部分 CAS：
	//   - CreateWithVersion 原子（x-bce-forbid-overwrite 服务端防覆盖）
	//   - UpdateWithVersion/DeleteWithVersion 非原子（BOS SDK 的 PutObject 未暴露
	//     If-Match 条件写，只能 Get-then-Put，并发下互斥性无保障）
	// 因此不声明 CapabilityCAS，避免框架误以为可用于生产分布式锁；
	// 也不声明 CapabilityAtomicDelete，释放只能走非原子删除。
	return []storage.StorageCapability{
		storage.CapabilityReliableTime,
		storage.CapabilityWatch,
	}
}

func (x *BOSStorage) Watch(ctx context.Context, lockId string) (<-chan *storage.LockInformation, error) {
	return storage.NewPollingLockWatcher(x, 500*time.Millisecond).Watch(ctx, lockId)
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

// CreateWithVersion 条件创建：BOS 原生支持 x-bce-forbid-overwrite 防覆盖语义，
// 该头部由服务端原子执行，满足 CAS 要求
func (x *BOSStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)
	value := bosEncodeValue(version, lockInformation.ToJsonString())

	// 使用 x-bce-forbid-overwrite=true：对象已存在时服务端原子拒绝，保证 Create 的原子性。
	// NewBodyFromBytes 只对内存字节计算 MD5，不会失败。
	body, _ := bce.NewBodyFromBytes(value)
	req := &bce.BceRequest{}
	req.SetUri(fmt.Sprintf("/%s/%s", x.options.BucketName, key))
	req.SetMethod("PUT")
	req.SetBody(body)
	req.SetHeader(bce_http.BCE_FORBID_OVERWRITE, "true")
	resp := &bce.BceResponse{}
	if err := bosSendRequest(x.client, req, resp); err != nil {
		if isBosPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	if resp.IsFail() {
		err := resp.ServiceError()
		if isBosPreconditionError(err) {
			return storage_lock.ErrVersionMiss
		}
		return err
	}
	defer func() { resp.Body().Close() }()
	return nil
}

// UpdateWithVersion 条件更新
func (x *BOSStorage) UpdateWithVersion(ctx context.Context, lockId string, expectedVersion, newVersion storage.Version, lockInformation *storage.LockInformation) error {
	key := x.objectKey(lockId)

	result, err := bosGetObject(x.client, x.options.BucketName, key)
	if err != nil {
		if isBosNotFoundError(err) {
			// 记录不存在返回 ErrVersionMiss（可重试），对齐 S3 基类/memory 已修口径。
			return storage_lock.ErrVersionMiss
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
	_, err = bosPutObjectFromBytes(x.client, x.options.BucketName, key, newValue)
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

	result, err := bosGetObject(x.client, x.options.BucketName, key)
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

	err = bosDeleteObject(x.client, x.options.BucketName, key)
	return err
}

func (x *BOSStorage) Get(ctx context.Context, lockId string) (string, error) {
	key := x.objectKey(lockId)
	result, err := bosGetObject(x.client, x.options.BucketName, key)
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
		result, err := bosListObjects(x.client, x.options.BucketName, &api.ListObjectsArgs{
			Prefix:  prefix,
			Marker:  marker,
			MaxKeys: 1000,
		})
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Contents {
			getResult, err := bosGetObject(x.client, x.options.BucketName, obj.Key)
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
