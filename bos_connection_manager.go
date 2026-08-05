package bos_storage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/storage-lock/go-storage"
)

// BOSConnectionManager 管理 BOS 客户端连接
type BOSConnectionManager struct {
	AccessKeyID     string
	SecretAccessKey string
	Endpoint        string
	BucketName      string

	presetClient *bos.Client

	initMu      sync.Mutex
	initialized bool
	client      *bos.Client
}

var _ storage.ConnectionManager[*bos.Client] = &BOSConnectionManager{}

// NewBOSConnectionManagerFromConfig 从配置创建
func NewBOSConnectionManagerFromConfig(accessKeyID, secretAccessKey, endpoint, bucketName string) *BOSConnectionManager {
	return &BOSConnectionManager{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Endpoint:        endpoint,
		BucketName:      bucketName,
	}
}

// NewBOSConnectionManagerFromClient 从已有客户端创建
func NewBOSConnectionManagerFromClient(client *bos.Client) *BOSConnectionManager {
	return &BOSConnectionManager{
		presetClient: client,
		initialized:  true,
		client:       client,
	}
}

func (x *BOSConnectionManager) Name() string {
	return "bos-connection-manager"
}

func (x *BOSConnectionManager) Take(ctx context.Context) (*bos.Client, error) {
	x.initMu.Lock()
	defer x.initMu.Unlock()

	if x.initialized {
		return x.client, nil
	}

	client, err := bos.NewClient(x.AccessKeyID, x.SecretAccessKey, x.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create BOS client: %w", err)
	}

	x.client = client
	x.initialized = true
	return x.client, nil
}

func (x *BOSConnectionManager) Return(ctx context.Context, connection *bos.Client) error {
	return nil
}

func (x *BOSConnectionManager) Shutdown(ctx context.Context) error {
	x.initMu.Lock()
	defer x.initMu.Unlock()
	x.client = nil
	x.initialized = false
	return nil
}

// objectKey 返回 BOS 对象 key
func (x *BOSConnectionManager) objectKey(lockId string, keyPrefix string) string {
	return fmt.Sprintf("%s/lock/%s", keyPrefix, lockId)
}

// isBosPreconditionError 检查是否为前置条件失败
func isBosPreconditionError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "PreconditionFailed") ||
		strings.Contains(errStr, "412") ||
		strings.Contains(errStr, "condition")
}

// isBosNotFoundError 检查是否为对象不存在
func isBosNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "NoSuchKey") ||
		strings.Contains(errStr, "404") ||
		strings.Contains(errStr, "does not exist")
}