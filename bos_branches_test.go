package bos_storage

import (
	"context"
	"errors"
	"testing"

	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/storage-lock/go-storage"
	"github.com/stretchr/testify/assert"
)

type takeErrorManager struct{}

func (takeErrorManager) Name() string { return "take-error" }
func (takeErrorManager) Take(context.Context) (*bos.Client, error) {
	return nil, errors.New("take failed")
}
func (takeErrorManager) Return(context.Context, *bos.Client) error { return nil }
func (takeErrorManager) Shutdown(context.Context) error            { return nil }

func TestBOSConnectionManager(t *testing.T) {
	preset := &bos.Client{}
	fromClient := NewBOSConnectionManagerFromClient(preset)
	assert.Equal(t, "bos-connection-manager", fromClient.Name())
	got, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, preset, got)
	again, err := fromClient.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, got, again)
	assert.NoError(t, fromClient.Return(context.Background(), got))
	assert.Equal(t, "locks/lock/id", fromClient.objectKey("id", "locks"))
	assert.NoError(t, fromClient.Shutdown(context.Background()))

	orig := bosNewClient
	defer func() { bosNewClient = orig }()
	bosNewClient = func(string, string, string) (*bos.Client, error) { return nil, errors.New("open failed") }
	fromConfig := NewBOSConnectionManagerFromConfig("ak", "sk", "https://bos.example.com", "bucket")
	_, err = fromConfig.Take(context.Background())
	assert.Error(t, err)
	opened := &bos.Client{}
	bosNewClient = func(string, string, string) (*bos.Client, error) { return opened, nil }
	got, err = fromConfig.Take(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, opened, got)
	assert.NoError(t, fromConfig.Shutdown(context.Background()))
	_, err = fromConfig.Take(context.Background())
	assert.NoError(t, err)
}

func TestBOSStorage_OfflineBranches(t *testing.T) {
	assert.Panics(t, func() { _, _ = NewBOSStorage(context.Background(), nil) })
	_, err := NewBOSStorage(context.Background(), NewBOSStorageOptions())
	assert.ErrorIs(t, err, ErrBucketNameEmpty)
	opts := NewBOSStorageOptions().SetKeyPrefix("locks")
	assert.ErrorIs(t, opts.Check(), ErrBucketNameEmpty)
	opts.SetBucketName("bucket")
	assert.ErrorIs(t, opts.Check(), ErrConnectionManagerNil)

	_, err = NewBOSStorage(context.Background(), &BOSStorageOptions{ConnectionManager: takeErrorManager{}, BucketName: "bucket", KeyPrefix: "locks"})
	assert.EqualError(t, err, "take failed")

	s, err := NewBOSStorage(context.Background(), NewBOSStorageOptions().
		SetConnectionManager(NewBOSConnectionManagerFromClient(&bos.Client{})).
		SetBucketName("bucket").
		SetKeyPrefix("locks").
		SetTimeProvider(&LocalTimeProvider{}))
	assert.NoError(t, err)
	assert.Equal(t, BOSStorageName, s.GetName())
	assert.Len(t, s.Capabilities(), 2)
	assert.Equal(t, "locks/lock/id", s.objectKey("id"))
	encoded := bosEncodeValue(7, `{"owner_id":"owner"}`)
	version, jsonText, err := bosDecodeValue(encoded)
	assert.NoError(t, err)
	assert.Equal(t, storage.Version(7), version)
	assert.Contains(t, jsonText, "owner")
	_, _, err = bosDecodeValue([]byte("bad"))
	assert.Error(t, err)
	_, _, err = bosDecodeValue([]byte("bad\x00json"))
	assert.Error(t, err)
	now, err := s.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
	assert.NoError(t, s.Close(context.Background()))
	_, err = (&BOSStorage{}).Watch(context.Background(), "")
	assert.Error(t, err)

	assert.False(t, isBosPreconditionError(nil))
	assert.True(t, isBosPreconditionError(errors.New("PreconditionFailed")))
	assert.True(t, isBosPreconditionError(errors.New("status 412")))
	assert.True(t, isBosPreconditionError(errors.New("condition failed")))
	assert.False(t, isBosPreconditionError(errors.New("other")))
	assert.False(t, isBosNotFoundError(nil))
	assert.True(t, isBosNotFoundError(errors.New("NoSuchKey")))
	assert.True(t, isBosNotFoundError(errors.New("404")))
	assert.True(t, isBosNotFoundError(errors.New("does not exist")))
	assert.False(t, isBosNotFoundError(errors.New("other")))

	plain, err := NewBOSStorage(context.Background(), &BOSStorageOptions{
		ConnectionManager: NewBOSConnectionManagerFromClient(&bos.Client{}),
		BucketName:        "bucket",
		KeyPrefix:         "locks",
	})
	assert.NoError(t, err)
	now, err = plain.GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, now.IsZero())
}
