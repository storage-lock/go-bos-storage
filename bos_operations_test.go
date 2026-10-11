package bos_storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/baidubce/bce-sdk-go/bce"
	bce_http "github.com/baidubce/bce-sdk-go/http"
	"github.com/baidubce/bce-sdk-go/services/bos"
	"github.com/baidubce/bce-sdk-go/services/bos/api"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

func newTestBOSStorage(t *testing.T) *BOSStorage {
	t.Helper()
	s, err := NewBOSStorage(context.Background(), &BOSStorageOptions{
		ConnectionManager: NewBOSConnectionManagerFromClient(&bos.Client{}),
		BucketName:        "bucket",
		KeyPrefix:         "locks",
	})
	assert.NoError(t, err)
	return s
}

func restoreBOSOps(t *testing.T) {
	t.Helper()
	send, get, put, del, list := bosSendRequest, bosGetObject, bosPutObjectFromBytes, bosDeleteObject, bosListObjects
	t.Cleanup(func() {
		bosSendRequest, bosGetObject, bosPutObjectFromBytes, bosDeleteObject, bosListObjects = send, get, put, del, list
	})
}

type failReader struct{ err error }

func (r failReader) Read([]byte) (int, error) { return 0, r.err }

func lockJSON(owner string) string {
	return (&storage.LockInformation{LockId: "id", OwnerId: owner}).ToJsonString()
}

func TestBOSStorage_CreateWithVersion(t *testing.T) {
	s := newTestBOSStorage(t)
	restoreBOSOps(t)
	info := &storage.LockInformation{LockId: "id", OwnerId: "owner"}

	bosSendRequest = func(_ *bos.Client, _ *bce.BceRequest, resp *bce.BceResponse) error {
		httpResp := &http.Response{StatusCode: 200, Status: "200 OK", Body: io.NopCloser(bytes.NewReader(nil)), Header: make(http.Header)}
		wrapped := &bce_http.Response{}
		wrapped.SetHttpResponse(httpResp)
		resp.SetHttpResponse(wrapped)
		return nil
	}
	assert.NoError(t, s.CreateWithVersion(context.Background(), "id", 1, info))

	bosSendRequest = func(*bos.Client, *bce.BceRequest, *bce.BceResponse) error {
		return errors.New("PreconditionFailed")
	}
	assert.ErrorIs(t, s.CreateWithVersion(context.Background(), "id", 1, info), storage_lock.ErrVersionMiss)

	bosSendRequest = func(*bos.Client, *bce.BceRequest, *bce.BceResponse) error {
		return errors.New("network down")
	}
	assert.EqualError(t, s.CreateWithVersion(context.Background(), "id", 1, info), "network down")
}

func TestBOSCreate_ResponseBranches(t *testing.T) {
	s := newTestBOSStorage(t)
	restoreBOSOps(t)
	info := &storage.LockInformation{OwnerId: "owner"}

	bosSendRequest = func(_ *bos.Client, _ *bce.BceRequest, resp *bce.BceResponse) error {
		httpResp := &http.Response{
			StatusCode: 412,
			Status:     "412 Precondition Failed",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}
		wrapped := &bce_http.Response{}
		wrapped.SetHttpResponse(httpResp)
		resp.SetHttpResponse(wrapped)
		resp.ParseResponse()
		return nil
	}
	assert.ErrorIs(t, s.CreateWithVersion(context.Background(), "id", 2, info), storage_lock.ErrVersionMiss)

	bosSendRequest = func(_ *bos.Client, _ *bce.BceRequest, resp *bce.BceResponse) error {
		httpResp := &http.Response{
			StatusCode: 500,
			Status:     "500 Internal Error",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}
		wrapped := &bce_http.Response{}
		wrapped.SetHttpResponse(httpResp)
		resp.SetHttpResponse(wrapped)
		resp.ParseResponse()
		return nil
	}
	err := s.CreateWithVersion(context.Background(), "id", 2, info)
	assert.Error(t, err)
	assert.NotErrorIs(t, err, storage_lock.ErrVersionMiss)

	bosSendRequest = func(_ *bos.Client, _ *bce.BceRequest, resp *bce.BceResponse) error {
		httpResp := &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}
		wrapped := &bce_http.Response{}
		wrapped.SetHttpResponse(httpResp)
		resp.SetHttpResponse(wrapped)
		resp.ParseResponse()
		return nil
	}
	assert.NoError(t, s.CreateWithVersion(context.Background(), "id", 2, info))
}

func TestBOSStorage_UpdateWithVersion(t *testing.T) {
	s := newTestBOSStorage(t)
	restoreBOSOps(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return nil, errors.New("NoSuchKey")
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return nil, errors.New("timeout")
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "timeout")

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(failReader{errors.New("read failed")})}, nil
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "read failed")

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader([]byte("bad")))}, nil
	}
	assert.Error(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(9, lockJSON("other"))))}, nil
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(1, lockJSON("owner"))))}, nil
	}
	bosPutObjectFromBytes = func(*bos.Client, string, string, []byte) (string, error) {
		return "", errors.New("condition failed")
	}
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), storage_lock.ErrVersionMiss)

	bosPutObjectFromBytes = func(*bos.Client, string, string, []byte) (string, error) {
		return "", errors.New("put failed")
	}
	assert.EqualError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info), "put failed")

	bosPutObjectFromBytes = func(*bos.Client, string, string, []byte) (string, error) { return "etag", nil }
	assert.NoError(t, s.UpdateWithVersion(ctx, "id", 1, 2, info))
}

func TestBOSStorage_DeleteWithVersion(t *testing.T) {
	s := newTestBOSStorage(t)
	restoreBOSOps(t)
	info := &storage.LockInformation{OwnerId: "owner"}
	ctx := context.Background()

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return nil, errors.New("does not exist")
	}
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrLockNotFound)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return nil, errors.New("get failed")
	}
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "get failed")

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(failReader{errors.New("read failed")})}, nil
	}
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "read failed")

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader([]byte("bad")))}, nil
	}
	assert.Error(t, s.DeleteWithVersion(ctx, "id", 1, info))

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(3, lockJSON("owner"))))}, nil
	}
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "id", 1, info), storage_lock.ErrVersionMiss)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(1, lockJSON("owner"))))}, nil
	}
	bosDeleteObject = func(*bos.Client, string, string) error { return errors.New("delete failed") }
	assert.EqualError(t, s.DeleteWithVersion(ctx, "id", 1, info), "delete failed")

	bosDeleteObject = func(*bos.Client, string, string) error { return nil }
	assert.NoError(t, s.DeleteWithVersion(ctx, "id", 1, info))
}

func TestBOSStorage_Get(t *testing.T) {
	s := newTestBOSStorage(t)
	restoreBOSOps(t)
	ctx := context.Background()

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return nil, errors.New("404")
	}
	_, err := s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return nil, errors.New("get failed")
	}
	_, err = s.Get(ctx, "id")
	assert.EqualError(t, err, "get failed")

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(failReader{errors.New("read failed")})}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.EqualError(t, err, "read failed")

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader([]byte("bad")))}, nil
	}
	_, err = s.Get(ctx, "id")
	assert.Error(t, err)

	bosGetObject = func(*bos.Client, string, string) (*api.GetObjectResult, error) {
		return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(1, lockJSON("owner"))))}, nil
	}
	got, err := s.Get(ctx, "id")
	assert.NoError(t, err)
	assert.Contains(t, got, "owner")
}

func TestBOSStorage_List(t *testing.T) {
	s := newTestBOSStorage(t)
	restoreBOSOps(t)
	ctx := context.Background()

	bosListObjects = func(*bos.Client, string, *api.ListObjectsArgs) (*api.ListObjectsResult, error) {
		return nil, errors.New("list failed")
	}
	_, err := s.List(ctx)
	assert.EqualError(t, err, "list failed")

	calls := 0
	bosListObjects = func(_ *bos.Client, _ string, args *api.ListObjectsArgs) (*api.ListObjectsResult, error) {
		calls++
		if args.Marker == "" {
			return &api.ListObjectsResult{
				Contents: []api.ObjectSummaryType{
					{Key: "locks/lock/skip-get"},
					{Key: "locks/lock/skip-read"},
					{Key: "locks/lock/skip-empty"},
					{Key: "locks/lock/skip-decode"},
					{Key: "locks/lock/skip-json"},
					{Key: "locks/lock/ok"},
				},
				IsTruncated: true,
				NextMarker:  "next",
			}, nil
		}
		assert.Equal(t, "next", args.Marker)
		return &api.ListObjectsResult{IsTruncated: false}, nil
	}
	bosGetObject = func(_ *bos.Client, _ string, key string) (*api.GetObjectResult, error) {
		switch key {
		case "locks/lock/skip-get":
			return nil, errors.New("missing")
		case "locks/lock/skip-read":
			return &api.GetObjectResult{Body: io.NopCloser(failReader{errors.New("read")})}, nil
		case "locks/lock/skip-empty":
			return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(nil))}, nil
		case "locks/lock/skip-decode":
			return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader([]byte("bad")))}, nil
		case "locks/lock/skip-json":
			return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(1, "not-json")))}, nil
		default:
			return &api.GetObjectResult{Body: io.NopCloser(bytes.NewReader(bosEncodeValue(1, lockJSON("owner"))))}, nil
		}
	}
	listed, err := s.List(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, calls)
	count := 0
	for listed.Next() {
		count++
		assert.Equal(t, "owner", listed.Value().OwnerId)
	}
	assert.Equal(t, 1, count)
}

func TestBOSDefaultOperations_Delegate(t *testing.T) {
	client, err := bos.NewClient("ak", "sk", "http://127.0.0.1:1")
	assert.NoError(t, err)
	req := &bce.BceRequest{}
	req.SetUri("/bucket/key")
	req.SetMethod("PUT")
	body, err := bce.NewBodyFromBytes([]byte("value"))
	assert.NoError(t, err)
	req.SetBody(body)
	assert.Error(t, bosSendRequest(client, req, &bce.BceResponse{}))
	_, err = bosGetObject(client, "bucket", "key")
	assert.Error(t, err)
	_, err = bosPutObjectFromBytes(client, "bucket", "key", []byte("value"))
	assert.Error(t, err)
	assert.Error(t, bosDeleteObject(client, "bucket", "key"))
	_, err = bosListObjects(client, "bucket", &api.ListObjectsArgs{Prefix: "locks", MaxKeys: 1})
	assert.Error(t, err)
}

func TestBOSOptions_EmptyPrefix(t *testing.T) {
	opts := NewBOSStorageOptions()
	opts.KeyPrefix = ""
	assert.ErrorIs(t, opts.Check(), ErrKeyPrefixEmpty)
	local, err := (&LocalTimeProvider{}).GetTime(context.Background())
	assert.NoError(t, err)
	assert.False(t, local.IsZero())
}
