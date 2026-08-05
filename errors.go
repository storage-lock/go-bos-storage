package bos_storage

import "fmt"

var (
	ErrBucketNameEmpty      = fmt.Errorf("BucketName can not empty")
	ErrKeyPrefixEmpty       = fmt.Errorf("KeyPrefix can not empty")
	ErrConnectionManagerNil = fmt.Errorf("ConnectionManager can not nil")
)