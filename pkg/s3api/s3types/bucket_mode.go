package s3types

type BucketMode uint64

const (
	BucketModeNone BucketMode = iota
	BucketModeMigrating
	BucketModeAdminLocked
	BucketModeAsyncDelete
	BucketModeReadOnly
)
