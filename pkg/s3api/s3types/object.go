package s3types

type Object struct {
	Name              string
	Size              int64
	ContentType       string
	ETag              string
	ChecksumAlgorithm ChecksumAlgorithm
	ChecksumValue     string
	StorageClass      StorageClass

	// TODO: Add specific backend metadata fields
}
