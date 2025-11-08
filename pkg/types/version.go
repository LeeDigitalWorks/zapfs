package types

// ObjectVersion represents a version of an object for listing
type ObjectVersion struct {
	Key            string
	VersionID      string
	IsLatest       bool
	IsDeleteMarker bool
	LastModified   int64 // Unix nano timestamp
	ETag           string
	Size           int64
	StorageClass   string
	OwnerID        string
}
