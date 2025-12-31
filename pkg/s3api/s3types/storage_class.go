package s3types

import "github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"

type StorageClass uint8

const (
	StorageClassUnknown StorageClass = iota
	StorageClassStandard
	StorageClassInfrequentAccess
	StorageClassGlacier
	StorageClassDeepArchive
	StorageClassIntelligentTiering
)

var (
	storageClassTypes = map[StorageClass]string{
		StorageClassUnknown:            "UNKNOWN",
		StorageClassStandard:           "STANDARD",
		StorageClassInfrequentAccess:   "STANDARD_IA",
		StorageClassGlacier:            "GLACIER",
		StorageClassDeepArchive:        "DEEP_ARCHIVE",
		StorageClassIntelligentTiering: "INTELLIGENT_TIERING",
	}
	storageClassNames = map[string]StorageClass{
		"UNKNOWN":             StorageClassUnknown,
		"STANDARD":            StorageClassStandard,
		"STANDARD_IA":         StorageClassInfrequentAccess,
		"GLACIER":             StorageClassGlacier,
		"DEEP_ARCHIVE":        StorageClassDeepArchive,
		"INTELLIGENT_TIERING": StorageClassIntelligentTiering,
	}
)

func (sc StorageClass) String() string {
	if name, ok := storageClassTypes[sc]; ok {
		return name
	}
	return "UNKNOWN"
}

func ParseStorageClass(name string) (StorageClass, error) {
	if sc, ok := storageClassNames[name]; ok {
		return sc, nil
	}
	return StorageClassUnknown, s3err.ErrInvalidStorageClass
}
