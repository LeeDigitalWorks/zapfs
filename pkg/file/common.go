package file

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// parseURLPath expects a /<objectid> otherwise errors
func parseURLPath(path string) (uuid.UUID, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 1 {
		return uuid.Nil, fmt.Errorf("invalid path: %s", path)
	}

	objectID, err := uuid.Parse(parts[0])
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid object ID: %s", parts[0])
	}

	return objectID, nil
}
