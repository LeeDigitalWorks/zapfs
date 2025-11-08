//go:build integration

package testutil

// ServiceAddresses holds addresses for all services used in tests
type ServiceAddresses struct {
	FileServer1      string
	FileServer1Admin string // Debug/admin HTTP port
	FileServer2      string
	FileServer2Admin string // Debug/admin HTTP port
	FileServer2Repl  string // Docker network address for server-to-server replication
	MetadataServer   string
	ManagerServer1   string
	ManagerServer2   string
	ManagerServer3   string
}

// DefaultAddresses returns default service addresses for local Docker testing
func DefaultAddresses() ServiceAddresses {
	return ServiceAddresses{
		// File servers (gRPC and admin ports)
		FileServer1:      GetEnv("FILE_SERVER_1_ADDR", "localhost:8081"),
		FileServer1Admin: GetEnv("FILE_SERVER_1_ADMIN_ADDR", "localhost:8010"),
		FileServer2:      GetEnv("FILE_SERVER_2_ADDR", "localhost:8091"),
		FileServer2Admin: GetEnv("FILE_SERVER_2_ADMIN_ADDR", "localhost:8011"),
		FileServer2Repl:  GetEnv("FILE_SERVER_2_REPL_ADDR", "file-2:8091"),

		// Metadata server
		MetadataServer: GetEnv("METADATA_SERVER_ADDR", "localhost:8083"),

		// Manager cluster
		ManagerServer1: GetEnv("MANAGER_SERVER_1_ADDR", "localhost:8050"),
		ManagerServer2: GetEnv("MANAGER_SERVER_2_ADDR", "localhost:8052"),
		ManagerServer3: GetEnv("MANAGER_SERVER_3_ADDR", "localhost:8054"),
	}
}

// Addrs is a global instance of ServiceAddresses for convenience
var Addrs = DefaultAddresses()
