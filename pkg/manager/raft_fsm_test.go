package manager

import (
	"testing"

	"zapfs/proto/common_pb"
	"zapfs/proto/manager_pb"
)

func TestRegistrationMateriallyChanged(t *testing.T) {
	tests := []struct {
		name     string
		existing *ServiceRegistration
		new      *ServiceRegistration
		want     bool
	}{
		{
			name: "identical registrations - no change",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "local-1", Type: "local"},
				},
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "local-1", Type: "local"},
				},
			},
			want: false,
		},
		{
			name: "status changed from offline to active",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceOffline,
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
			},
			want: true,
		},
		{
			name: "status changed from active to offline",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceOffline,
			},
			want: true,
		},
		{
			name: "backend added",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "local-1", Type: "local"},
				},
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "local-1", Type: "local"},
					{Id: "local-2", Type: "local"},
				},
			},
			want: true,
		},
		{
			name: "backend removed",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "local-1", Type: "local"},
					{Id: "local-2", Type: "local"},
				},
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "local-1", Type: "local"},
				},
			},
			want: true,
		},
		{
			name: "backend type changed",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "backend-1", Type: "local"},
				},
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_FILE_SERVICE,
				Status:      ServiceActive,
				StorageBackends: []*manager_pb.StorageBackend{
					{Id: "backend-1", Type: "s3"},
				},
			},
			want: true,
		},
		{
			name: "nil backends both - no change",
			existing: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_METADATA_SERVICE,
				Status:      ServiceActive,
			},
			new: &ServiceRegistration{
				ServiceType: manager_pb.ServiceType_METADATA_SERVICE,
				Status:      ServiceActive,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := registrationMateriallyChanged(tt.existing, tt.new)
			if got != tt.want {
				t.Errorf("registrationMateriallyChanged() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStorageBackendsEqual(t *testing.T) {
	tests := []struct {
		name string
		a    []*manager_pb.StorageBackend
		b    []*manager_pb.StorageBackend
		want bool
	}{
		{
			name: "both nil",
			a:    nil,
			b:    nil,
			want: true,
		},
		{
			name: "both empty",
			a:    []*manager_pb.StorageBackend{},
			b:    []*manager_pb.StorageBackend{},
			want: true,
		},
		{
			name: "identical single backend",
			a: []*manager_pb.StorageBackend{
				{Id: "local-1", Type: "local"},
			},
			b: []*manager_pb.StorageBackend{
				{Id: "local-1", Type: "local"},
			},
			want: true,
		},
		{
			name: "different lengths",
			a: []*manager_pb.StorageBackend{
				{Id: "local-1", Type: "local"},
			},
			b: []*manager_pb.StorageBackend{
				{Id: "local-1", Type: "local"},
				{Id: "local-2", Type: "local"},
			},
			want: false,
		},
		{
			name: "same length different IDs",
			a: []*manager_pb.StorageBackend{
				{Id: "local-1", Type: "local"},
			},
			b: []*manager_pb.StorageBackend{
				{Id: "local-2", Type: "local"},
			},
			want: false,
		},
		{
			name: "same ID different type",
			a: []*manager_pb.StorageBackend{
				{Id: "backend-1", Type: "local"},
			},
			b: []*manager_pb.StorageBackend{
				{Id: "backend-1", Type: "s3"},
			},
			want: false,
		},
		{
			name: "order independent - same backends different order",
			a: []*manager_pb.StorageBackend{
				{Id: "local-1", Type: "local"},
				{Id: "s3-1", Type: "s3"},
			},
			b: []*manager_pb.StorageBackend{
				{Id: "s3-1", Type: "s3"},
				{Id: "local-1", Type: "local"},
			},
			want: true,
		},
		{
			name: "with nested backends - identical",
			a: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data/disk1"},
						{Id: "disk-2", Type: "local", Path: "/data/disk2"},
					},
				},
			},
			b: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data/disk1"},
						{Id: "disk-2", Type: "local", Path: "/data/disk2"},
					},
				},
			},
			want: true,
		},
		{
			name: "with nested backends - different path",
			a: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data/disk1"},
					},
				},
			},
			b: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data/disk-changed"},
					},
				},
			},
			want: false,
		},
		{
			name: "ignores UsedBytes difference",
			a: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data", UsedBytes: 1000},
					},
				},
			},
			b: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data", UsedBytes: 9999},
					},
				},
			},
			want: true,
		},
		{
			name: "ignores TotalBytes difference",
			a: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data", TotalBytes: 100000},
					},
				},
			},
			b: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data", TotalBytes: 200000},
					},
				},
			},
			want: true,
		},
		{
			name: "ignores both UsedBytes and TotalBytes difference",
			a: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data", UsedBytes: 1000, TotalBytes: 100000},
					},
				},
			},
			b: []*manager_pb.StorageBackend{
				{
					Id:   "local-1",
					Type: "local",
					Backends: []*common_pb.Backend{
						{Id: "disk-1", Type: "local", Path: "/data", UsedBytes: 50000, TotalBytes: 200000},
					},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := storageBackendsEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("storageBackendsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
