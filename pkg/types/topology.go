package types

import (
	"net"

	"github.com/google/uuid"
)

type Datacenter struct {
	DCID    uuid.UUID
	Name    string
	Region  string
	RackIDs []uuid.UUID
}

type Rack struct {
	RackID  uuid.UUID
	Name    string
	NodeIDs []uuid.UUID
}

type Node struct {
	NodeID  uuid.UUID
	Name    string
	RackID  uuid.UUID
	DCID    uuid.UUID
	IP      net.IP
	DiskIDs []uuid.UUID
}
