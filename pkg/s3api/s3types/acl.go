package s3types

import (
	"encoding/xml"
	"fmt"
)

// Canned ACL types
type CannedACL string

const (
	ACLPrivate           CannedACL = "private"
	ACLPublicRead        CannedACL = "public-read"
	ACLPublicReadWrite   CannedACL = "public-read-write"
	ACLAuthenticatedRead CannedACL = "authenticated-read"
	ACLBucketOwnerRead   CannedACL = "bucket-owner-read"
	ACLBucketOwnerFull   CannedACL = "bucket-owner-full-control"
	ACLLogDeliveryWrite  CannedACL = "log-delivery-write"
	ACLAwsExecRead       CannedACL = "aws-exec-read"
)

func (ca CannedACL) String() string {
	return string(ca)
}

func ParseValidCannedACL(input string) (CannedACL, error) {
	switch input {
	case ACLPrivate.String(),
		ACLPublicRead.String(),
		ACLPublicReadWrite.String(),
		ACLAuthenticatedRead.String(),
		ACLBucketOwnerRead.String(),
		ACLBucketOwnerFull.String(),
		ACLLogDeliveryWrite.String(),
		ACLAwsExecRead.String():
		return CannedACL(input), nil
	default:
		return "", fmt.Errorf("invalid canned ACL: %s", input)
	}
}

// Predefined ACL group URIs
const (
	AllUsersGroup           = "http://acs.amazonaws.com/groups/global/AllUsers"
	AuthenticatedUsersGroup = "http://acs.amazonaws.com/groups/global/AuthenticatedUsers"
	LogDeliveryGroup        = "http://acs.amazonaws.com/groups/s3/LogDelivery"
)

// ACL permission types
type Permission string

const (
	PermissionFullControl Permission = "FULL_CONTROL"
	PermissionRead        Permission = "READ"
	PermissionWrite       Permission = "WRITE"
	PermissionReadACP     Permission = "READ_ACP"
	PermissionWriteACP    Permission = "WRITE_ACP"
)

// GranteeType identifies how a grantee is specified
type GranteeType string

const (
	GranteeTypeCanonicalUser GranteeType = "CanonicalUser"
	GranteeTypeGroup         GranteeType = "Group"
	GranteeTypeEmail         GranteeType = "AmazonCustomerByEmail"
)

// AccessControlPolicy is the XML format for ACL API responses
// This is the top-level wrapper for GetBucketAcl/GetObjectAcl responses
type AccessControlPolicy struct {
	XMLName           xml.Name        `xml:"AccessControlPolicy"`
	Xmlns             string          `xml:"xmlns,attr,omitempty"`
	Owner             Owner           `xml:"Owner"`
	AccessControlList AccessControlListXML `xml:"AccessControlList"`
}

// AccessControlListXML is the XML wrapper for grants in AccessControlPolicy
type AccessControlListXML struct {
	Grants []GrantXML `xml:"Grant"`
}

// GrantXML represents a grant in XML format with proper namespace attributes
type GrantXML struct {
	Grantee    GranteeXML `xml:"Grantee"`
	Permission Permission `xml:"Permission"`
}

// GranteeXML represents a grantee in XML format with xsi:type attribute
type GranteeXML struct {
	XMLName     xml.Name `xml:"Grantee"`
	Xmlns       string   `xml:"xmlns:xsi,attr,omitempty"`
	XsiType     string   `xml:"xsi:type,attr,omitempty"`
	ID          string   `xml:"ID,omitempty"`
	DisplayName string   `xml:"DisplayName,omitempty"`
	URI         string   `xml:"URI,omitempty"`
}

// ToAccessControlPolicy converts an AccessControlList to the XML response format
func (acl *AccessControlList) ToAccessControlPolicy() *AccessControlPolicy {
	if acl == nil {
		return nil
	}

	policy := &AccessControlPolicy{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: acl.Owner,
	}

	for _, grant := range acl.Grants {
		grantXML := GrantXML{
			Permission: grant.Permission,
			Grantee: GranteeXML{
				Xmlns: "http://www.w3.org/2001/XMLSchema-instance",
			},
		}

		switch grant.Grantee.Type {
		case GranteeTypeCanonicalUser:
			grantXML.Grantee.XsiType = "CanonicalUser"
			grantXML.Grantee.ID = grant.Grantee.ID
			grantXML.Grantee.DisplayName = grant.Grantee.DisplayName
		case GranteeTypeGroup:
			grantXML.Grantee.XsiType = "Group"
			grantXML.Grantee.URI = grant.Grantee.URI
		case GranteeTypeEmail:
			grantXML.Grantee.XsiType = "AmazonCustomerByEmail"
			// Email grantees would use EmailAddress field
		}

		policy.AccessControlList.Grants = append(policy.AccessControlList.Grants, grantXML)
	}

	return policy
}

// AccessControlList represents an S3 Access Control List (internal representation)
type AccessControlList struct {
	Owner  Owner   `xml:"Owner" json:"owner"`
	Grants []Grant `xml:"AccessControlList>Grant" json:"grants"`
}

// Owner identifies the owner of an object or bucket
type Owner struct {
	ID          string `xml:"ID" json:"id"`
	DisplayName string `xml:"DisplayName,omitempty" json:"display_name,omitempty"`
}

// Grant represents a single permission grant to a grantee
type Grant struct {
	Grantee    Grantee    `xml:"Grantee" json:"grantee"`
	Permission Permission `xml:"Permission" json:"permission"`
}

// Grantee identifies who receives the permission
type Grantee struct {
	Type         GranteeType `xml:"type,attr" json:"type"`
	ID           string      `xml:"ID,omitempty" json:"id,omitempty"`
	DisplayName  string      `xml:"DisplayName,omitempty" json:"display_name,omitempty"`
	EmailAddress string      `xml:"EmailAddress,omitempty" json:"email_address,omitempty"`
	URI          string      `xml:"URI,omitempty" json:"uri,omitempty"`
}

// CheckAccess checks if the given account ID has the required permission
func (acl *AccessControlList) CheckAccess(accountID string, isAuthenticated bool, required Permission) bool {
	if acl == nil {
		return false
	}

	for _, grant := range acl.Grants {
		if !permissionSatisfies(grant.Permission, required) {
			continue
		}

		switch grant.Grantee.Type {
		case GranteeTypeCanonicalUser:
			if grant.Grantee.ID == accountID {
				return true
			}
		case GranteeTypeGroup:
			switch grant.Grantee.URI {
			case AllUsersGroup:
				return true // Everyone
			case AuthenticatedUsersGroup:
				if isAuthenticated {
					return true
				}
			}
		}
	}

	// Owner always has FULL_CONTROL implicitly
	if acl.Owner.ID == accountID {
		return true
	}

	return false
}

// permissionSatisfies checks if the granted permission satisfies the required permission
func permissionSatisfies(granted, required Permission) bool {
	if granted == PermissionFullControl {
		return true
	}
	return granted == required
}

// NewPrivateACL creates a private ACL owned by the given account
func NewPrivateACL(ownerID, ownerDisplayName string) *AccessControlList {
	return &AccessControlList{
		Owner: Owner{
			ID:          ownerID,
			DisplayName: ownerDisplayName,
		},
		Grants: []Grant{
			{
				Grantee: Grantee{
					Type:        GranteeTypeCanonicalUser,
					ID:          ownerID,
					DisplayName: ownerDisplayName,
				},
				Permission: PermissionFullControl,
			},
		},
	}
}

// FromCannedACL creates an AccessControlList from a canned ACL
func FromCannedACL(canned CannedACL, ownerID, ownerDisplayName string) *AccessControlList {
	acl := NewPrivateACL(ownerID, ownerDisplayName)

	switch canned {
	case ACLPublicRead:
		acl.Grants = append(acl.Grants, Grant{
			Grantee:    Grantee{Type: GranteeTypeGroup, URI: AllUsersGroup},
			Permission: PermissionRead,
		})
	case ACLPublicReadWrite:
		acl.Grants = append(acl.Grants,
			Grant{Grantee: Grantee{Type: GranteeTypeGroup, URI: AllUsersGroup}, Permission: PermissionRead},
			Grant{Grantee: Grantee{Type: GranteeTypeGroup, URI: AllUsersGroup}, Permission: PermissionWrite},
		)
	case ACLAuthenticatedRead:
		acl.Grants = append(acl.Grants, Grant{
			Grantee:    Grantee{Type: GranteeTypeGroup, URI: AuthenticatedUsersGroup},
			Permission: PermissionRead,
		})
	case ACLLogDeliveryWrite:
		acl.Grants = append(acl.Grants,
			Grant{Grantee: Grantee{Type: GranteeTypeGroup, URI: LogDeliveryGroup}, Permission: PermissionWrite},
			Grant{Grantee: Grantee{Type: GranteeTypeGroup, URI: LogDeliveryGroup}, Permission: PermissionReadACP},
		)
		// ACLBucketOwnerRead, ACLBucketOwnerFull are handled by the caller with bucket owner info
	}

	return acl
}
