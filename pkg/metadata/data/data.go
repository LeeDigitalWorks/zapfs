package data

import (
	"context"
	"net/http"

	"zapfs/pkg/iam"
	"zapfs/pkg/s3api/s3action"
)

type Data struct {
	Ctx      context.Context
	Req      *http.Request
	S3Info   *S3Info
	Identity *iam.Identity // Authenticated user identity (nil for anonymous)
}

func NewData(ctx context.Context, req *http.Request) *Data {
	return &Data{
		Ctx:    ctx,
		Req:    req,
		S3Info: &S3Info{},
	}
}

type S3Info struct {
	Bucket    string
	Key       string
	Action    s3action.Action
	OwnerID   string
	AccessKey string
	SecretKey string
}
