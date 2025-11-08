.PHONY: protoc

protoc:
	protoc common.proto --go_out=./common_pb --go-grpc_out=./common_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc metadata.proto --go_out=./metadata_pb --go-grpc_out=./metadata_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc file.proto --go_out=./file_pb --go-grpc_out=./file_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc manager.proto --go_out=./manager_pb --go-grpc_out=./manager_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc iam.proto --go_out=./iam_pb --go-grpc_out=./iam_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative