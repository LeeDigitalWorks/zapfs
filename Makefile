SOURCE_DIR = .
MOCKERY_VERSION = v3.2.5

all: install

.PHONY: clean install test test-race lint mocks mocks-install integration build build-enterprise proto

# =============================================================================
# Build Targets
# =============================================================================

# Build community edition (default)
build:
	go build -ldflags="-s -w" -o zapfs .

# Build enterprise edition with all enterprise features
build-enterprise:
	go build -tags enterprise -ldflags="-s -w" -o zapfs-enterprise .

install:
	go install -ldflags="-s -w"

install-enterprise:
	go install -tags enterprise -ldflags="-s -w"

clean:
	go clean $(SOURCE_DIR)
	rm -f zapfs zapfs-enterprise
	rm -rf mocks/

# =============================================================================
# Testing
# =============================================================================

test:
	go test ./...

test-race:
	go test -race ./...

test-cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Test enterprise features only
test-enterprise:
	go test -tags enterprise ./enterprise/...

# Test everything including enterprise
test-all:
	go test -tags enterprise ./...

# =============================================================================
# Integration Tests
# =============================================================================

# Default DB_DSN for Docker Compose MySQL (use localhost from host machine)
DB_DSN ?= zapfs:zapfs@tcp(localhost:3306)/zapfs

integration: integration-file integration-s3 integration-metadata integration-manager

integration-file:
	go test -race -cover -v -tags=integration -count=1 ./integration/file/...

integration-s3:
	go test -race -cover -v -tags=integration -count=1 ./integration/s3/...

integration-metadata:
	DB_DSN="$(DB_DSN)" go test -race -cover -v -tags=integration -count=1 ./integration/metadata/...

integration-manager:
	go test -race -cover -v -tags=integration -count=1 ./integration/manager/...

# =============================================================================
# Mock Generation (mockery v3)
# =============================================================================

# Install mockery if not present
mocks-install:
	@which mockery > /dev/null || go install github.com/vektra/mockery/v3@latest
	@echo "mockery installed: $$(mockery version)"

# Generate all mocks from .mockery.yaml config
mocks: mocks-install
	@echo "Generating mocks..."
	mockery
	@echo "Mocks generated in mocks/"

# Clean and regenerate mocks
mocks-clean:
	rm -rf mocks/
	$(MAKE) mocks

# =============================================================================
# Code Quality
# =============================================================================

lint:
	@which golangci-lint > /dev/null || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run ./...

fmt:
	go fmt ./...
	goimports -w .

vet:
	go vet ./...

# =============================================================================
# Protobuf Generation
# =============================================================================

proto:
	@echo "Generating protobuf files..."
	cd proto && make protoc

# =============================================================================
# Docker (Production)
# =============================================================================

docker-up:
	cd docker && docker compose up -d

docker-down:
	cd docker && docker compose down

docker-logs:
	cd docker && docker compose logs -f

docker-rebuild:
	cd docker && docker compose build && docker compose up -d

# =============================================================================
# Docker (Development with Hot Reload)
# =============================================================================

# Start development environment with hot reload
# First build takes longer; subsequent builds are fast due to caching
docker-dev:
	cd docker && docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build

# Start in detached mode
docker-dev-d:
	cd docker && docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

# View logs in dev mode
docker-dev-logs:
	cd docker && docker compose -f docker-compose.yml -f docker-compose.dev.yml logs -f

# Stop dev environment
docker-dev-down:
	cd docker && docker compose -f docker-compose.yml -f docker-compose.dev.yml down

# Rebuild dev images (clears go caches)
docker-dev-rebuild:
	cd docker && docker compose -f docker-compose.yml -f docker-compose.dev.yml build --no-cache
