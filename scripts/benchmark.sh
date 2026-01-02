#!/bin/bash
# benchmark.sh - S3 Performance Benchmark for ZapFS
#
# This script uses MinIO WARP to stress test ZapFS with various workloads.
# It measures throughput (MiB/s), operations/sec, and latency percentiles.
#
# Prerequisites:
#   - Go 1.21+ (for installing warp)
#   - Running ZapFS cluster
#
# Usage:
#   ./scripts/benchmark.sh                    # Run with defaults
#   ./scripts/benchmark.sh --profile large    # Large object profile
#   ./scripts/benchmark.sh --profile mixed    # Mixed workload profile
#   ./scripts/benchmark.sh --help             # Show help
#
# Environment variables:
#   S3_HOST               - S3 endpoint (default: localhost:8082)
#   AWS_ACCESS_KEY_ID     - Access key (default: AKIAADMINKEY00001)
#   AWS_SECRET_ACCESS_KEY - Secret key (default: ****)
#   DURATION         - Test duration (default: 60s)
#   CONCURRENT       - Concurrent operations (default: 32)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
HOST="${S3_HOST:-localhost:8082}"
ACCESS_KEY="${AWS_ACCESS_KEY_ID:-AKIAADMINKEY00001}"
SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE1}"
DURATION="${DURATION:-60s}"
CONCURRENT="${CONCURRENT:-32}"
BUCKET="${BUCKET:-zapfs-benchmark}"
PROFILE="quick"
OUTPUT_DIR=""
TLS=""

# Test profiles
declare -A PROFILES
PROFILES[quick]="Quick sanity check (1 min, 32 concurrent, 1MiB objects)"
PROFILES[standard]="Standard benchmark (5 min, 64 concurrent, mixed sizes)"
PROFILES[large]="Large object stress test (5 min, 32 concurrent, 100MiB objects)"
PROFILES[small]="Small object throughput (5 min, 128 concurrent, 4KiB objects)"
PROFILES[mixed]="Mixed realistic workload (10 min, 64 concurrent, random sizes)"
PROFILES[stress]="High stress test (30 min, 128 concurrent, continuous)"
PROFILES[multipart]="Multipart upload test (5 min, 16 concurrent, 500MiB objects)"

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "S3 Performance Benchmark for ZapFS using MinIO WARP"
    echo ""
    echo "Options:"
    echo "  -h, --host HOST        S3 endpoint (default: $HOST)"
    echo "  -a, --access-key KEY   Access key (default: AKIA...)"
    echo "  -s, --secret-key KEY   Secret key (default: ****)"
    echo "  -d, --duration DUR     Test duration (default: $DURATION)"
    echo "  -c, --concurrent N     Concurrent operations (default: $CONCURRENT)"
    echo "  -p, --profile NAME     Test profile (default: $PROFILE)"
    echo "  -o, --output DIR       Save results to directory"
    echo "  -t, --tls              Use TLS/HTTPS"
    echo "  --help                 Show this help message"
    echo ""
    echo "Available profiles:"
    for profile in "${!PROFILES[@]}"; do
        printf "  %-12s %s\n" "$profile" "${PROFILES[$profile]}"
    done
    echo ""
    echo "Examples:"
    echo "  $0                                    # Quick benchmark"
    echo "  $0 --profile standard                 # Standard 5-minute benchmark"
    echo "  $0 --profile large --concurrent 64    # Large objects, high concurrency"
    echo "  $0 --host s3.example.com:443 --tls    # Remote endpoint with TLS"
    echo "  $0 --output ./results                 # Save results to directory"
    echo ""
    echo "Environment variables:"
    echo "  S3_HOST, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, DURATION, CONCURRENT"
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

header() {
    echo ""
    echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  $1${NC}"
    echo -e "${GREEN}════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -a|--access-key)
            ACCESS_KEY="$2"
            shift 2
            ;;
        -s|--secret-key)
            SECRET_KEY="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -c|--concurrent)
            CONCURRENT="$2"
            shift 2
            ;;
        -p|--profile)
            PROFILE="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -t|--tls)
            TLS="--tls"
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate profile
if [[ ! -v "PROFILES[$PROFILE]" ]]; then
    log_error "Unknown profile: $PROFILE"
    echo "Available profiles: ${!PROFILES[*]}"
    exit 1
fi

# Apply profile settings
case $PROFILE in
    quick)
        DURATION="60s"
        CONCURRENT="32"
        OBJ_SIZE="1MiB"
        OBJECTS="1000"
        ;;
    standard)
        DURATION="5m"
        CONCURRENT="64"
        OBJ_SIZE="1MiB"
        OBJECTS="5000"
        ;;
    large)
        DURATION="5m"
        CONCURRENT="32"
        OBJ_SIZE="100MiB"
        OBJECTS="500"
        ;;
    small)
        DURATION="5m"
        CONCURRENT="128"
        OBJ_SIZE="4KiB"
        OBJECTS="50000"
        ;;
    mixed)
        DURATION="10m"
        CONCURRENT="64"
        OBJ_SIZE=""  # Will use --obj.randsize
        OBJECTS="10000"
        ;;
    stress)
        DURATION="30m"
        CONCURRENT="128"
        OBJ_SIZE="1MiB"
        OBJECTS="100000"
        ;;
    multipart)
        DURATION="5m"
        CONCURRENT="16"
        OBJ_SIZE="500MiB"
        OBJECTS="100"
        ;;
esac

# Check if warp is installed
check_warp() {
    if command -v warp &> /dev/null; then
        log_success "warp found: $(which warp)"
        return 0
    fi

    log_warn "warp not found, attempting to install..."

    if ! command -v go &> /dev/null; then
        log_error "Go is required to install warp. Please install Go 1.21+ first."
        exit 1
    fi

    log_info "Installing warp via 'go install'..."
    go install github.com/minio/warp@latest

    # Check if GOPATH/bin is in PATH
    GOBIN="${GOBIN:-$(go env GOPATH)/bin}"
    if [[ ":$PATH:" != *":$GOBIN:"* ]]; then
        export PATH="$PATH:$GOBIN"
        log_warn "Added $GOBIN to PATH for this session"
    fi

    if command -v warp &> /dev/null; then
        log_success "warp installed successfully"
    else
        log_error "Failed to install warp"
        exit 1
    fi
}

# Check S3 endpoint connectivity
check_endpoint() {
    log_info "Checking S3 endpoint: $HOST"

    PROTOCOL="http"
    [[ -n "$TLS" ]] && PROTOCOL="https"

    if curl -s --connect-timeout 5 "$PROTOCOL://$HOST/minio/health/live" > /dev/null 2>&1; then
        log_success "Endpoint is reachable"
        return 0
    fi

    # Try basic TCP connection
    HOST_ONLY="${HOST%%:*}"
    PORT="${HOST##*:}"
    [[ "$PORT" == "$HOST_ONLY" ]] && PORT="9000"

    if nc -z -w5 "$HOST_ONLY" "$PORT" 2>/dev/null; then
        log_success "Endpoint is reachable (TCP)"
        return 0
    fi

    log_warn "Could not verify endpoint connectivity. Proceeding anyway..."
}

# Create output directory if specified
setup_output() {
    if [[ -n "$OUTPUT_DIR" ]]; then
        mkdir -p "$OUTPUT_DIR"
        log_info "Results will be saved to: $OUTPUT_DIR"
    fi
}

# Build warp command with common options
build_warp_cmd() {
    local cmd="warp"
    cmd+=" --host=$HOST"
    cmd+=" --access-key=$ACCESS_KEY"
    cmd+=" --secret-key=$SECRET_KEY"
    cmd+=" --concurrent=$CONCURRENT"
    cmd+=" --autoterm"
    [[ -n "$TLS" ]] && cmd+=" --tls"

    if [[ -n "$OBJ_SIZE" ]]; then
        cmd+=" --obj.size=$OBJ_SIZE"
    else
        cmd+=" --obj.randsize"
    fi

    cmd+=" --duration=$DURATION"

    if [[ -n "$OUTPUT_DIR" ]]; then
        cmd+=" --benchdata=$OUTPUT_DIR"
    fi

    echo "$cmd"
}

# Run PUT benchmark
run_put_benchmark() {
    header "PUT Benchmark (Upload Performance)"

    log_info "Testing upload throughput..."
    log_info "  Concurrent: $CONCURRENT"
    log_info "  Object size: ${OBJ_SIZE:-random}"
    log_info "  Duration: $DURATION"
    echo ""

    local cmd
    cmd=$(build_warp_cmd)
    cmd+=" put"

    eval "$cmd"
}

# Run GET benchmark
run_get_benchmark() {
    header "GET Benchmark (Download Performance)"

    log_info "Testing download throughput..."
    log_info "  Concurrent: $CONCURRENT"
    log_info "  Object size: ${OBJ_SIZE:-random}"
    log_info "  Duration: $DURATION"
    echo ""

    local cmd
    cmd=$(build_warp_cmd)
    cmd+=" get"

    eval "$cmd"
}

# Run mixed benchmark
run_mixed_benchmark() {
    header "Mixed Workload Benchmark"

    log_info "Testing mixed PUT/GET workload..."
    log_info "  Concurrent: $CONCURRENT"
    log_info "  Object size: ${OBJ_SIZE:-random}"
    log_info "  Duration: $DURATION"
    echo ""

    local cmd
    cmd=$(build_warp_cmd)
    cmd+=" mixed"

    eval "$cmd"
}

# Run DELETE benchmark
run_delete_benchmark() {
    header "DELETE Benchmark"

    log_info "Testing delete throughput..."
    echo ""

    local cmd
    cmd=$(build_warp_cmd)
    cmd+=" delete"

    eval "$cmd"
}

# Run LIST benchmark
run_list_benchmark() {
    header "LIST Benchmark"

    log_info "Testing list operations..."
    echo ""

    local cmd
    cmd=$(build_warp_cmd)
    cmd+=" list"

    eval "$cmd"
}

# Run multipart benchmark
run_multipart_benchmark() {
    header "Multipart Upload Benchmark"

    log_info "Testing multipart uploads..."
    log_info "  Concurrent: $CONCURRENT"
    log_info "  Object size: $OBJ_SIZE"
    echo ""

    local cmd
    cmd=$(build_warp_cmd)
    cmd+=" multipart"
    cmd+=" --parts=10"

    eval "$cmd"
}

# Print configuration summary
print_config() {
    header "ZapFS Performance Benchmark"

    echo "Configuration:"
    echo "  Profile:     $PROFILE - ${PROFILES[$PROFILE]}"
    echo "  Endpoint:    $HOST"
    echo "  Access Key:  ${ACCESS_KEY:0:4}****"
    echo "  TLS:         ${TLS:-disabled}"
    echo ""
    echo "Test Parameters:"
    echo "  Duration:    $DURATION"
    echo "  Concurrent:  $CONCURRENT"
    echo "  Object Size: ${OBJ_SIZE:-random}"
    echo "  Objects:     $OBJECTS"
    [[ -n "$OUTPUT_DIR" ]] && echo "  Output:      $OUTPUT_DIR"
    echo ""
    echo "Prometheus Metrics to Monitor:"
    echo "  - zapfs_s3_request_duration_seconds (latency)"
    echo "  - zapfs_s3_requests_total (RPS)"
    echo "  - zapfs_storage_bytes_written_total (write bandwidth)"
    echo "  - zapfs_storage_bytes_read_total (read bandwidth)"
    echo ""
}

# Run all benchmarks based on profile
run_benchmarks() {
    case $PROFILE in
        quick)
            run_put_benchmark
            run_get_benchmark
            ;;
        standard|large|small)
            run_put_benchmark
            run_get_benchmark
            run_mixed_benchmark
            ;;
        mixed)
            run_mixed_benchmark
            ;;
        stress)
            run_mixed_benchmark
            ;;
        multipart)
            run_multipart_benchmark
            ;;
    esac
}

# Print summary
print_summary() {
    header "Benchmark Complete"

    echo "Results Summary:"
    echo "  Profile: $PROFILE"
    echo "  Duration: $DURATION"
    echo "  Concurrent operations: $CONCURRENT"
    echo ""

    if [[ -n "$OUTPUT_DIR" ]]; then
        echo "Raw data saved to: $OUTPUT_DIR"
        echo ""
        echo "To analyze results:"
        echo "  warp analyze $OUTPUT_DIR/*.csv.zst"
        echo ""
        echo "To compare two runs:"
        echo "  warp cmp $OUTPUT_DIR/run1.csv.zst $OUTPUT_DIR/run2.csv.zst"
    fi

    echo ""
    echo "Tips for improving performance:"
    echo "  1. Increase file server replicas for higher throughput"
    echo "  2. Tune chunk size based on object size distribution"
    echo "  3. Enable connection pooling in clients"
    echo "  4. Monitor Prometheus metrics for bottlenecks"
}

# Main execution
main() {
    check_warp
    check_endpoint
    setup_output
    print_config

    read -p "Press Enter to start benchmark or Ctrl+C to cancel..."
    echo ""

    START_TIME=$(date +%s)

    run_benchmarks

    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))

    print_summary
    echo "Total time: ${ELAPSED}s"
}

main
