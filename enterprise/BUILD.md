# Building ZapFS Enterprise

This directory contains enterprise-only features for ZapFS. These features are
only available with a valid enterprise license.

**License**: See [LICENSE.enterprise](../LICENSE.enterprise) in the repository root.

## Build Tags

Enterprise features are excluded from the standard build using Go build tags.
To build with enterprise features enabled:

```bash
# Build enterprise binary
go build -tags enterprise -o zapfs-enterprise .

# Or using make
make build-enterprise
```

## Community Build (Default)

The default build excludes enterprise features:

```bash
# Standard build
go build -o zapfs .

# Or using make
make build
```

## License Key

Enterprise features require a valid license key. The license key is provided
during purchase and can be configured via:

1. Environment variable: `ZAPFS_LICENSE_KEY`
2. Config file: `license_key` field
3. Command line: `--license-key` flag

## Features

| Feature | Package | Description |
|---------|---------|-------------|
| Audit Logging | `enterprise/audit` | Compliance audit logs |
| LDAP/AD Integration | `enterprise/ldap` | Directory service integration |
| KMS Integration | `enterprise/kms` | External key management |
| Task Queue | `enterprise/taskqueue` | Replication and async job processing |
| License Management | `enterprise/license` | License validation |

## Development

When developing enterprise features:

1. Add `//go:build enterprise` at the top of each file
2. Create a stub file with `//go:build !enterprise` for community compatibility
3. Add tests with the enterprise build tag
4. Update this documentation

## Testing

Run enterprise tests:

```bash
go test -tags enterprise ./enterprise/...
```

Run all tests including enterprise:

```bash
go test -tags enterprise ./...
```

