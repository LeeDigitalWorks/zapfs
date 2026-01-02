#!/bin/bash
# Script to add copyright headers to Go files
#
# Usage:
#   ./scripts/add-copyright-headers.sh                    # Process pkg/ with Apache 2.0
#   ./scripts/add-copyright-headers.sh /path/to/dir       # Process specific directory
#   ./scripts/add-copyright-headers.sh enterprise/        # Auto-detects enterprise header
#
# The script automatically uses:
#   - ZapFS Enterprise License for files in enterprise/ OR with //go:build enterprise
#   - Apache 2.0 for all other files

APACHE_HEADER="// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0"

ENTERPRISE_HEADER="// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file."

get_header() {
    local file="$1"

    # Check if file is in enterprise/ directory
    if [[ "$file" == *"/enterprise/"* ]]; then
        echo "$ENTERPRISE_HEADER"
        return
    fi

    # Check if file has //go:build enterprise tag
    if head -5 "$file" | grep -q "//go:build enterprise"; then
        echo "$ENTERPRISE_HEADER"
        return
    fi

    # Default to Apache 2.0
    echo "$APACHE_HEADER"
}

add_header() {
    local file="$1"

    # Skip if already has Copyright
    if head -20 "$file" | grep -q "Copyright"; then
        echo "SKIP (has copyright): $file"
        return
    fi

    # Skip generated files
    if head -5 "$file" | grep -q "DO NOT EDIT\|Code generated\|auto-generated"; then
        echo "SKIP (generated): $file"
        return
    fi

    # Get the appropriate header for this file
    local header
    header=$(get_header "$file")

    # Check if file starts with build tags
    local first_line
    first_line=$(head -1 "$file")

    if [[ "$first_line" == "//go:build"* ]] || [[ "$first_line" == "// +build"* ]]; then
        # Find where build tags end (look for first blank line or package)
        local build_section=""
        local rest=""
        local in_build=true

        while IFS= read -r line; do
            if $in_build; then
                if [[ "$line" == "//go:build"* ]] || [[ "$line" == "// +build"* ]] || [[ "$line" == "//"* && "$line" != "// Package"* && "$line" != "// Copyright"* ]]; then
                    build_section+="$line"$'\n'
                elif [[ -z "$line" ]]; then
                    build_section+="$line"$'\n'
                    in_build=false
                else
                    in_build=false
                    rest+="$line"$'\n'
                fi
            else
                rest+="$line"$'\n'
            fi
        done < "$file"

        # Write: build tags + blank + copyright + blank + rest
        {
            printf "%s" "$build_section"
            echo "$header"
            echo ""
            printf "%s" "$rest"
        } > "$file"
    else
        # No build tags, just prepend header
        {
            echo "$header"
            echo ""
            cat "$file"
        } > "$file.tmp" && mv "$file.tmp" "$file"
    fi

    # Show which header type was used
    if [[ "$file" == *"/enterprise/"* ]] || head -5 "$file" | grep -q "//go:build enterprise"; then
        echo "UPDATED (enterprise): $file"
    else
        echo "UPDATED (apache-2.0): $file"
    fi
}

# Process directories passed as arguments, or default to pkg/
if [ $# -eq 0 ]; then
    DIRS="pkg"
else
    DIRS="$@"
fi

for dir in $DIRS; do
    echo "Processing $dir..."
    find "$dir" -name "*.go" -type f | while read -r file; do
        add_header "$file"
    done
done

echo ""
echo "Done!"
