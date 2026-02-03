#!/bin/bash
# Switch between deployment versions

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <single-machine|distributed>"
    echo ""
    echo "Examples:"
    echo "  $0 single-machine   # Use Version 1"
    echo "  $0 distributed      # Use Version 2"
    exit 1
fi

VERSION=$1
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_FILE="${PROJECT_ROOT}/config/.env.${VERSION}"
TARGET_FILE="${PROJECT_ROOT}/.env"

if [ ! -f "$SOURCE_FILE" ]; then
    echo "Error: $SOURCE_FILE not found"
    exit 1
fi

cp "$SOURCE_FILE" "$TARGET_FILE"
echo "✓ Switched to $VERSION environment"
echo "  Config: $TARGET_FILE"
