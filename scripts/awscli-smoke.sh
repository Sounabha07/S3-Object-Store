#!/usr/bin/env bash
# Smoke test with the unmodified aws CLI (--endpoint-url), against a running
# gateway. Objects stay under the CLI's 8 MiB multipart threshold because
# multipart upload arrives in M5.
# Usage: scripts/awscli-smoke.sh [endpoint]   (default http://localhost:9000)
set -euo pipefail

EP="${1:-http://localhost:9000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test-access-key}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test-secret-key}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export AWS_EC2_METADATA_DISABLED=true
BUCKET="cli-$(date +%s)-$RANDOM"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
s3() { aws --endpoint-url "$EP" "$@"; }

aws --version
s3 s3 mb "s3://$BUCKET"
s3 s3 ls | grep -q "$BUCKET"

head -c $((5 * 1024 * 1024 + 7)) /dev/urandom > "$TMP/a.bin"
echo "hello from the aws cli" > "$TMP/b.txt"
s3 s3 cp "$TMP/a.bin" "s3://$BUCKET/data/a.bin"
s3 s3 cp "$TMP/b.txt" "s3://$BUCKET/data/b.txt"

s3 s3 ls "s3://$BUCKET/data/" | tee "$TMP/ls"
grep -q 'a.bin' "$TMP/ls" && grep -q 'b.txt' "$TMP/ls"

s3 s3 cp "s3://$BUCKET/data/a.bin" "$TMP/a.out"
cmp "$TMP/a.bin" "$TMP/a.out"

s3 s3api head-object --bucket "$BUCKET" --key data/b.txt > /dev/null
s3 s3api get-object --bucket "$BUCKET" --key data/a.bin --range bytes=0-9 "$TMP/range" > /dev/null
cmp "$TMP/range" <(head -c 10 "$TMP/a.bin")

s3 s3 rm "s3://$BUCKET/data/a.bin"
s3 s3 rm "s3://$BUCKET/data/b.txt"
s3 s3 rb "s3://$BUCKET"
echo "aws cli smoke test passed"
