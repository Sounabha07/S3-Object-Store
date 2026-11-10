#!/usr/bin/env bash
# Demo: the unmodified aws CLI uploads, lists and downloads while a storage
# node is killed in the middle of the upload. Needs the docker-compose
# cluster (make up) and the aws CLI.
# Usage: scripts/demo.sh [endpoint]   (default http://localhost:9000)
set -euo pipefail
cd "$(dirname "$0")/.."

EP="${1:-http://localhost:9000}"
DC="docker compose -f deploy/docker-compose.yml"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test-access-key}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test-secret-key}"
export AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
BUCKET="demo-$(date +%s)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"; $DC start storage-2 >/dev/null 2>&1 || true' EXIT
s3() { aws --endpoint-url "$EP" "$@"; }
say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

say "create bucket $BUCKET"
s3 s3 mb "s3://$BUCKET"

say "generate a 64 MiB file and a few small ones"
head -c $((64 * 1024 * 1024)) /dev/urandom > "$TMP/video.bin"
for i in 1 2 3; do echo "note $i" > "$TMP/note-$i.txt"; done
sha256sum "$TMP/video.bin" | tee "$TMP/expected.sha256"

say "upload (multipart) and kill storage-2 one second in"
( sleep 1; $DC kill storage-2 >/dev/null; echo "    [storage-2 killed]" ) &
s3 s3 cp "$TMP/video.bin" "s3://$BUCKET/media/video.bin"
wait
s3 s3 cp "$TMP" "s3://$BUCKET/notes/" --recursive --exclude "*" --include "note-*.txt"

say "list (storage-2 still down)"
s3 s3 ls "s3://$BUCKET/" --recursive --human-readable --summarize

say "download with storage-2 down and verify the checksum"
s3 s3 cp "s3://$BUCKET/media/video.bin" "$TMP/downloaded.bin"
( cd "$TMP" && sha256sum downloaded.bin | sed 's/downloaded.bin/video.bin/' | cmp - expected.sha256 ) && echo "    checksum OK"

say "bring storage-2 back and repair"
$DC start storage-2 >/dev/null
sleep 3
curl -fsS -X POST -H "Authorization: Bearer ${S3OS_ADMIN_TOKEN:-dev-admin-token}" "$EP/_admin/repair"; echo

say "clean up"
s3 s3 rm "s3://$BUCKET" --recursive >/dev/null
s3 s3 rb "s3://$BUCKET"
echo "demo finished"
