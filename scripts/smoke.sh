#!/usr/bin/env bash
# Minimal S3 smoke test using only curl: bucket and object CRUD, a range GET,
# and a listing, against a running gateway.
# Usage: scripts/smoke.sh [endpoint]   (default http://localhost:9000)
set -euo pipefail

EP="${1:-http://localhost:9000}"
BUCKET="smoke-$(date +%s)-$RANDOM"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
status() { curl -s -o "$TMP/body" -w '%{http_code}' "$@"; }

echo "endpoint $EP, bucket $BUCKET"

[ "$(status -X PUT "$EP/$BUCKET")" = 200 ] || fail "create bucket: $(cat "$TMP/body")"
[ "$(status -I "$EP/$BUCKET")" = 200 ] || fail "head bucket"

# 9 MiB spans three 4 MiB chunks.
head -c $((9 * 1024 * 1024)) /dev/urandom > "$TMP/obj"
[ "$(status -X PUT -H 'Content-Type: application/x-test' --data-binary "@$TMP/obj" "$EP/$BUCKET/dir/obj.bin")" = 200 ] \
  || fail "put object: $(cat "$TMP/body")"

curl -fsS -o "$TMP/got" "$EP/$BUCKET/dir/obj.bin" || fail "get object"
cmp -s "$TMP/obj" "$TMP/got" || fail "downloaded object differs from upload"

# Range across the first chunk boundary.
curl -fsS -o "$TMP/range" -H 'Range: bytes=4194300-4194309' "$EP/$BUCKET/dir/obj.bin" || fail "range get"
cmp -s "$TMP/range" <(tail -c +4194301 "$TMP/obj" | head -c 10) || fail "range content wrong"

[ "$(status "$EP/$BUCKET?list-type=2&delimiter=/")" = 200 ] || fail "list"
grep -q '<Prefix>dir/</Prefix>' "$TMP/body" || fail "listing missing common prefix: $(cat "$TMP/body")"

[ "$(status "$EP/$BUCKET/missing")" = 404 ] || fail "missing key status"
grep -q '<Code>NoSuchKey</Code>' "$TMP/body" || fail "missing key error code"

[ "$(status -X DELETE "$EP/$BUCKET")" = 409 ] || fail "delete non-empty bucket should be 409"
[ "$(status -X DELETE "$EP/$BUCKET/dir/obj.bin")" = 204 ] || fail "delete object"
[ "$(status -X DELETE "$EP/$BUCKET")" = 204 ] || fail "delete bucket"

echo "smoke test passed"
