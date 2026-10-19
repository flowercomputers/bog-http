#!/usr/bin/env bash
# Outside judges for the generated OpenAPI document (tests/golden):
# the reference validator, a linter, and a client that must type-check.
# Needs uv and node. Run after `UPDATE_GOLDEN=1 cargo test --test openapi`.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
spec="$root/tests/golden/openapi.json"

uvx openapi-spec-validator "$spec"

# Style rules we knowingly break: no auth yet (security-defined), no
# license, undocumented tags for undocumented fields, meta routes with
# no 4xx.
npx --yes @redocly/cli@1 lint "$spec" --format=stylish \
  --skip-rule security-defined --skip-rule info-license \
  --skip-rule tag-description --skip-rule operation-4xx-response

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
npx --yes openapi-typescript@7 "$spec" -o "$work/api.d.ts" >/dev/null
cp "$root/scripts/client-check.ts" "$work/use.ts"
(cd "$work" && npx --yes -p typescript@5 tsc --noEmit --strict use.ts api.d.ts)
echo "openapi: valid, lint-clean, client type-checks"
