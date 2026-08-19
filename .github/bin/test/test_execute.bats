#!/usr/bin/env bats
#
# Post-install smoke tests, run against an INSTALLED aql.
# Version assertions and EXPECTED_VERSION: see version_lib.sh.

setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
  VERSION_FILE="$REPO_ROOT/VERSION"
  load "$BATS_TEST_DIRNAME/version_lib.sh"
}

@test "can run aql" {
  run aql --help
  [ "$status" -eq 0 ]
}

@test "aql reports version" {
  run aql --version
  [ "$status" -eq 0 ]
}

@test "aql reports the version from the VERSION file" {
  local expected
  expected="$(expected_version "$VERSION_FILE")"

  run aql --version
  [ "$status" -eq 0 ]
  echo "expected (from $expected):"
  expected_version_lines "$expected"
  echo "reported:"
  echo "$output"
  assert_version_output "$output" "$expected"
}
