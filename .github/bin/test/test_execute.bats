#!/usr/bin/env bats
#
# Post-install smoke tests, run against an INSTALLED aql.
# Version assertions and EXPECTED_VERSION: see version_lib.sh.

setup() {
  REPO_ROOT="$(cd "$BATS_TEST_DIRNAME/../../.." && pwd)"
  VERSION_FILE="$REPO_ROOT/VERSION"
  load "$BATS_TEST_DIRNAME/version_lib.sh"
}

installed_with() {
  if command -v dpkg-query >/dev/null && dpkg-query -W aerospike-aql >/dev/null 2>&1; then
    echo deb
  elif command -v rpm >/dev/null && rpm -q aerospike-aql >/dev/null 2>&1; then
    echo rpm
  fi
}

loaded_openssl() {
  ldd "$(readlink -f "$(command -v aql)")" | awk '$1 ~ /^lib(ssl|crypto)\.so/ { print $1, $3 }'
}

owning_package() {
  local file
  file="$(readlink -f "$1")"
  case "$(installed_with)" in
    deb) dpkg-query -S "$file" | cut -d: -f1 ;;
    rpm) rpm -qf --qf '%{NAME}\n' "$file" ;;
  esac
}

declared_dependencies() {
  case "$(installed_with)" in
    deb) dpkg-query -W -f='${Depends}' aerospike-aql | tr ',|' '\n\n' | sed -E 's/\(.*\)//; s/^ +//; s/ +$//' ;;
    rpm) rpm -q --requires aerospike-aql | awk '{ print $1 }' ;;
  esac
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

@test "aql loads OpenSSL from a host package" {
  [ -n "$(installed_with)" ] || skip "aql was not installed from a deb or rpm"

  run loaded_openssl
  echo "$output"
  [ "$status" -eq 0 ]
  grep -q '^libssl\.so' <<< "$output"
  grep -q '^libcrypto\.so' <<< "$output"

  local lib path owner
  while read -r lib path; do
    owner="$(owning_package "$path")"
    echo "$lib -> $path (${owner:-no package})"
    [ -n "$owner" ]
    [ "$owner" != aerospike-aql ]
  done <<< "$output"
}

@test "the aql package declares the OpenSSL it loads" {
  [ -n "$(installed_with)" ] || skip "aql was not installed from a deb or rpm"

  local declared loaded
  declared="$(declared_dependencies)"
  loaded="$(loaded_openssl)"
  echo "declared: $(tr '\n' ' ' <<< "$declared")"
  [ -n "$loaded" ]

  local lib path owner
  while read -r lib path; do
    owner="$(owning_package "$path")"
    echo "$lib comes from ${owner:-no package}"
    [ -n "$owner" ]
    grep -qxF "$owner" <<< "$declared"
  done <<< "$loaded"
}
