#!/usr/bin/env bats

@test "can run aql" {
  aql --help
  [ "$?" -eq 0 ]
}

@test "aql reports version" {
  aql --version
  [ "$?" -eq 0 ]
}
