#!/usr/bin/env bats

@test "can run aql" {
  aql --help
  [ "$?" -eq 0 ]
}