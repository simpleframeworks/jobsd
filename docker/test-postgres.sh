#! /bin/bash
set -e

echo "Waiting for PostgreSQL - Started"
until nc -vz postgres 5432 &>/dev/null; do
  sleep 1
done
echo "Waiting for PostgreSQL - Completed"

echo "Testing - Started"
go test -v ./...
echo "Testing - Completed"
