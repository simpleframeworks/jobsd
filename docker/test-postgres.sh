#! /bin/bash
set -e

echo "Waiting for PostgreSQL - Started"
until nc -vz postgres 5432 &>/dev/null; do
  sleep 1
done
echo "Waiting for PostgreSQL - Completed"

echo "Setting up DB - Started"
go run ./testing/main.go
echo "Setting up DB - Completed"

echo "Testing - Started"
go test -v -timeout 30s ./...
echo "Testing - Completed"
