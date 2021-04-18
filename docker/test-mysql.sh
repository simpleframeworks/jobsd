#! /bin/bash
set -e

echo "Waiting for MySQL - Started"
until nc -vz mysql 3306 &>/dev/null; do
  sleep 1
done
echo "Waiting for MySQL - Completed"

echo "Setting up DB - Started"
go run ./testing/main.go
echo "Setting up DB - Completed"

echo "Testing - Started"
go test -v -timeout 30s ./...
echo "Testing - Completed"
