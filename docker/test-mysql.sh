#! /bin/bash
set -e

echo "Waiting for MySQL - Started"
until nc -vz mysql 3306 &>/dev/null; do
  sleep 1
done
echo "Waiting for MySQL - Completed"

echo "Testing - Started"
go test -v ./...
echo "Testing - Completed"
