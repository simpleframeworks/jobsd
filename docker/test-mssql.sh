#! /bin/bash
set -e

echo "Waiting for MSSQL - Started"
until nc -vz mssql 1433 &>/dev/null; do
  sleep 1
done
echo "Waiting for MSSQL - Completed"

echo "Testing - Started"
go test -v ./...
echo "Testing - Completed"
