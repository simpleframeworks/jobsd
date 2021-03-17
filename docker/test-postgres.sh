#! /bin/bash
set -e

echo "PostgreSQL is not ready, sleeping... Zzzz."
until nc -vz postgres 5432 &>/dev/null; do
  sleep 1
done
echo "PostgreSQL is ready!!!"

go test -v ./...
