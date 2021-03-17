


test: test-sqlite test-postgres cleanup

test-sqlite:
	docker-compose run go116 go test -v ./...

test-postgres:
	docker-compose run -e JOBSD_DB=postgres go116 docker/test-postgres.sh

cleanup:
	docker-compose down

