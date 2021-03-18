.PHONY: test test-sqlite test-postgres cleanup

test: test-sqlite test-postgres cleanup

test-local:
	@echo "Running SQLite tests - Started"
	@go test -race -v ./...
	@echo "Running SQLite tests - Completed"

test-sqlite:
	@echo "Running SQLite tests - Started"
	@docker-compose run go116 go test -race -v ./...
	@echo "Running SQLite tests - Completed"

test-postgres:
	@echo "Running PostgreSQL tests - Started"
	@docker-compose up -d postgres
	@docker-compose run -e JOBSD_DB=postgres go116 docker/test-postgres.sh
	@docker-compose rm -svf postgres
	@echo "Running PostgreSQL tests - Completed"

cleanup:
	@docker-compose down

