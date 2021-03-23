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

test-mysql:
	@echo "Running MySQL tests - Started"
	@docker-compose up -d mysql
	@docker-compose run -e JOBSD_DB=mysql go116 docker/test-mysql.sh
	@docker-compose rm -svf mysql
	@echo "Running MySQL tests - Completed"

test-mssql:
	@echo "Running MSSQL tests - Started"
	@docker-compose up -d mssql
	@docker-compose run -e JOBSD_DB=mssql go116 docker/test-mssql.sh
	@docker-compose rm -svf mssql
	@echo "Running MSSQL tests - Completed"

cleanup:
	@docker-compose down

