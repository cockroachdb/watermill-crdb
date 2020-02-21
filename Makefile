.PHONY: lint format test-short sql-shell

test-short:
	docker-compose down
	docker-compose up -d
	go test ./... -short

format:
	go fmt ./...

lint:
	golangci-lint run \
		-E gocritic \
		-E misspell \
		-E wsl

sql-shell:
	docker-compose exec cockroachdb ./cockroach sql --insecure
