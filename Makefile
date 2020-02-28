.PHONY: lint format test-short

test-short:
	docker-compose down
	docker-compose up -d
	go test ./... -short

format:
	go fmt ./...

lint:
	golangci-lint run \
		-E misspell \
		-E rowserrcheck \
		-E wsl \
		-E gocritic \
		-E maligned

sql-shell:
	docker-compose exec cockroachdb -- ./cockroach sql --insecure
