default: run

PROXY_PORT ?=5433
DATABASE_PORT ?=5432

run:
	go run cmd/pgspy/main.go \
		DATABASE_PORT=$(DATABASE_PORT) \
		PROXY_PORT=$(PROXY_PORT)
