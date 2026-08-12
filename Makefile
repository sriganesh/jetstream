GO_CMD_W_CGO = CGO_ENABLED=1 GOOS=linux go
GO_CMD = CGO_ENABLED=0 GOOS=linux go
JETSTREAM_VERSION = sha-$(shell git rev-parse HEAD)

.PHONY: help
help: ## Print info about all commands
	@echo "Commands:"
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    \033[01;32m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build Jetstream
	@echo "Building Jetstream Go binary..."
	$(GO_CMD_W_CGO) build -o jetstream cmd/jetstream/*.go

.PHONY: test
test: ## Run tests
	go test ./...

.PHONY: lint
lint: ## Verify code style and run static checks
	go vet ./...
	test -z $(gofmt -l ./...)

.PHONY: fmt
fmt: ## Run syntax re-formatting (modify in place)
	go fmt ./...

.PHONY: run
run: .env ## Run Jetstream
	@echo "Running Jetstream..."
	$(GO_CMD_W_CGO) run cmd/jetstream/*.go

.PHONY: up
up:
	@echo "Starting Jetstream..."
	JETSTREAM_VERSION=${JETSTREAM_VERSION} docker compose up -d

.PHONY: rebuild
rebuild:
	@echo "Starting Jetstream..."
	JETSTREAM_VERSION=${JETSTREAM_VERSION} docker compose up -d --build

.PHONY: down
down:
	@echo "Stopping Jetstream..."
	JETSTREAM_VERSION=${JETSTREAM_VERSION} docker compose down
