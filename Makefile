.PHONY: build test test-unit test-integration lint docker deploy teardown clean

BIN := bin/orbitcp
IMAGE := orbitcp:latest
TEST_PG_DSN ?= postgres://postgres:postgres@127.0.0.1:5432/orbitcp?sslmode=disable

build: ## Compile the binary
	GOFLAGS=-mod=mod GOPROXY=off go build -o $(BIN) .

test: test-unit test-integration ## Run the full test suite (unit + integration)

test-unit: ## Run unit tests with race detector + coverage (persistence tests need TEST_PG_DSN)
	@UNFORMATTED=$$(gofmt -l . | grep -v third_party || true); \
	if [ -n "$$UNFORMATTED" ]; then \
		echo "gofmt found unformatted files:"; echo "$$UNFORMATTED"; \
		echo "run 'make fmt' to fix"; exit 1; \
	fi
	GOFLAGS=-mod=mod GOPROXY=off go vet ./...
	TEST_PG_DSN=$(TEST_PG_DSN) GOFLAGS=-mod=mod GOPROXY=off go test ./... -race -cover

test-integration: ## Run the multi-node integration/chaos test (real 5-node cluster + Postgres)
	TEST_PG_DSN=$(TEST_PG_DSN) bash test/integration_test.sh

fmt: ## Format all Go source
	gofmt -w .

docker: ## Build the container image
	docker build -t $(IMAGE) .

deploy: ## Full local deploy pipeline (kind cluster + Postgres + build + apply manifests)
	./setup.sh

teardown: ## Delete the local kind cluster
	./setup.sh teardown

clean: ## Remove build artifacts
	rm -rf bin/ /tmp/orbitcp*