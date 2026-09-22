.PHONY: help build test test-race vet fmt demo worker dashboard mcp clean \
	sam-validate sam-build sam-deploy sam-outputs build-WorkerFunction build-WebhookFunction

BIN := bin
DATA ?= .icr

help:
	@echo "make build      build bin/icr, bin/worker, bin/dashboard"
	@echo "make test       run all unit tests (no network, no AWS)"
	@echo "make demo       reset local data, ingest demo/tickets, print status + outbox"
	@echo "make worker     run the worker (drop zone + webhook on :8081 + queue)"
	@echo "make dashboard  run the review dashboard on http://127.0.0.1:8080"
	@echo "make mcp        run the MCP server on stdio (read-only)"

build:
	go build -o $(BIN)/icr ./cmd/cli
	go build -o $(BIN)/worker ./cmd/worker
	go build -o $(BIN)/dashboard ./cmd/dashboard

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

demo: build
	./demo/run-demo.sh

worker: build
	$(BIN)/worker -data $(DATA)

dashboard: build
	$(BIN)/dashboard -data $(DATA)

mcp: build
	$(BIN)/icr -data $(DATA) mcp

clean:
	rm -rf $(BIN) $(DATA)

# --- AWS SAM (phase 5) -------------------------------------------------------
sam-validate:
	cd infra && sam validate --lint

sam-build:
	cd infra && sam build

sam-deploy: sam-build
	cd infra && sam deploy

sam-outputs:
	aws cloudformation describe-stacks --stack-name rivergate-icr-pipeline --profile demos-admin --region us-west-2 \
		--query "Stacks[0].Outputs[].[OutputKey,OutputValue]" --output table

# Called by `sam build` (BuildMethod: makefile, CodeUri: repo root).
build-WorkerFunction:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags="-s -w" -o $(ARTIFACTS_DIR)/bootstrap ./cmd/lambda/worker
	mkdir -p $(ARTIFACTS_DIR)/config && cp config/rivergate.yaml $(ARTIFACTS_DIR)/config/

build-WebhookFunction:
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -ldflags="-s -w" -o $(ARTIFACTS_DIR)/bootstrap ./cmd/lambda/webhook
	mkdir -p $(ARTIFACTS_DIR)/config && cp config/rivergate.yaml $(ARTIFACTS_DIR)/config/
