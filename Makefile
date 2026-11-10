GO            ?= go
STATICCHECK_VERSION := v0.8.2
GOVULNCHECK_VERSION := v1.9.0
GOSEC_VERSION       := v2.30.0
BIN           := bin
CMDS          := gateway metadata storage
COVERPROFILE  := coverage.out

ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: all build test race vet staticcheck lint vuln sec cover fuzz smoke run fmt tidy up down logs clean ci

all: lint race build

build:
	$(foreach c,$(CMDS),$(GO) build -o $(BIN)/$(c)$(EXE) ./cmd/$(c) &&) true

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

staticcheck:
	$(GO) run honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION) ./...

# Known-vulnerability scan of dependencies and the standard library.
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

# Static security analysis. Intentional uses are annotated inline with
# "#nosec G<rule> -- reason"; no rules are excluded globally.
sec:
	$(GO) run github.com/securego/gosec/v2/cmd/gosec@$(GOSEC_VERSION) -exclude-generated ./...

fmt:
	gofmt -w .

lint: vet staticcheck
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

# Coverage counts statements in internal/... hit by any test, so code
# exercised by the integration suite is included.
cover:
	$(GO) test -race -coverpkg=./internal/... -coverprofile=$(COVERPROFILE) ./...
	$(GO) tool cover -func=$(COVERPROFILE) | tail -1

FUZZTIME ?= 30s
fuzz:
	$(GO) test -run=^$$ -fuzz=^FuzzListPagination$$ -fuzztime=$(FUZZTIME) ./internal/metadata/boltstore
	$(GO) test -run=^$$ -fuzz=^FuzzChunkedReader$$ -fuzztime=$(FUZZTIME) ./internal/s3api
	$(GO) test -run=^$$ -fuzz=^FuzzParseRange$$ -fuzztime=$(FUZZTIME) ./internal/s3api

smoke:
	bash scripts/smoke.sh

run:
	$(GO) run ./cmd/gateway -data ./data/gateway

tidy:
	$(GO) mod tidy

up:
	docker compose -f deploy/docker-compose.yml up -d --build --wait

down:
	docker compose -f deploy/docker-compose.yml down -v

logs:
	docker compose -f deploy/docker-compose.yml logs -f

clean:
	rm -rf $(BIN) $(COVERPROFILE)

ci: lint race vuln sec
