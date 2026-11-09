GO            ?= go
STATICCHECK_VERSION := v0.8.2
BIN           := bin
CMDS          := gateway metadata storage
COVERPROFILE  := coverage.out

ifeq ($(OS),Windows_NT)
EXE := .exe
endif

.PHONY: all build test race vet staticcheck lint cover fmt tidy up down logs clean ci

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

fmt:
	gofmt -w .

lint: vet staticcheck
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

cover:
	$(GO) test -race -coverprofile=$(COVERPROFILE) ./...
	$(GO) tool cover -func=$(COVERPROFILE) | tail -1

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

ci: lint race
