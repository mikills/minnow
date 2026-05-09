.PHONY: test build run fmt

GOFILES := $(shell find . -name '*.go' -not -path './testdata/*')
GOLINES := go run github.com/segmentio/golines@latest
MAX_LEN ?= 120

test:
	go test ./... -v -race -count=1

build:
	go build ./...

run:
	go run .

fmt:
	$(GOLINES) --max-len=$(MAX_LEN) -w .
	gofmt -w $(GOFILES)
