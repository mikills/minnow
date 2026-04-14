.PHONY: test build run

test:
	go test ./... -v -race -count=1

build:
	go build ./...

run:
	go run .
