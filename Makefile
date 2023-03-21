include .bingo/Variables.mk

test:
	go test -race -v ./...

build: download-deps tidy-deps fmt test lint compile

fmt:
	gofmt -l -s -w .

download-deps:
	go mod download

tidy-deps:
	go mod tidy

update-deps:
	go get -u -t ./...
	go mod tidy

compile:
	go build -v ./...

lint: $(GOLANGCI_LINT) $(GORELEASER)
	$(GOLANGCI_LINT) run
	$(GORELEASER) check
