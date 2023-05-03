include .bingo/Variables.mk

test:
	go test -race -v ./...

build: download-deps tidy-deps fmt compile test lint

fmt:
	gofmt -l -s -w .

download-deps:
	go mod download

tidy-deps:
	go mod tidy

update-binary-deps: $(BINGO)
	$(BINGO) get bingo@latest
	$(BINGO) get golangci-lint@latest
	$(BINGO) get goreleaser@latest

update-deps: $(update-binary-deps)
	go get -u -t ./...
	go mod tidy

compile:
	go build -v ./...

lint: $(GOLANGCI_LINT) $(GORELEASER)
	$(GOLANGCI_LINT) run
	$(GORELEASER) check
