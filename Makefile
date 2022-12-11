unit-test:
	go test -race -v ./...

test: generate-sources unit-test

build: download-deps tidy-deps fmt test lint compile

fmt: ## Run the code formatter
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

install-binary-dependencies:
	go list -tags=tools -f '{{ join .Imports "\n" }}' ./tools | xargs go get -v
	go list -tags=tools -f '{{ join .Imports "\n" }}' ./tools | xargs go install -v
	go mod tidy

generate-sources: install-binary-dependencies
	go generate -v ./...

lint: fmt install-binary-dependencies
	golangci-lint run
	goreleaser check
