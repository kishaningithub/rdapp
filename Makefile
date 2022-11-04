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
	go get -u ./...
	go mod tidy

compile:
	go build -v ./...

install-binary-dependencies:
	go list -tags=tools -f '{{ join .Imports "\n" }}' ./tools | xargs go install -v

generate-sources: install-binary-dependencies
	go generate -v ./...

lint: install-binary-dependencies
	golangci-lint run
