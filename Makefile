unit-test:
	go test -v ./...

test: unit-test

build: download-deps tidy-deps fmt test compile

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
