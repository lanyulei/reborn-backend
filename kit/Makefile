.PHONY: format lint vuln test check
format:
	gofumpt -w .
lint:
	golangci-lint run ./...
vuln:
	govulncheck ./...
test:
	go test ./... -race -shuffle=on
check: format lint vuln test
