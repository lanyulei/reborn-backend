.PHONY: gen format lint vuln test check
# gen 生成 proto/kitconf（各服务 internal/conf 共用的配置结构）。
gen:
	buf generate
format:
	gofumpt -w .
lint:
	golangci-lint run ./...
vuln:
	govulncheck ./...
test:
	go test ./... -race -shuffle=on
check: format lint vuln test
