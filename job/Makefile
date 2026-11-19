GOFLAGS?=
# 本地环境变量：backend/.env（不入库，模板见 ../.env.example），全部服务共用一份；只导出 KRATOS_* 与 SERVER_ROLES。
-include ../.env
export $(filter KRATOS_% SERVER_ROLES,$(.VARIABLES))

# 工具版本锁定：禁止 @latest（依赖治理，契约见 ../notify/AGENTS.md）。
GOLANGCI_LINT_VERSION:=v2.15.0
GOVULNCHECK_VERSION:=v1.9.0
GOFUMPT_VERSION:=v0.12.0

.PHONY: init
# init 安装锁定的本地工具链（golangci-lint / govulncheck / gofumpt）。
init:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	go install mvdan.cc/gofumpt@$(GOFUMPT_VERSION)

.PHONY: gen
# gen 生成 proto（internal/conf）与 wire 依赖注入代码。作业引擎不暴露 proto 接口
# （引擎与执行节点的 HTTP 协议定义在 kit/jobcontract），没有 api 目录，也没有远程 buf 依赖。
gen:
	buf generate --template buf.gen.config.yaml
	go tool wire gen ./cmd/job

.PHONY: run
# run 本地运行（必须用 kratos CLI）；环境变量取自 ../.env（见 ../.env.example）。
run:
	kratos run

.PHONY: format
# format 按 gofumpt 规范格式化全部源码。
format:
	gofumpt -w .

.PHONY: lint
# lint 静态检查。
lint:
	golangci-lint run ./...

.PHONY: vuln
# vuln 漏洞扫描。
vuln:
	govulncheck ./...

.PHONY: test
# test 分层执行测试：biz / runner / 全量随机顺序。
test:
	go test ./internal/biz/... -race -count=1
	go test ./internal/runner/... -race -count=1
	go test ./... -race -shuffle=on

.PHONY: tidy
# tidy 依赖治理：tidy + verify。
tidy:
	go mod tidy
	go mod verify

.PHONY: check
# check 提交前全量自检：格式化 + lint + 漏洞 + 测试。
check: format lint vuln test

# show help
help:
	@echo ''
	@echo 'Usage:'
	@echo ' make [target]'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
	helpMessage = match(lastLine, /^# (.*)/); \
		if (helpMessage) { \
			helpCommand = substr($$1, 0, index($$1, ":")); \
			helpMessage = substr(lastLine, RSTART + 2, RLENGTH); \
			printf "\033[36m%-22s\033[0m %s\n", helpCommand,helpMessage; \
		} \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help
