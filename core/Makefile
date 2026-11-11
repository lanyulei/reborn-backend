GOFLAGS?=
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
# gen 生成 proto（api + internal/conf）与 wire 依赖注入代码。
# buf dep update 需访问 buf.build（BSR），网络不稳时可能失败；buf.lock 已锁定
# 依赖且缓存于 ~/.cache/buf，buf generate 可离线完成，故失败仅告警不中断。
gen:
	@buf dep update || echo "warn: buf dep update 失败（buf.build 不可达），改用本地缓存继续"
	buf generate
	buf generate --template buf.gen.config.yaml
	go tool wire gen ./cmd/server

.PHONY: dep
# dep 刷新 BSR 远程依赖锁定（buf.lock），需可访问 buf.build；变更 buf.yaml deps 后执行。
dep:
	buf dep update

.PHONY: run
# run 本地运行（必须用 kratos CLI）。
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
# test 分层执行测试：biz / data / 全量随机顺序。
test:
	go test ./internal/biz/... -race -count=1
	go test ./internal/data/... -race -count=1
	go test ./... -race -shuffle=on

.PHONY: migrate
# migrate 应用全部未执行的 migrations/*.up.sql（golang-migrate；DSN 读 configs/config.yaml，可重复执行）。
migrate:
	go run ./cmd/migrate -conf configs up

.PHONY: migrate-down
# migrate-down 回退最近一个版本（执行其 down.sql）。
migrate-down:
	go run ./cmd/migrate -conf configs down

.PHONY: migrate-version
# migrate-version 显示当前版本与 dirty 状态。
migrate-version:
	go run ./cmd/migrate -conf configs version

.PHONY: migrate-force
# migrate-force 直接写入版本号、不执行 SQL：为手工建库的旧环境设基线或修复 dirty，如 make migrate-force VERSION=23。
migrate-force:
	@test -n "$(VERSION)" || { echo "usage: make migrate-force VERSION=23"; exit 1; }
	go run ./cmd/migrate -conf configs force $(VERSION)

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
