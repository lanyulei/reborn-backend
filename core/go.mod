module github.com/lanyulei/reborn/backend/core

go 1.26.0

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2
	github.com/go-kratos/kratos/contrib/middleware/jwt/v3 v3.0.0-20261026152050-dd0fb7df1e32
	github.com/go-kratos/kratos/contrib/otel/v3 v3.0.0-20261026152050-dd0fb7df1e32
	github.com/go-kratos/kratos/v3 v3.0.0
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/golang-migrate/migrate/v4 v4.20.1
	github.com/google/uuid v1.6.0
	github.com/google/wire v0.7.0
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.33.0
	github.com/jackc/pgerrcode v0.0.0-20250907135507-afb5586c32a6
	github.com/jackc/pgx/v5 v5.13.0
	github.com/lanyulei/reborn/backend/kit v0.0.0
	github.com/openai/openai-go/v3 v3.98.0
	go.uber.org/automaxprocs v1.6.0
	golang.org/x/crypto v0.59.0
	google.golang.org/genproto/googleapis/api v0.0.0-20261110000943-09e21b237f2c
	google.golang.org/protobuf v1.36.12
)

require (
	buf.build/go/protovalidate v1.4.0 // indirect
	cel.dev/cel-go v0.33.2 // indirect
	cel.dev/expr v0.26.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coder/websocket v1.8.15 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/go-kratos/kratos/cmd/protoc-gen-go-errors/v3 v3.0.0-20261026152050-dd0fb7df1e32 // indirect
	github.com/go-kratos/kratos/cmd/protoc-gen-go-http/v3 v3.0.0-20261026152050-dd0fb7df1e32 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-playground/form/v4 v4.5.1 // indirect
	github.com/google/subcommands v1.2.0 // indirect
	github.com/gorilla/mux v1.8.1 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/tidwall/gjson v1.20.2 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.3.0 // indirect
	github.com/tidwall/sjson v1.2.9 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/exp v0.0.0-20261109185158-a52e3aef5920 // indirect
	golang.org/x/mod v0.43.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/tools v0.52.1-0.20261106145633-d407941a339a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c // indirect
	google.golang.org/grpc v1.85.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool (
	github.com/go-kratos/kratos/cmd/protoc-gen-go-errors/v3
	github.com/go-kratos/kratos/cmd/protoc-gen-go-http/v3
	github.com/google/wire/cmd/wire
	github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2
	google.golang.org/protobuf/cmd/protoc-gen-go
)

replace github.com/lanyulei/reborn/backend/kit => ../kit
