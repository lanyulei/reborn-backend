package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

// TestLoadMergesSharedServiceEnv 共享基线 → 服务配置 → 环境变量逐层覆盖，嵌套段逐键合并。
func TestLoadMergesSharedServiceEnv(t *testing.T) {
	dir := t.TempDir()
	svc := "data:\n  database:\n    max_conns: 32\nserver:\n  http:\n    addr: 0.0.0.0:1\n"
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(svc), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KRATOS_DATA_DATABASE_SOURCE", "host=db user=svc")
	t.Setenv("KRATOS_NOTIFY_MASTER_KEY", "k")

	out := &structpb.Struct{}
	if err := Load(dir, out); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	db := out.GetFields()["data"].GetStructValue().GetFields()["database"].GetStructValue().GetFields()
	if got := db["source"].GetStringValue(); got != "host=db user=svc" {
		t.Errorf("source = %q, want the env value", got)
	}
	if got := db["max_conns"].GetNumberValue(); got != 32 {
		t.Errorf("max_conns = %v, want the service override 32", got)
	}
	if db["min_conns"].GetNumberValue() != 2 || db["max_conn_idle_time"].GetStringValue() != "1800s" {
		t.Errorf("min_conns / max_conn_idle_time = %v / %v, want the shared baseline", db["min_conns"], db["max_conn_idle_time"])
	}
	key := out.GetFields()["notify"].GetStructValue().GetFields()["master"].GetStructValue().GetFields()["key"]
	if key.GetStringValue() != "k" {
		t.Errorf("master key = %q, want the env value", key.GetStringValue())
	}
	addr := out.GetFields()["server"].GetStructValue().GetFields()["http"].GetStructValue().GetFields()["addr"]
	if addr.GetStringValue() != "0.0.0.0:1" {
		t.Errorf("addr = %q, want the service value", addr.GetStringValue())
	}
}

// TestLoadUnsetSecretsResolveEmpty 未注入环境变量时占位符解析为空串，由装配阶段各自快速失败。
func TestLoadUnsetSecretsResolveEmpty(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("server: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KRATOS_DATA_DATABASE_SOURCE", "")
	t.Setenv("KRATOS_NOTIFY_MASTER_KEY", "")
	out := &structpb.Struct{}
	if err := Load(dir, out); err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	db := out.GetFields()["data"].GetStructValue().GetFields()["database"].GetStructValue().GetFields()
	if got, ok := db["source"]; !ok || got.GetStringValue() != "" {
		t.Errorf("source = %v, want an empty string", got)
	}
}
