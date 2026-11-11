package biz

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"google.golang.org/protobuf/types/known/durationpb"
)

// 未配置 API Key 时助手停用：Run 在触碰任何依赖前返回 ErrDisabled，且不写出事件。
func TestAgentUsecase_Disabled(t *testing.T) {
	t.Parallel()
	uc := NewAgentUsecase(nil, nil, nil, nil, nil, nil, nil, nil, NewAgentConfig(&conf.Assistant{}), slog.Default())
	if uc.Enabled() {
		t.Fatal("empty api_key must disable the assistant")
	}
	emitted := false
	err := uc.Run(context.Background(), RunInput{UserID: "u", Content: "hi"}, func(Event) error {
		emitted = true
		return nil
	})
	if !errors.Is(err, ErrDisabled) || emitted {
		t.Fatalf("Run() error = %v, emitted = %v; want ErrDisabled and no events", err, emitted)
	}
}

func TestNewAgentConfig(t *testing.T) {
	t.Parallel()
	cfg := NewAgentConfig(nil)
	if cfg.Enabled || cfg.Timeout != defaultRunTimeout || cfg.MaxSteps != defaultMaxSteps || cfg.MaxConcurrentRuns != defaultMaxConcurrentRuns {
		t.Fatalf("nil config defaults wrong: %+v", cfg)
	}
	cfg = NewAgentConfig(&conf.Assistant{ApiKey: "k", Timeout: durationpb.New(time.Minute), MaxSteps: 3, MaxConcurrentRuns: 2})
	if !cfg.Enabled || cfg.Timeout != time.Minute || cfg.MaxSteps != 3 || cfg.MaxConcurrentRuns != 2 {
		t.Fatalf("config not applied: %+v", cfg)
	}
}
