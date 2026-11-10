package testfake

// 跨域事务编排的内存桩：service/server 层测试组装「业务写 + 通知事件
// 直插」链路用；无真实回滚语义，原子性断言在 data 层集成测试（真实 PG）。

import (
	"context"
	"errors"
	"time"

	biznotify "github.com/lanyulei/reborn/backend/core/internal/biz/notify"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// TxManager 是 txrun.Manager 的内存桩：直接执行闭包（恒“提交”）。
type TxManager struct{}

// NewTxManager new a TxManager.
func NewTxManager() *TxManager { return &TxManager{} }

// WithinTx implements txrun.Manager。
func (TxManager) WithinTx(_ context.Context, fn func(tx txrun.Tx) error) error {
	return fn(Tx{})
}

// Tx 是 txrun.Tx 的内存桩：Exec 恒成功（返回 1 行受影响）。
type Tx struct{}

// Exec implements txrun.Tx。
func (Tx) Exec(context.Context, string, ...any) (int64, error) { return 1, nil }

// QueryRow implements txrun.Tx：内存桩不执行 SQL（仓储桩自行实现语义），Scan 报错。
func (Tx) QueryRow(context.Context, string, ...any) txrun.Row { return unsupportedRow{} }

type unsupportedRow struct{}

func (unsupportedRow) Scan(...any) error { return errors.New("testfake: QueryRow not supported") }

// InsertEventTx implements biznotify.EventTxRepo：内存实现无事务语义，
// 与 SeedEvent 同口径追加（时间缺省取当前、深拷贝入表）。
func (s *Store) InsertEventTx(_ context.Context, _ txrun.Tx, event *biznotify.Event) error {
	s.SeedEvent(event)
	return nil
}

// dedupEntry 内存去重门闩行。
type dedupEntry struct {
	eventID string
	expire  time.Time
}

// GateDedupTx implements biznotify.EventTxRepo：与数据层单语句 upsert 同语义——
// 未过期命中返回首次 event_id；缺失或已过期则以本次 event_id 占位。
func (s *Store) GateDedupTx(_ context.Context, _ txrun.Tx, bizID, dedupKey, eventID string, ttl time.Duration) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dedup == nil {
		s.dedup = map[string]dedupEntry{}
	}
	key := bizID + "|" + dedupKey
	if e, ok := s.dedup[key]; ok && e.expire.After(time.Now()) {
		return e.eventID, nil
	}
	s.dedup[key] = dedupEntry{eventID: eventID, expire: time.Now().Add(ttl)}
	return eventID, nil
}

// ExpireDedup 测试辅助：把门闩置为已过期（模拟 TTL 流逝）。
func (s *Store) ExpireDedup(bizID, dedupKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.dedup[bizID+"|"+dedupKey]; ok {
		e.expire = time.Now().Add(-time.Second)
		s.dedup[bizID+"|"+dedupKey] = e
	}
}
