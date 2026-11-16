// Package data 文件域的 data 层：对象存储驱动（本地磁盘、S3 兼容对象存储）与文件元数据仓储。
package data

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"

	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
)

// DriverLocal 本地磁盘驱动名（file_object.driver）。
const DriverLocal = "local"

var _ storage.ObjectStore = (*LocalStore)(nil)

// localKeyPattern 本地驱动接受的对象键：yyyy/mm/dd/<uuid>。键由服务端生成，
// 此处再做白名单校验作为纵深防御——不匹配的键一律拒绝，杜绝路径穿越。
var localKeyPattern = regexp.MustCompile(`^\d{4}/\d{2}/\d{2}/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// LocalStore 本地磁盘对象存储：<root>/<key>，写入经临时文件 + rename 原子可见。
// 多实例部署时 root 须为各实例共享的卷。
type LocalStore struct {
	root string
}

// NewLocalStore new a LocalStore；root 为附件根目录。
func NewLocalStore(root string) *LocalStore {
	return &LocalStore{root: root}
}

// path 校验键并解析为磁盘路径。
func (s *LocalStore) path(key string) (string, error) {
	if !localKeyPattern.MatchString(key) {
		return "", fmt.Errorf("local store: invalid object key %q", key)
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

// Put implements storage.ObjectStore.
func (s *LocalStore) Put(_ context.Context, key string, data []byte) error {
	target, err := s.path(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(target)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("create object dir: %w", err)
	}
	return basedata.WriteFileAtomic(dir, target, data)
}

// Get implements storage.ObjectStore.
func (s *LocalStore) Get(_ context.Context, key string) ([]byte, error) {
	target, err := s.path(key)
	if err != nil {
		return nil, err
	}
	// #nosec G304 -- 路径由受控根目录与白名单校验过的对象键拼接
	data, err := os.ReadFile(target)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("read object: %w", err)
	}
	return data, nil
}

// Delete implements storage.ObjectStore：不存在视为成功。
func (s *LocalStore) Delete(_ context.Context, key string) error {
	target, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(target); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove object: %w", err)
	}
	return nil
}
