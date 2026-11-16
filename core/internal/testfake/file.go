package testfake

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	bizfile "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
)

// 编译期断言。
var (
	_ bizfile.Repo        = (*FileStore)(nil)
	_ bizfile.BindRepo    = (*FileStore)(nil)
	_ bizfile.PurgeRepo   = (*FileStore)(nil)
	_ storage.ObjectStore = (*ObjectStore)(nil)
)

// fileEntry 文件元数据行（含软删时间）。
type fileEntry struct {
	file       bizfile.File
	deleteTime time.Time
}

// FileStore 是文件元数据仓储的内存实现，语义对齐 data 层（条件绑定、软删、清理）。
type FileStore struct {
	mu    sync.Mutex
	files map[string]*fileEntry
}

// NewFileStore new a FileStore.
func NewFileStore() *FileStore { return &FileStore{files: map[string]*fileEntry{}} }

// Create implements bizfile.Repo：未绑定的临时文件数达到上限时拒绝。
func (s *FileStore) Create(_ context.Context, f *bizfile.File, maxTemp int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	temp := 0
	for _, e := range s.files {
		if e.deleteTime.IsZero() && e.file.Status == bizfile.StatusTemp && e.file.CreateUser == f.CreateUser {
			temp++
		}
	}
	if temp >= maxTemp {
		return bizfile.ErrFileQuotaExceeded
	}
	s.files[f.ID] = &fileEntry{file: *f}
	return nil
}

// Discard implements bizfile.Repo.
func (s *FileStore) Discard(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.files[id]; ok && e.file.Status == bizfile.StatusTemp {
		delete(s.files, id)
	}
	return nil
}

// Get implements bizfile.Repo.
func (s *FileStore) Get(_ context.Context, id string) (*bizfile.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.files[id]
	if !ok || !e.deleteTime.IsZero() {
		return nil, bizfile.ErrFileNotFound
	}
	cp := e.file
	return &cp, nil
}

// BindTx implements bizfile.BindRepo：只命中「临时 + 上传者匹配 + 未删除」的行。
func (s *FileStore) BindTx(_ context.Context, _ txrun.Tx, uploader, bizType, bizID string, fileIDs []string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, id := range fileIDs {
		e, ok := s.files[id]
		if !ok || !e.deleteTime.IsZero() || e.file.Status != bizfile.StatusTemp || e.file.CreateUser != uploader {
			continue
		}
		e.file.Status, e.file.BizType, e.file.BizID = bizfile.StatusBound, bizType, bizID
		n++
	}
	return n, nil
}

// ListByBiz implements bizfile.BindRepo.
func (s *FileStore) ListByBiz(_ context.Context, bizType, bizID string, limit int) ([]*bizfile.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*bizfile.File
	for _, e := range s.files {
		if e.deleteTime.IsZero() && e.file.Status == bizfile.StatusBound && e.file.BizType == bizType && e.file.BizID == bizID {
			cp := e.file
			out = append(out, &cp)
		}
	}
	slices.SortFunc(out, func(a, b *bizfile.File) int { return strings.Compare(a.ID, b.ID) })
	return out[:min(len(out), limit)], nil
}

// ReleaseTx implements bizfile.BindRepo.
func (s *FileStore) ReleaseTx(_ context.Context, _ txrun.Tx, bizType, bizID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.files {
		if e.deleteTime.IsZero() && e.file.Status == bizfile.StatusBound && e.file.BizType == bizType && e.file.BizID == bizID {
			e.deleteTime = time.Now()
		}
	}
	return nil
}

// PurgeExpired implements bizfile.PurgeRepo.
func (s *FileStore) PurgeExpired(ctx context.Context, tempBefore, deletedBefore time.Time, _ time.Duration, limit int, remove func(ctx context.Context, f *bizfile.File) error) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.files))
	for id, e := range s.files {
		expiredTemp := e.deleteTime.IsZero() && e.file.Status == bizfile.StatusTemp && e.file.CreateTime.Before(tempBefore)
		expiredDeleted := !e.deleteTime.IsZero() && e.deleteTime.Before(deletedBefore)
		if expiredTemp || expiredDeleted {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	purged := 0
	for _, id := range ids {
		f := s.files[id].file
		if err := remove(ctx, &f); err != nil {
			return purged, err
		}
		delete(s.files, id)
		purged++
	}
	return purged, nil
}

// Backdate 把文件的上传时间（及软删时间，若已软删）前移 d，供清理用例构造过期数据。
func (s *FileStore) Backdate(id string, d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.files[id]; ok {
		e.file.CreateTime = e.file.CreateTime.Add(-d)
		if !e.deleteTime.IsZero() {
			e.deleteTime = e.deleteTime.Add(-d)
		}
	}
}

// ObjectStore 是 storage.ObjectStore 的内存实现。
type ObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

// NewObjectStore new an ObjectStore.
func NewObjectStore() *ObjectStore { return &ObjectStore{objects: map[string][]byte{}} }

// Put implements storage.ObjectStore.
func (s *ObjectStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = slices.Clone(data)
	return nil
}

// Get implements storage.ObjectStore.
func (s *ObjectStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, storage.ErrObjectNotFound
	}
	return slices.Clone(data), nil
}

// Delete implements storage.ObjectStore.
func (s *ObjectStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
	return nil
}

// Len 当前对象数（清理断言用）。
func (s *ObjectStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.objects)
}

// FileChecker 是 bizfile.AccessChecker 的桩：按 userID|bizID 白名单放行。
type FileChecker map[string]bool

// CanReadFile implements bizfile.AccessChecker.
func (c FileChecker) CanReadFile(_ context.Context, userID, bizID string) (bool, error) {
	return c[userID+"|"+bizID], nil
}

// NewFileUsecase 组装文件用例（内存仓储 + 内存对象存储 + 给定检查器）。
func NewFileUsecase(checkers bizfile.AccessCheckers) (*bizfile.Usecase, *FileStore, *ObjectStore, error) {
	files, objects := NewFileStore(), NewObjectStore()
	uc, err := bizfile.NewUsecase(files, files, files,
		bizfile.Stores{Default: "memory", Drivers: map[string]storage.ObjectStore{"memory": objects}}, checkers)
	return uc, files, objects, err
}
