package biz

import (
	"context"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// FileRef 容器内的一个路径。
type FileRef struct {
	Namespace string
	Pod       string
	Container string
	// Path 下载：文件或目录；上传：目标目录。绝对路径。
	Path string
}

// FileBegin 下载开始时回调一次（此后才有内容写出）：name 为建议的文件名，isTar 表示内容是 tar（目录），
// size 为单个文件的字节数（目录为 -1）。返回写入内容的目标。
type FileBegin func(name string, isTar bool, size int64) io.Writer

// FileStore 以用户身份经 exec + tar 读写容器内的文件（data/kube 实现）。能否访问由集群判定
// （pods/exec 的权限）。
type FileStore interface {
	// Download 把 ref.Path 的内容写给 begin 返回的目标，返回写出的字节数；超过 maxBytes 中止。
	Download(ctx context.Context, c *Cluster, id Identity, ref FileRef, maxBytes int64, begin FileBegin) (int64, error)
	// Upload 把 r 的 size 个字节写为 ref.Path 目录下名为 name 的文件。
	Upload(ctx context.Context, c *Cluster, id Identity, ref FileRef, name string, size int64, r io.Reader) error
}

// ErrFileInvalid 文件传输参数不合法（400）。
func ErrFileInvalid(format string, args ...any) *Error {
	return NewError(http.StatusBadRequest, k8scontract.ReasonFileInvalid, format, args...)
}

// ErrFileTooLarge 文件超过上限（413）。
func ErrFileTooLarge(limit int64) *Error {
	return NewError(http.StatusRequestEntityTooLarge, k8scontract.ReasonFileTooLarge, "file exceeds the transfer limit of %d MiB", limit>>20)
}

// CheckContainerPath 校验容器内的绝对路径：必须规范化（无 . / .. / 重复斜杠）、不含控制字符。
func CheckContainerPath(p string, allowRoot bool) error {
	if p == "" || p[0] != '/' || len(p) > 4096 || path.Clean(p) != p {
		return ErrFileInvalid("path must be a clean absolute path")
	}
	if p == "/" && !allowRoot {
		return ErrFileInvalid("refusing to transfer the root directory")
	}
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return ErrFileInvalid("path contains control characters")
		}
	}
	return nil
}

// CheckFileName 校验上传的文件名：单个路径段，不是 . / ..，不含斜杠与控制字符。
func CheckFileName(name string) error {
	if name == "" || len(name) > 255 || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
		return ErrFileInvalid("file name must be a single path segment")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return ErrFileInvalid("file name contains control characters")
		}
	}
	return nil
}

// SafeTarEntry 报告 tar 条目名是否安全且位于 base 之内：相对路径、无 .. 段、等于 base 或在其之下。
func SafeTarEntry(name, base string) bool {
	if name == "" || strings.HasPrefix(name, "/") || strings.ContainsRune(name, 0) {
		return false
	}
	clean := path.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == ".." {
			return false
		}
	}
	return clean == base || strings.HasPrefix(clean, base+"/")
}

// SafeTarLink 报告链接条目的目标是否安全：相对路径且不逃出条目所在的目录树（按 base 为根计算）。
func SafeTarLink(entry, target string) bool {
	if target == "" || strings.HasPrefix(target, "/") || strings.ContainsRune(target, 0) {
		return false
	}
	resolved := path.Clean(path.Join(path.Dir(entry), target))
	return resolved != ".." && !strings.HasPrefix(resolved, "../") && !strings.HasPrefix(resolved, "/")
}

// FileTransfer 容器文件传输用例：集群判定 → 参数校验 → 传输 → 审计（只记路径、大小、方向，不记内容）。
type FileTransfer struct {
	registry *Registry
	prober   *Prober
	scopes   *ScopeSyncer
	store    FileStore
	auditor  *Auditor
	maxBytes int64
}

// NewFileTransfer new a FileTransfer（上限缺省 100MiB，WithMaxBytes 调整）。
func NewFileTransfer(registry *Registry, prober *Prober, scopes *ScopeSyncer, store FileStore, auditor *Auditor) *FileTransfer {
	return &FileTransfer{registry: registry, prober: prober, scopes: scopes, store: store, auditor: auditor, maxBytes: k8scontract.DefaultMaxFileBytes}
}

// WithMaxBytes 设置单次传输的大小上限（≤0 保持缺省）。
func (f *FileTransfer) WithMaxBytes(n int64) {
	if n > 0 {
		f.maxBytes = n
	}
}

// MaxBytes 单次传输的大小上限。
func (f *FileTransfer) MaxBytes() int64 { return f.maxBytes }

func (f *FileTransfer) cluster(clusterID string, ref FileRef) (*Cluster, error) {
	c, err := gate(f.registry, f.prober, f.scopes, clusterID)
	if err != nil {
		return nil, err
	}
	if c.ExecDisabled { // 文件传输经 exec 实现：禁止终端的集群同样禁止
		return nil, errExecDisabled
	}
	if ref.Namespace == "" || ref.Pod == "" {
		return nil, errInvalid("namespace and pod are required")
	}
	return c, nil
}

func fileAudit(ref FileRef, action string, extra map[string]any) *AuditEvent {
	patch := map[string]any{"container": ref.Container, "path": ref.Path}
	for k, v := range extra {
		patch[k] = v
	}
	return &AuditEvent{
		Ref:  k8scontract.Ref{Version: "v1", Resource: "pods", Namespace: ref.Namespace, Name: ref.Pod},
		Kind: "Pod", Action: action, Patch: patch,
	}
}

// Download 下载文件或目录。
func (f *FileTransfer) Download(ctx context.Context, clusterID string, id Identity, ref FileRef, begin FileBegin) error {
	c, err := f.cluster(clusterID, ref)
	if err != nil {
		return err
	}
	if err := CheckContainerPath(ref.Path, false); err != nil {
		return err
	}
	n, err := f.store.Download(ctx, c, id, ref, f.maxBytes, begin)
	ev := fileAudit(ref, k8scontract.ActionFileDownload, map[string]any{"size": n})
	ev.Err = err
	f.auditor.Record(ctx, c, id, []*AuditEvent{ev})
	return err
}

// Upload 上传一个文件到目录 ref.Path。
func (f *FileTransfer) Upload(ctx context.Context, clusterID string, id Identity, ref FileRef, name string, size int64, r io.Reader) error {
	c, err := f.cluster(clusterID, ref)
	if err != nil {
		return err
	}
	if err := CheckContainerPath(ref.Path, true); err != nil {
		return err
	}
	if err := CheckFileName(name); err != nil {
		return err
	}
	if size < 0 {
		return ErrFileInvalid("Content-Length is required")
	}
	if size > f.maxBytes {
		return ErrFileTooLarge(f.maxBytes)
	}
	err = f.store.Upload(ctx, c, id, ref, name, size, r)
	ev := fileAudit(ref, k8scontract.ActionFileUpload, map[string]any{"name": name, "size": size})
	ev.Err = err
	f.auditor.Record(ctx, c, id, []*AuditEvent{ev})
	return err
}
