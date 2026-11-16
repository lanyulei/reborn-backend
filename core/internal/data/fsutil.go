package data

import (
	"fmt"
	"os"
)

// WriteFileAtomic 经同目录临时文件 + fsync + rename 原子落盘 target（0600）。
// 任一步失败清理临时文件，目标文件保持原状。各域的本地落盘（logo / 头像 / 附件）共用。
func WriteFileAtomic(dir, target string, data []byte) (err error) {
	tmp, err := os.CreateTemp(dir, ".upload-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if _, err = tmp.Write(data); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err = os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}
	return nil
}
