package runner

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 快速执行在作业目录里的文件名。
const (
	adhocPlaybookName = "playbook.yml"
	scriptName        = "script"
	payloadDirName    = "payload"
	// resultVar 命令 / 脚本任务登记执行结果的变量名（随后的任务据此把输出打印进作业日志）。
	resultVar = "job_result"
)

// taskKind 任务的作业类型（一期的引擎不带这个字段：0 归一为作业模板）。
func taskKind(t *jobcontract.Task) int32 {
	if t.Kind == 0 {
		return jobcontract.KindPlaybook
	}
	return t.Kind
}

// validateAdhoc 复核快速执行的内容（契约口径），并确认任务里没有夹带作业模板才有的字段。
func validateAdhoc(t *jobcontract.Task, kind int32) error {
	switch {
	case t.Adhoc == nil:
		return errors.New("缺少快速执行的内容")
	case t.Adhoc.Validate(kind) != nil:
		return errors.New("快速执行的内容不合法")
	case t.RepoURL != "" || t.Ref != "" || t.Playbook != "" || t.Git.Type > jobcontract.GitAuthNone || t.Git.Secret != "":
		return errors.New("快速执行不应带有仓库信息")
	case len(t.ExtraVars) > 0:
		return errors.New("快速执行不接受执行参数")
	case kind != jobcontract.KindFile && len(t.FileContent) > 0:
		return errors.New("只有分发文件才带文件内容")
	}
	return nil
}

// verifyPayload 核对待分发文件的内容与任务里记录的大小、SHA-256 一致（审批与审计看到的是这两个值）。
func verifyPayload(t *jobcontract.Task) error {
	sum := sha256.Sum256(t.FileContent)
	if int64(len(t.FileContent)) != t.Adhoc.FileSize || !strings.EqualFold(hex.EncodeToString(sum[:]), t.Adhoc.FileSHA256) {
		return errors.New("文件内容校验失败：大小或摘要与提交时记录的不一致")
	}
	return nil
}

// normalizeScript 把脚本的 CRLF 换行统一成 LF：带 \r 的脚本在目标主机上会因 `bash\r` 之类的解释器名
// 或行尾多出的 \r 而执行失败。
func normalizeScript(script string) []byte {
	return bytes.ReplaceAll([]byte(script), []byte("\r\n"), []byte("\n"))
}

// adhocTitle 快速执行开始时写进作业日志的平台消息。不回显命令、脚本与文件内容。
func adhocTitle(t *jobcontract.Task) string {
	scale := fmt.Sprintf("主机 %d 台，并发 %d", len(t.Hosts), t.Options.Forks)
	switch taskKind(t) {
	case jobcontract.KindCommand:
		return "执行命令（" + scale + "）"
	case jobcontract.KindScript:
		return fmt.Sprintf("执行脚本（解释器 %s，%s）", t.Adhoc.Interpreter, scale)
	default:
		return fmt.Sprintf("分发文件 %s → %s（%d 字节，%s）", t.Adhoc.FileName, t.Adhoc.Dest, t.Adhoc.FileSize, scale)
	}
}

// showOutput 命令 / 脚本任务之后的两个任务：把登记下来的标准输出、标准错误打印进作业日志
// （缺省的输出回调只在失败或 -v 时才显示任务结果）。预检模式下命令被跳过、没有输出，两个任务随之跳过。
func showOutput() []any {
	return []any{
		map[string]any{
			"name":                  plain("标准输出"),
			"ansible.builtin.debug": map[string]any{"var": plain(resultVar + ".stdout_lines")},
			"when":                  plain(resultVar + ".stdout_lines is defined"),
		},
		map[string]any{
			"name":                  plain("标准错误"),
			"ansible.builtin.debug": map[string]any{"var": plain(resultVar + ".stderr_lines")},
			"when":                  plain(resultVar + ".stderr_lines is defined and " + resultVar + ".stderr_lines | length > 0"),
		},
	}
}

// buildAdhocPlaybook 生成快速执行的 Playbook（YAML 流式写法，见 encodeVars）。Playbook 的结构由平台写死；
// 来自任务的内容（命令、脚本参数、目标目录）只作为带 !unsafe 标记的字符串出现在模块参数里——既不会改变
// YAML 结构，也不会被 Ansible 当模板求值。jobDir 是作业目录（脚本与待分发文件已写在其中）。
func buildAdhocPlaybook(t *jobcontract.Task, jobDir string) ([]byte, error) {
	a := t.Adhoc
	var (
		name  string
		tasks []any
	)
	switch taskKind(t) {
	case jobcontract.KindCommand:
		name = "执行命令"
		tasks = append([]any{map[string]any{
			"name":                  plain(name),
			"ansible.builtin.shell": map[string]any{"cmd": a.Command},
			"register":              plain(resultVar),
		}}, showOutput()...)
	case jobcontract.KindScript:
		name = "执行脚本"
		cmd := filepath.Join(jobDir, scriptName)
		if args := strings.TrimSpace(a.ScriptArgs); args != "" {
			cmd += " " + args
		}
		tasks = append([]any{map[string]any{
			"name":                   plain(name),
			"ansible.builtin.script": map[string]any{"cmd": cmd, "executable": plain(a.Interpreter)},
			"register":               plain(resultVar),
		}}, showOutput()...)
	case jobcontract.KindFile:
		name = "分发文件"
		tasks = []any{
			// 目标目录不存在才创建；已存在时不改它的权限与属主。
			map[string]any{
				"name":                 plain("确保目标目录存在"),
				"ansible.builtin.file": map[string]any{"path": a.Dest, "state": plain("directory")},
			},
			map[string]any{
				"name": plain(name),
				"ansible.builtin.copy": map[string]any{
					"src":  plain(filepath.Join(jobDir, payloadDirName, a.FileName)),
					"dest": path.Join(a.Dest, a.FileName),
					"mode": plain(a.Mode),
				},
			},
		}
	default:
		return nil, errors.New("not a quick-execution kind")
	}
	return encodeVars([]any{map[string]any{
		"name": plain(name), "hosts": plain("all"), "gather_facts": false, "tasks": tasks,
	}})
}

// writeAdhocFiles 把快速执行要用的文件写进作业目录：生成的 Playbook；脚本（0700）；待分发的文件
// （payload/<文件名>，0600）。它们都随作业目录删除。返回 Playbook 的绝对路径。
func writeAdhocFiles(t *jobcontract.Task, jobDir string) (string, error) {
	switch taskKind(t) {
	case jobcontract.KindScript:
		if err := writeFile(filepath.Join(jobDir, scriptName), normalizeScript(t.Adhoc.Script), 0o700); err != nil {
			return "", fmt.Errorf("写入脚本失败: %w", err)
		}
	case jobcontract.KindFile:
		if err := verifyPayload(t); err != nil {
			return "", err
		}
		dir := filepath.Join(jobDir, payloadDirName)
		if err := os.Mkdir(dir, 0o700); err != nil {
			return "", fmt.Errorf("写入待分发的文件失败: %w", err)
		}
		if err := writeFile(filepath.Join(dir, t.Adhoc.FileName), t.FileContent, 0o600); err != nil {
			return "", fmt.Errorf("写入待分发的文件失败: %w", err)
		}
	}
	playbook, err := buildAdhocPlaybook(t, jobDir)
	if err != nil {
		return "", fmt.Errorf("生成 Playbook 失败: %w", err)
	}
	target := filepath.Join(jobDir, adhocPlaybookName)
	if err := writeFile(target, playbook, 0o600); err != nil {
		return "", fmt.Errorf("写入 Playbook 失败: %w", err)
	}
	return target, nil
}
