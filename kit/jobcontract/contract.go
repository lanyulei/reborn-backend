// Package jobcontract 是 core 管理面（job 域）、执行引擎与执行节点（backend/job）之间的数据契约：
// 三者互不 import 业务代码，共享的只有 job_* 表"落库长什么样"——jsonb 列结构（json 标签即落库
// 格式）、状态与枚举字面量——以及引擎与执行节点之间的 HTTP 协议（路径、请求 / 响应结构），和各方
// 必须算出同一结果的纯函数（地址 / 路径 / 仓库地址校验、终态判定、接入令牌）。任何一方改契约必须
// 改这里，由编译器把漂移点全部暴露。本包不 import 任何驱动与传输层，biz 层可用。
//
// 表结构见 core/migrations/0070_job_init.up.sql。
package jobcontract

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
)

// 凭据类型（job_credential.type）。
const (
	CredentialSSHKey      int32 = 1 // SSH 私钥
	CredentialSSHPassword int32 = 2 // SSH 密码
)

// 提权方式（job_credential.become_method）；空串 = 不提权。
const (
	BecomeSudo = "sudo"
	BecomeSu   = "su"
)

// ValidBecomeMethod 报告提权方式是否受支持（含空串）。
func ValidBecomeMethod(m string) bool { return m == "" || m == BecomeSudo || m == BecomeSu }

// 项目的 Git 认证方式（job_project.auth_type）。
const (
	GitAuthNone   int32 = 1 // 无（公开仓库）
	GitAuthToken  int32 = 2 // 用户名 + 令牌（仅 http / https 地址）
	GitAuthSSHKey int32 = 3 // SSH 私钥（仅 ssh 地址）
)

// 目标主机的选择方式（Target.Mode）。
const (
	TargetModeCIs    int32 = 1 // 指定资源
	TargetModeFilter int32 = 2 // 按条件筛选（执行时解析）
)

// 触发来源（job_execution.trigger）。
const (
	TriggerManual   int32 = 1 // 手动
	TriggerSchedule int32 = 2 // 定时作业
	TriggerWorkflow int32 = 3 // 流程节点（开放接口）
	TriggerAlert    int32 = 4 // 告警自愈
)

// 作业类型（job_execution.kind / Spec.Kind / Task.Kind）。
const (
	KindPlaybook int32 = 1 // 作业模板：项目中的 Playbook
	KindCommand  int32 = 2 // 快速执行：一条 Shell 命令
	KindScript   int32 = 3 // 快速执行：一段脚本
	KindFile     int32 = 4 // 快速执行：分发一个文件
)

// ValidKind 报告作业类型是否受支持。
func ValidKind(k int32) bool { return k >= KindPlaybook && k <= KindFile }

// 脚本解释器（Adhoc.Interpreter）。
const (
	InterpreterBash   = "bash"
	InterpreterSh     = "sh"
	InterpreterPython = "python3"
)

// ValidInterpreter 报告脚本解释器是否受支持。
func ValidInterpreter(i string) bool {
	return i == InterpreterBash || i == InterpreterSh || i == InterpreterPython
}

// 执行状态（job_execution.status）。
const (
	StatusQueued   int32 = 1  // 排队中
	StatusRunning  int32 = 2  // 执行中
	StatusSuccess  int32 = 3  // 成功
	StatusFailed   int32 = 4  // 失败
	StatusPartial  int32 = 5  // 部分失败：有主机成功，也有主机失败或不可达
	StatusCanceled int32 = 6  // 已取消
	StatusTimeout  int32 = 7  // 超时
	StatusUnknown  int32 = 8  // 结果未知：执行节点失联，作业可能仍在运行或已中断
	StatusPending  int32 = 9  // 待审批：审批通过后才进入排队
	StatusRejected int32 = 10 // 审批未通过（或审批被撤销）：不会执行
)

// ValidStatus 报告执行状态是否在取值范围内。
func ValidStatus(s int32) bool { return s >= StatusQueued && s <= StatusRejected }

// Active 报告执行是否尚未结束（待审批 / 排队中 / 执行中）。未结束的互斥执行占着模板的互斥位。
func Active(s int32) bool { return s == StatusQueued || s == StatusRunning || s == StatusPending }

// 主机结果（job_execution_host.status）。
const (
	HostPending     int32 = 0 // 未执行
	HostOK          int32 = 1 // 成功
	HostChanged     int32 = 2 // 成功且有变更
	HostFailed      int32 = 3 // 失败
	HostUnreachable int32 = 4 // 不可达
	HostSkipped     int32 = 5 // 已跳过
)

// 执行节点上报的结束方式（FinishRequest.Outcome）。
const (
	OutcomeExited   = "exited"   // ansible-playbook 自行退出，退出码见 ExitCode
	OutcomeCanceled = "canceled" // 应取消请求而终止
	OutcomeTimeout  = "timeout"  // 超时被终止
	OutcomeError    = "error"    // 未能启动（拉取代码失败、Playbook 不存在等），原因见 Error
)

// 限额。
const (
	// MaxHosts 一次执行的目标主机数上限。
	MaxHosts = 500
	// MaxParams 一个模板的执行参数个数上限。
	MaxParams = 30
	// MaxLogChunkBytes 单个日志块的字节数上限。
	MaxLogChunkBytes = 256 << 10
	// MaxLogChunks 一次上报携带的日志块数上限。
	MaxLogChunks = 16
	// MaxLogBytes 一次执行的日志总字节数上限；超出后内容丢弃并标记截断（序号照常确认）。
	MaxLogBytes = 32 << 20
	// MaxErrorLen job_execution.error 的字符数上限。
	MaxErrorLen = 1024

	// MaxCommandLen 快速执行的命令长度上限（字节）。
	MaxCommandLen = 4096
	// MaxScriptBytes 脚本内容的字节数上限。
	MaxScriptBytes = 256 << 10
	// MaxScriptArgsLen 脚本参数的长度上限（字节）。
	MaxScriptArgsLen = 1024
	// MaxFileBytes 分发文件的字节数上限：文件内容随任务下发给执行节点（base64 后须放得进一次请求）。
	MaxFileBytes = 4 << 20
	// MaxSlices 一次提交拆成的执行数上限（作业切片）；每片至多 MaxHosts 台。
	MaxSlices = 10

	DefaultForks    int32 = 10
	MaxForks        int32 = 100
	DefaultTimeoutS int32 = 3600
	MinTimeoutS     int32 = 60
	MaxTimeoutS     int32 = 86400
	DefaultPort     int32 = 22
	MaxVerbosity    int32 = 3
)

// Target 目标主机（job_template.target）：从 CMDB 的一个资源类型里选主机。
type Target struct {
	// ModelCode CMDB 资源类型码。
	ModelCode string `json:"model_code"`
	// Mode 选择方式（TargetMode*）。
	Mode int32 `json:"mode"`
	// CIIDs 指定的资源 ID（Mode = TargetModeCIs）。
	CIIDs []string `json:"ci_ids,omitempty"`
	// Env / Tag / Keyword 筛选条件（Mode = TargetModeFilter）；全空 = 该类型的全部资源。
	Env     string `json:"env,omitempty"`
	Tag     string `json:"tag,omitempty"`
	Keyword string `json:"keyword,omitempty"`
	// AddressField 存放连接地址（IP 或主机名）的属性码；空 = 用资源名称。
	AddressField string `json:"address_field,omitempty"`
}

// 执行参数的数据类型（Param.Type）。
const (
	ParamString = "string"
	ParamText   = "text"
	ParamInt    = "int"
	ParamFloat  = "float"
	ParamBool   = "bool"
	ParamEnum   = "enum"
)

// ValidParamType 报告参数类型是否受支持。
func ValidParamType(t string) bool {
	switch t {
	case ParamString, ParamText, ParamInt, ParamFloat, ParamBool, ParamEnum:
		return true
	}
	return false
}

// Param 一个执行参数的定义（job_template.params 的元素）：执行时填写，以同名变量传给 Playbook。
type Param struct {
	// Code 变量名（见 ValidVarName）。
	Code string `json:"code"`
	// Name 显示名。
	Name string `json:"name"`
	// Type 数据类型（Param*）。
	Type string `json:"type"`
	// Required 是否必填。
	Required bool `json:"required,omitempty"`
	// Default 缺省值的文本形式（bool 为 true / false）；空 = 无缺省值。
	Default string `json:"default,omitempty"`
	// Options 可选值（Type = ParamEnum）。
	Options []string `json:"options,omitempty"`
	// Description 填写说明。
	Description string `json:"description,omitempty"`
}

// Options 执行选项（job_template.options，提交时固化进 Spec）。
type Options struct {
	// Forks 并发主机数，1-100。
	Forks int32 `json:"forks"`
	// TimeoutS 执行超时秒数，60-86400。
	TimeoutS int32 `json:"timeout_s"`
	// Port SSH 端口。
	Port int32 `json:"port"`
	// Become 是否提权执行（提权方式与密码取自凭据）。
	Become bool `json:"become,omitempty"`
	// Check 预检模式：只报告将发生的变更，不实际修改（--check）。
	Check bool `json:"check,omitempty"`
	// Diff 输出文件变更的差异（--diff）。
	Diff bool `json:"diff,omitempty"`
	// Verbosity 输出详细程度，0-3（-v 的个数）。
	Verbosity int32 `json:"verbosity,omitempty"`
	// BatchSize 滚动执行的每批主机数；0 = 不分批，全部主机一次执行。分批由执行节点实现：
	// 按主机序号切成若干批依次执行，与 Playbook 自身的 serial 无关。
	BatchSize int32 `json:"batch_size,omitempty"`
	// MaxFailPercent 滚动执行的失败阈值（0-100）：一批结束后，已执行主机中失败与不可达的占比
	// 超过它即中止，剩余批次不再执行（主机保持「未执行」）。0 = 出现任何失败即中止。仅分批时生效。
	MaxFailPercent int32 `json:"max_fail_percent,omitempty"`
}

// Normalize 把未设置的选项补为缺省值；越界返回错误说明。
func (o *Options) Normalize() error {
	if o.Forks == 0 {
		o.Forks = DefaultForks
	}
	if o.TimeoutS == 0 {
		o.TimeoutS = DefaultTimeoutS
	}
	if o.Port == 0 {
		o.Port = DefaultPort
	}
	switch {
	case o.Forks < 1 || o.Forks > MaxForks:
		return errors.New("forks must be between 1 and 100")
	case o.TimeoutS < MinTimeoutS || o.TimeoutS > MaxTimeoutS:
		return errors.New("timeout must be between 60 and 86400 seconds")
	case o.Port < 1 || o.Port > 65535:
		return errors.New("port must be between 1 and 65535")
	case o.Verbosity < 0 || o.Verbosity > MaxVerbosity:
		return errors.New("verbosity must be between 0 and 3")
	case o.BatchSize < 0 || o.BatchSize > MaxHosts:
		return errors.New("batch size must be between 0 and 500")
	case o.MaxFailPercent < 0 || o.MaxFailPercent > 100:
		return errors.New("max fail percent must be between 0 and 100")
	}
	if o.BatchSize == 0 {
		o.MaxFailPercent = 0
	}
	return nil
}

// Spec 提交时固化的执行内容（job_execution.spec）。凭据与仓库的密文不在其中：引擎派发时按 ID 读取。
type Spec struct {
	// Kind 作业类型（Kind*）；0 视为 KindPlaybook（一期落库的记录没有这个字段）。
	Kind int32 `json:"kind,omitempty"`
	// Adhoc 快速执行的内容（Kind 为命令 / 脚本 / 文件时）；作业模板为 nil。
	Adhoc *Adhoc `json:"adhoc,omitempty"`
	// Target 快速执行提交时的目标主机配置（仅展示与「再次执行」用；主机快照另存）。
	Target *Target `json:"target,omitempty"`

	// 以下四项仅作业模板（KindPlaybook）使用。
	ProjectID    string `json:"project_id"`
	ProjectName  string `json:"project_name"`
	RepoURL      string `json:"repo_url"`
	Ref          string `json:"ref"`
	Playbook     string `json:"playbook"`
	CredentialID string `json:"credential_id"`
	// CredentialName 提交时的凭据名称（仅展示）。
	CredentialName string `json:"credential_name"`
	// ExtraVars 校验并归一后的参数取值（变量名 → 值）。
	ExtraVars map[string]any `json:"extra_vars"`
	Options   Options        `json:"options"`
}

// JobKind 作业类型（把一期记录的 0 归一为 KindPlaybook）。
func (s Spec) JobKind() int32 {
	if s.Kind == 0 {
		return KindPlaybook
	}
	return s.Kind
}

// Adhoc 快速执行的内容。文件内容不在其中：它单独存放（job_execution_payload），由引擎随任务下发。
type Adhoc struct {
	// Command Shell 命令（KindCommand），至多 MaxCommandLen 字节。
	Command string `json:"command,omitempty"`

	// Script 脚本内容（KindScript），至多 MaxScriptBytes 字节。
	Script string `json:"script,omitempty"`
	// Interpreter 脚本解释器（Interpreter*）。
	Interpreter string `json:"interpreter,omitempty"`
	// ScriptArgs 脚本参数（按 Shell 规则分词），至多 MaxScriptArgsLen 字节。
	ScriptArgs string `json:"script_args,omitempty"`
	// ScriptID / ScriptName / ScriptVersion 取自脚本库时的出处（仅展示）；临时输入的脚本为空。
	ScriptID      string `json:"script_id,omitempty"`
	ScriptName    string `json:"script_name,omitempty"`
	ScriptVersion int32  `json:"script_version,omitempty"`

	// FileName 分发的文件名（KindFile；不含路径），见 ValidFileName。
	FileName string `json:"file_name,omitempty"`
	// FileSize / FileSHA256 文件大小与内容摘要（十六进制）：审批与审计据此确认分发的是哪一份内容。
	FileSize   int64  `json:"file_size,omitempty"`
	FileSHA256 string `json:"file_sha256,omitempty"`
	// Dest 目标主机上的存放目录（绝对路径），见 ValidDestDir；文件落在 Dest/FileName。
	Dest string `json:"dest,omitempty"`
	// Mode 文件权限（八进制，如 0644），见 ValidFileMode。
	Mode string `json:"mode,omitempty"`
}

// Validate 按作业类型校验快速执行的内容（形状与长度；高危规则另行判定）。
func (a *Adhoc) Validate(kind int32) error {
	switch kind {
	case KindCommand:
		if strings.TrimSpace(a.Command) == "" || len(a.Command) > MaxCommandLen || strings.ContainsRune(a.Command, 0) {
			return errors.New("command must be 1-4096 bytes")
		}
	case KindScript:
		switch {
		case strings.TrimSpace(a.Script) == "" || len(a.Script) > MaxScriptBytes || strings.ContainsRune(a.Script, 0):
			return errors.New("script must be 1 byte to 256KB")
		case !ValidInterpreter(a.Interpreter):
			return errors.New("unknown script interpreter")
		case len(a.ScriptArgs) > MaxScriptArgsLen || strings.ContainsAny(a.ScriptArgs, "\r\n\x00"):
			return errors.New("script args must be a single line of at most 1024 bytes")
		}
	case KindFile:
		switch {
		case !ValidFileName(a.FileName):
			return errors.New("invalid file name")
		case !ValidDestDir(a.Dest):
			return errors.New("destination must be an absolute directory path without .. segments")
		case !ValidFileMode(a.Mode):
			return errors.New("file mode must be octal such as 0644")
		case a.FileSize < 0 || a.FileSize > MaxFileBytes:
			return errors.New("file is larger than 4MB")
		}
	default:
		return errors.New("not a quick-execution kind")
	}
	return nil
}

// ---------------------------------------------------------------------------
// 校验（core 写入时、引擎派发前、执行节点使用前都按同一口径判定）
// ---------------------------------------------------------------------------

var (
	hostnamePattern = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9._-]{0,251}[A-Za-z0-9])?$`)
	playbookPattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]{0,250}\.ya?ml$`)
	refPattern      = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._/-]{0,127}$`)
	varNamePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	scpRepoPattern  = regexp.MustCompile(`^[A-Za-z0-9._-]+@[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?:[A-Za-z0-9._~/-]+$`)
	zonePattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	fileNamePattern = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._+-]{0,127}$`)
	destDirPattern  = regexp.MustCompile(`^/[A-Za-z0-9._/+-]{0,254}$`)
	fileModePattern = regexp.MustCompile(`^0[0-7]{3}$`)
)

// ValidFileName 报告分发的文件名是否合法：不含路径分隔符与空白，不以点或连字符开头。
func ValidFileName(name string) bool { return fileNamePattern.MatchString(name) }

// ValidDestDir 报告目标目录是否合法：绝对路径，只含字母、数字与 . _ / + -，不含 .. 段。
func ValidDestDir(dir string) bool {
	if !destDirPattern.MatchString(dir) {
		return false
	}
	for seg := range strings.SplitSeq(dir, "/") {
		if seg == ".." || seg == "." {
			return false
		}
	}
	return true
}

// ValidFileMode 报告文件权限是否合法（0 开头的三位八进制，不含 setuid / setgid / sticky 位）。
func ValidFileMode(mode string) bool { return fileModePattern.MatchString(mode) }

// NormalizeAddress 校验并规范化主机连接地址：IP（v4 / v6，规范化文本）或主机名。
// 不合法返回 ok = false——地址会进入 Ansible 清单与 SSH 命令行，不接受空白、引号与以 - 开头的值。
func NormalizeAddress(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || len(s) > 253 {
		return "", false
	}
	if ip, err := netip.ParseAddr(s); err == nil {
		if ip.Zone() != "" {
			return "", false
		}
		return ip.String(), true
	}
	if !hostnamePattern.MatchString(s) {
		return "", false
	}
	s = strings.ToLower(s)
	// Ansible 清单里的保留名：all / ungrouped 是组名（出现在 --limit 里会选中全部主机），
	// localhost 会被当成本机。
	if s == "all" || s == "ungrouped" || s == "localhost" {
		return "", false
	}
	return s, true
}

// ValidPlaybook 报告 Playbook 路径是否合法：仓库内的相对路径，.yml / .yaml 结尾，不含 .. 段。
func ValidPlaybook(p string) bool {
	if !playbookPattern.MatchString(p) || strings.Contains(p, "//") {
		return false
	}
	for seg := range strings.SplitSeq(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// ValidRef 报告分支 / 标签名是否合法（不以 - 开头、不含 .. 与空白）。
func ValidRef(ref string) bool {
	return refPattern.MatchString(ref) && !strings.Contains(ref, "..") && !strings.HasSuffix(ref, "/") && !strings.HasSuffix(ref, ".lock")
}

// ValidZone 报告区域名是否合法（空串 = 不分区域，亦合法）。
func ValidZone(z string) bool { return z == "" || zonePattern.MatchString(z) }

// reservedVars 不允许作为执行参数名的变量。用了它们 Ansible 只给一条告警、变量照样生效，但含义容易混淆
// （如 strategy / serial / port / timeout 同时是 Play 或任务的关键字），所以在保存模板时就拒绝。
//
// 前一段是 Ansible 的保留名：Play / Role / Block / Task 的全部关键字与模板内置全局函数，取自
// ansible-core 2.22 的 ansible.vars.reserved.get_reserved_names()（只列出符合参数名格式的）；
// 后一段是 Ansible 注入的魔法变量与循环变量（ansible_ 开头的另由前缀拦截）。
var reservedVars = map[string]struct{}{
	"action": {}, "always": {}, "any_errors_fatal": {}, "args": {}, "async": {}, "async_val": {}, "become": {},
	"become_exe": {}, "become_flags": {}, "become_method": {}, "become_user": {}, "block": {}, "changed_when": {},
	"check_mode": {}, "collections": {}, "connection": {}, "cycler": {}, "debugger": {}, "delay": {},
	"delegate_facts": {}, "delegate_to": {}, "dict": {}, "diff": {}, "environment": {}, "fact_path": {},
	"failed_when": {}, "force_handlers": {}, "gather_facts": {}, "gather_timeout": {}, "handlers": {}, "hosts": {},
	"ignore_errors": {}, "ignore_unreachable": {}, "joiner": {}, "lipsum": {}, "local_action": {}, "lookup": {},
	"loop": {}, "loop_control": {}, "loop_with": {}, "max_fail_percentage": {}, "module_defaults": {}, "name": {},
	"namespace": {}, "no_log": {}, "notify": {}, "now": {}, "omit": {}, "order": {}, "poll": {}, "port": {},
	"post_tasks": {}, "pre_tasks": {}, "q": {}, "query": {}, "range": {}, "register": {}, "remote_user": {},
	"rescue": {}, "retries": {}, "roles": {}, "run_once": {}, "serial": {}, "strategy": {}, "tags": {}, "tasks": {},
	"throttle": {}, "timeout": {}, "undef": {}, "until": {}, "validate_argspec": {}, "vars": {}, "vars_files": {},
	"vars_prompt": {}, "when": {}, "with_": {},

	"group_names": {}, "groups": {}, "hostvars": {}, "inventory_dir": {}, "inventory_file": {},
	"inventory_hostname": {}, "inventory_hostname_short": {}, "play_hosts": {}, "playbook_dir": {}, "role_name": {},
	"role_names": {}, "role_path": {}, "role_uuid": {}, "item": {},
}

// ValidVarName 报告执行参数的变量名是否合法：小写字母开头，只含小写字母、数字、下划线，至多 64 字符；
// 不得以 ansible_ / cmdb_ 开头（连接变量与平台注入的主机变量），不得是 Ansible 的保留变量名。
func ValidVarName(name string) bool {
	if !varNamePattern.MatchString(name) || strings.HasPrefix(name, "ansible_") || strings.HasPrefix(name, "cmdb_") {
		return false
	}
	_, reserved := reservedVars[name]
	return !reserved
}

// RepoScheme 仓库地址的传输方式。
type RepoScheme int

// 仓库地址的传输方式。
const (
	RepoInvalid RepoScheme = iota
	RepoHTTP               // http:// 或 https://
	RepoSSH                // ssh:// 或 user@host:path
)

// ParseRepoURL 校验仓库地址并返回其传输方式。只接受 http(s) 与 ssh：不接受本地路径、file://、
// git:// 与 ext:: 等传输助手；http(s) 地址不得内嵌凭据、query 或 fragment（凭据走 secret 字段）。
func ParseRepoURL(raw string) RepoScheme {
	if raw == "" || len(raw) > 512 || strings.ContainsAny(raw, " \t\r\n\"'`\\") || strings.HasPrefix(raw, "-") {
		return RepoInvalid
	}
	if scpRepoPattern.MatchString(raw) {
		return RepoSSH
	}
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.Path == "" || u.Path == "/" || u.RawQuery != "" || u.Fragment != "" {
		return RepoInvalid
	}
	switch u.Scheme {
	case "http", "https":
		if u.User != nil {
			return RepoInvalid
		}
		return RepoHTTP
	case "ssh":
		if _, hasPassword := u.User.Password(); hasPassword {
			return RepoInvalid
		}
		return RepoSSH
	}
	return RepoInvalid
}

// ---------------------------------------------------------------------------
// 终态判定（引擎写库与界面展示同一口径）
// ---------------------------------------------------------------------------

// HostResult 一台主机的执行统计（取自 PLAY RECAP）。
type HostResult struct {
	Address     string `json:"address"`
	OK          int32  `json:"ok"`
	Changed     int32  `json:"changed"`
	Failed      int32  `json:"failed"`
	Unreachable int32  `json:"unreachable"`
	Skipped     int32  `json:"skipped"`
}

// Status 由统计推出主机结果：失败 > 不可达 > 有变更 > 成功 > 已跳过。
func (r HostResult) Status() int32 {
	switch {
	case r.Failed > 0:
		return HostFailed
	case r.Unreachable > 0:
		return HostUnreachable
	case r.Changed > 0:
		return HostChanged
	case r.OK > 0:
		return HostOK
	}
	return HostSkipped
}

// FinalStatus 由结束方式、退出码与各主机结果推出执行的终态。
func FinalStatus(outcome string, exitCode int32, hosts []HostResult) int32 {
	switch outcome {
	case OutcomeCanceled:
		return StatusCanceled
	case OutcomeTimeout:
		return StatusTimeout
	case OutcomeExited:
	default:
		return StatusFailed
	}
	good, bad := 0, 0
	for _, h := range hosts {
		switch h.Status() {
		case HostFailed, HostUnreachable:
			bad++
		case HostOK, HostChanged:
			good++
		}
	}
	switch {
	case exitCode == 0 && bad == 0:
		return StatusSuccess
	case good > 0 && bad > 0:
		return StatusPartial
	}
	return StatusFailed
}

// ---------------------------------------------------------------------------
// 执行节点接入令牌：<节点 ID>.<随机密钥>；库里只存密钥的 SHA-256
// ---------------------------------------------------------------------------

// NewRunnerToken 为节点生成新的接入令牌，返回令牌明文与应落库的摘要。
func NewRunnerToken(runnerID string) (token string, hash []byte, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, err
	}
	secret := base64.RawURLEncoding.EncodeToString(buf)
	return runnerID + "." + secret, HashRunnerSecret(secret), nil
}

// ParseRunnerToken 拆出令牌里的节点 ID 与密钥；格式不对返回 ok = false。
func ParseRunnerToken(token string) (runnerID, secret string, ok bool) {
	runnerID, secret, ok = strings.Cut(token, ".")
	if !ok || len(runnerID) != 36 || len(secret) < 32 || len(secret) > 128 {
		return "", "", false
	}
	return runnerID, secret, true
}

// HashRunnerSecret 令牌密钥的摘要（job_runner.token_hash）。
func HashRunnerSecret(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// RunnerSecretMatches 常数时间比较密钥与落库摘要。
func RunnerSecretMatches(secret string, hash []byte) bool {
	return subtle.ConstantTimeCompare(HashRunnerSecret(secret), hash) == 1
}

// ---------------------------------------------------------------------------
// 引擎 ↔ 执行节点协议（HTTP + JSON，全部 POST；Authorization: Bearer <接入令牌>）
//
// 状态码：200 成功；401 令牌无效；403 节点已停用（不再领取，在途任务的上报仍被接受）；
// 409 租约已失效（领取令牌不匹配或执行已结束）——节点须终止该作业并丢弃；其余按可重试处理。
// ---------------------------------------------------------------------------

// 协议路径。
const (
	RunnerPathHeartbeat = "/runner/v1/heartbeat"
	RunnerPathClaim     = "/runner/v1/claim"
	RunnerPathReport    = "/runner/v1/report"
	RunnerPathFinish    = "/runner/v1/finish"
)

// HeartbeatRequest 节点心跳（建议每 10 秒一次）。
type HeartbeatRequest struct {
	AgentVersion string `json:"agent_version"`
	Hostname     string `json:"hostname"`
	// Capacity 可同时执行的作业数；Running 正在执行的作业数。
	Capacity int32 `json:"capacity"`
	Running  int32 `json:"running"`
}

// HeartbeatResponse 心跳应答。
type HeartbeatResponse struct {
	// Enabled 节点是否启用；停用时不要再领取。
	Enabled bool `json:"enabled"`
}

// ClaimResponse 领取应答；没有可执行的任务时 Task 为 nil。
type ClaimResponse struct {
	Task *Task `json:"task,omitempty"`
}

// Task 派发给节点的一次执行。含凭据明文：只存在于内存与传输中，节点不得落盘（提权密码所在的
// 变量文件除外，见执行节点实现）、不得写入日志。
type Task struct {
	ExecutionID string `json:"execution_id"`
	// LeaseToken 领取令牌：上报与结束时原样带回。
	LeaseToken string `json:"lease_token"`
	// LeaseSeconds 租约秒数：节点须在此间隔内至少上报一次（建议每 LeaseSeconds / 4 秒）。
	LeaseSeconds int32 `json:"lease_seconds"`

	// Kind 作业类型（Kind*）；0 视为 KindPlaybook。
	Kind int32 `json:"kind,omitempty"`
	// Adhoc 快速执行的内容（Kind 为命令 / 脚本 / 文件时）。
	Adhoc *Adhoc `json:"adhoc,omitempty"`
	// FileContent 分发文件的内容（KindFile；JSON 里为 base64）。节点须核对其长度与 Adhoc.FileSHA256。
	FileContent []byte `json:"file_content,omitempty"`

	// 以下四项仅 KindPlaybook 使用；快速执行不拉取代码。
	RepoURL  string  `json:"repo_url"`
	Ref      string  `json:"ref"`
	Git      GitAuth `json:"git"`
	Playbook string  `json:"playbook"`

	Hosts      []TaskHost     `json:"hosts"`
	Credential TaskCredential `json:"credential"`
	ExtraVars  map[string]any `json:"extra_vars"`
	Options    Options        `json:"options"`
}

// GitAuth 拉取仓库的认证信息。
type GitAuth struct {
	// Type 认证方式（GitAuth*）。
	Type     int32  `json:"type"`
	Username string `json:"username,omitempty"`
	// Secret 令牌或私钥明文。
	Secret string `json:"secret,omitempty"`
}

// TaskHost 一台目标主机。
type TaskHost struct {
	Address string `json:"address"`
	Port    int32  `json:"port"`
	// Vars 平台注入的主机变量（cmdb_id / cmdb_name / cmdb_env）。
	Vars map[string]string `json:"vars,omitempty"`
}

// TaskCredential 登录目标主机的凭据。
type TaskCredential struct {
	// Type 凭据类型（Credential*）。
	Type     int32  `json:"type"`
	Username string `json:"username"`
	// Secret 私钥或密码明文。
	Secret       string `json:"secret"`
	BecomeMethod string `json:"become_method,omitempty"`
	BecomeUser   string `json:"become_user,omitempty"`
	BecomeSecret string `json:"become_secret,omitempty"`
}

// LogChunk 一块日志。
type LogChunk struct {
	// Seq 块序号，从 1 开始连续递增。
	Seq     int64  `json:"seq"`
	Content string `json:"content"`
}

// ReportRequest 执行中的周期上报：续租 + 日志 + 提交号。没有新日志也要按时上报以续租。
type ReportRequest struct {
	ExecutionID string `json:"execution_id"`
	LeaseToken  string `json:"lease_token"`
	// CommitID 拉取代码后得到的提交号；未知时留空。
	CommitID string     `json:"commit_id,omitempty"`
	Chunks   []LogChunk `json:"chunks,omitempty"`
}

// ReportResponse 上报应答。
type ReportResponse struct {
	// Cancel 已请求取消：节点应终止作业并以 OutcomeCanceled 结束。
	Cancel bool `json:"cancel"`
	// AckSeq 引擎已收到的最大日志序号：节点可丢弃不大于它的块，其后的块须重传。
	AckSeq int64 `json:"ack_seq"`
}

// FinishRequest 执行结束。须在全部日志被确认之后发送；重复发送是空操作。
type FinishRequest struct {
	ExecutionID string `json:"execution_id"`
	LeaseToken  string `json:"lease_token"`
	// Outcome 结束方式（Outcome*）。
	Outcome  string `json:"outcome"`
	ExitCode int32  `json:"exit_code"`
	// Error 失败原因（不含凭据），至多 MaxErrorLen 字符。
	Error    string       `json:"error,omitempty"`
	CommitID string       `json:"commit_id,omitempty"`
	Hosts    []HostResult `json:"hosts,omitempty"`
}

// ---------------------------------------------------------------------------
// 二期 / 三期：高危规则、定时作业、触发器（表结构见 core/migrations/0075_job_phase2.up.sql）
// ---------------------------------------------------------------------------

// 高危规则命中后的动作（job_risk_rule.action / RiskHit.Action）。
const (
	RiskBlock   int32 = 1 // 拦截：不允许提交
	RiskApprove int32 = 2 // 须审批
	RiskWarn    int32 = 3 // 仅提醒
)

// RiskHit 提交时命中的一条高危规则（job_execution.risk_hits 的元素）。
type RiskHit struct {
	RuleID string `json:"rule_id"`
	Name   string `json:"name"`
	// Action 动作（Risk*）。
	Action      int32  `json:"action"`
	Description string `json:"description,omitempty"`
	// Match 命中的片段（至多 120 字符），便于执行人与审批人定位。
	Match string `json:"match,omitempty"`
}

// 定时作业错过触发时间后的处理（job_schedule.misfire）。
const (
	MisfireSkip    int32 = 1 // 跳过，等下一个触发时间
	MisfireRunOnce int32 = 2 // 补跑一次
)

// 触发器类型（job_trigger.type）。
const (
	TriggerTypeAPI   int32 = 1 // 开放接口：流程节点 / 外部系统调用
	TriggerTypeAlert int32 = 2 // 告警：告警触发时执行（告警自愈）
)

// 触发结果（job_trigger_fire.result）。
const (
	FireSubmitted int32 = 1 // 已提交执行
	FireSkipped   int32 = 2 // 已跳过：冷却中、没有匹配的主机等
	FireFailed    int32 = 3 // 失败：提交被拒（参数不合法、模板停用、越权等）
)

// 流程信号状态（job_trigger_fire.signal_state）。
const (
	SignalNone    int32 = 0 // 无需发送
	SignalPending int32 = 1 // 待发送（作业尚未结束，或发送失败待重试）
	SignalSent    int32 = 2 // 已发送
	SignalGaveUp  int32 = 3 // 已放弃（流程实例已结束，或重试次数用尽）
)

// AlertMatch 告警触发器的匹配条件（job_trigger.alert）。各项之间是「并且」；空项不限。
type AlertMatch struct {
	// RuleIDs 告警规则 ID；空 = 任意规则（此时 Labels 必填，避免「任何告警都触发」）。
	RuleIDs []string `json:"rule_ids,omitempty"`
	// Severities 告警级别（1-4）；空 = 任意级别。
	Severities []int32 `json:"severities,omitempty"`
	// Labels 告警标签的等值条件。
	Labels map[string]string `json:"labels,omitempty"`
	// HostLabel 存放主机地址的告警标签名（如 instance）；取值去掉 :端口 后与模板目标主机的连接地址
	// 比对，只在匹配到的那台主机上执行。空 = 在模板的全部目标主机上执行。
	HostLabel string `json:"host_label,omitempty"`
}

// EventExecutionFinished 执行结束的通知事件类型（notify_event.event_type）。
const EventExecutionFinished = "job.execution.finished"

// ApprovalBusinessType 作业审批在流程引擎里的业务类型（workflow_instance.business_type）；
// business_id 为执行 ID（切片时为分组 ID）。
const ApprovalBusinessType = "job_execution"

// 开放触发接口（流程服务节点 / 外部系统调用）。
const (
	// TriggerTokenPrefix 触发器调用令牌的前缀。
	TriggerTokenPrefix = "jbt_"
	// SignatureSkew 签名时间戳允许的偏差秒数。
	SignatureSkew = 300
)

// NewTriggerToken 生成触发器的调用令牌，返回明文与应落库的摘要。
func NewTriggerToken() (token string, hash []byte, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", nil, err
	}
	token = TriggerTokenPrefix + base64.RawURLEncoding.EncodeToString(buf)
	return token, HashRunnerSecret(token), nil
}
