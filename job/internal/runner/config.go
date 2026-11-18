// Package runner 实现执行节点：向引擎心跳、领取任务，在私有工作目录里拉取代码并运行
// ansible-playbook，把输出（凭据已掩码）分块上报，结束时上报各主机结果。
//
// 执行节点不连接数据库、不持有主密钥，可以部署在隔离的网络区域；它与引擎之间只有
// kit/jobcontract 定义的 HTTP 协议。本包禁止 import data 层与任何数据库驱动（depguard 构建期强制）。
//
// 安全模型：
//   - 任务里的凭据明文只在内存与子进程之间传递：SSH 私钥经标准输入送进每次执行独占的 ssh-agent，
//     不落盘；SSH 密码与提权密码写入作业目录下 0600 的变量文件（Ansible 没有不落盘的传法），
//     随作业目录一并删除——工作目录应挂载为 tmpfs；Git 令牌经环境变量里的 git 配置传递，
//     不进命令行、不进仓库地址。
//   - 子进程的环境从零构造，不继承节点进程的环境（其中有接入令牌）。
//   - 来自任务的字符串（参数取值、CMDB 名称等）一律以 !unsafe 标注写入变量文件与清单：
//     Ansible 不会把它们当模板求值（否则含 {{ }} 的取值可以在节点上执行任意查找插件）。
//   - 平台自己写 ansible.cfg 并以 ANSIBLE_CONFIG 指定：仓库里的 ansible.cfg 不生效。
//   - 输出在进入缓冲之前掩码：私钥的每一行、密码、提权密码、Git 令牌都替换为 ********。
//
// 可靠性：作业一旦开始就不因引擎不可达而放弃——日志块留在有界缓冲里（满了对子进程施加背压，
// 不丢弃），从引擎确认的序号之后重传；只有引擎明确答复租约失效（409）才终止并丢弃作业。
package runner

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/job/internal/conf"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"

	"google.golang.org/protobuf/types/known/durationpb"
)

// conf.proto 注明的缺省值。
const (
	defaultCapacity          = 2
	defaultPollInterval      = 2 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
	defaultCancelGrace       = 30 * time.Second
	defaultDrainTimeout      = 10 * time.Minute
	defaultGitProtocols      = "http:https:ssh"
	// defaultReportEvery 作业没有新日志时的上报间隔上限：决定取消请求最迟多久被节点得知。
	defaultReportEvery = 3 * time.Second
	// maxSocketPath UNIX 套接字路径的保守上限（Linux 108、macOS 104 字节，含结尾 NUL）。
	maxSocketPath = 100
	// socketSuffixLen 工作目录之下套接字路径的最长后缀，取两者中较长的：
	//   - ssh-agent：/<36 字符执行 ID>/agent.sock
	//   - ssh 连接复用：/<13 字符控制目录>/<10 字符套接字名 + ssh 建立时的 17 字符临时后缀>
	socketSuffixLen = 1 + 36 + 1 + len("agent.sock")
)

// AgentVersion 节点程序版本（main.Version），随心跳上报。
type AgentVersion string

// Config 执行节点的运行参数（已补缺省值并校验）。
type Config struct {
	EngineURL          string
	Token              string
	Capacity           int
	WorkDir            string
	AnsiblePlaybook    string
	Git                string
	SSHAgent           string
	SSHAdd             string
	KnownHosts         string
	CAFile             string
	InsecureSkipVerify bool
	PollInterval       time.Duration
	HeartbeatInterval  time.Duration
	CancelGrace        time.Duration
	DrainTimeout       time.Duration
	GitAllowProtocols  string
	Version            string
	Hostname           string
	// reportEvery 作业没有新日志时的上报间隔上限（另受 lease/4 约束）。固定值，测试可调小。
	reportEvery time.Duration
}

func dur(d *durationpb.Duration, def time.Duration) time.Duration {
	if d == nil || d.AsDuration() <= 0 {
		return def
	}
	return d.AsDuration()
}

func str(v, def string) string {
	if v = strings.TrimSpace(v); v == "" {
		return def
	}
	return v
}

// devVersion 没有任何版本信息时上报的版本。
const devVersion = "dev"

// buildVersion 未经 ldflags 注入版本号（-X main.Version=…）时的版本：取构建信息里的模块版本，
// 没有（本地 go build）则为 dev——心跳上报的 agent_version 不留空。
func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return devVersion
	}
	return fallbackVersion(info.Main.Version)
}

// fallbackVersion 把构建信息里的模块版本归一：空与 (devel) 视为 dev。
func fallbackVersion(v string) string {
	if v == "" || v == "(devel)" {
		return devVersion
	}
	return v
}

// NewConfig 由配置构造并校验；错误信息不含接入令牌。
func NewConfig(c *conf.Runner, version AgentVersion) (*Config, error) {
	cfg := &Config{
		EngineURL:          strings.TrimRight(strings.TrimSpace(c.GetEngineUrl()), "/"),
		Token:              strings.TrimSpace(c.GetToken()),
		Capacity:           int(c.GetCapacity()),
		WorkDir:            strings.TrimSpace(c.GetWorkDir()),
		AnsiblePlaybook:    str(c.GetAnsiblePlaybook(), "ansible-playbook"),
		Git:                str(c.GetGit(), "git"),
		SSHAgent:           str(c.GetSshAgent(), "ssh-agent"),
		SSHAdd:             str(c.GetSshAdd(), "ssh-add"),
		KnownHosts:         strings.TrimSpace(c.GetKnownHosts()),
		CAFile:             strings.TrimSpace(c.GetCaFile()),
		InsecureSkipVerify: c.GetInsecureSkipVerify(),
		PollInterval:       dur(c.GetPollInterval(), defaultPollInterval),
		HeartbeatInterval:  dur(c.GetHeartbeatInterval(), defaultHeartbeatInterval),
		CancelGrace:        dur(c.GetCancelGrace(), defaultCancelGrace),
		DrainTimeout:       dur(c.GetDrainTimeout(), defaultDrainTimeout),
		GitAllowProtocols:  str(c.GetGitAllowProtocols(), defaultGitProtocols),
		Version:            string(version),
		reportEvery:        defaultReportEvery,
	}
	if cfg.Capacity <= 0 {
		cfg.Capacity = defaultCapacity
	}
	if cfg.Version == "" {
		cfg.Version = buildVersion()
	}
	cfg.Hostname, _ = os.Hostname()

	u, err := url.Parse(cfg.EngineURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("runner.engine_url must be an http(s) URL like https://job-engine.internal:8051")
	}
	if _, _, ok := jobcontract.ParseRunnerToken(cfg.Token); !ok {
		return nil, errors.New("runner.token is missing or malformed (export KRATOS_RUNNER_TOKEN=<runner id>.<secret>)")
	}
	if cfg.WorkDir == "" {
		return nil, errors.New("runner.work_dir is required")
	}
	if cfg.WorkDir, err = filepath.Abs(cfg.WorkDir); err != nil {
		return nil, fmt.Errorf("resolve runner.work_dir: %w", err)
	}
	if cfg.KnownHosts == "" {
		cfg.KnownHosts = filepath.Join(cfg.WorkDir, "known_hosts")
	} else if cfg.KnownHosts, err = filepath.Abs(cfg.KnownHosts); err != nil {
		return nil, fmt.Errorf("resolve runner.known_hosts: %w", err)
	}
	// 这两个路径会进入 ssh 的命令行选项（按空白切分），不接受空白、引号与反斜杠。
	for name, p := range map[string]string{"runner.work_dir": cfg.WorkDir, "runner.known_hosts": cfg.KnownHosts} {
		// 冒号与逗号：滚动批次的主机清单文件经 --limit @<路径> 传给 Ansible，而它按这两个字符切分主机模式。
		if strings.ContainsAny(p, " \t\r\n\"'\\%:,") {
			return nil, fmt.Errorf("%s must not contain whitespace, quotes, backslashes, %%, colons or commas", name)
		}
	}
	if len(cfg.WorkDir)+socketSuffixLen > maxSocketPath {
		return nil, fmt.Errorf("runner.work_dir is too long (%d bytes): unix sockets are created beneath it, keep it under %d bytes",
			len(cfg.WorkDir), maxSocketPath-socketSuffixLen)
	}
	for _, proto := range strings.Split(cfg.GitAllowProtocols, ":") {
		if proto == "" || strings.ContainsAny(proto, " \t\r\n=") {
			return nil, errors.New("runner.git_allow_protocols must be a colon-separated list like http:https:ssh")
		}
	}
	return cfg, nil
}
