package runner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/lanyulei/reborn/backend/job/internal/pkg/textx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// 作业目录里的文件名。
const (
	repoDirName   = "repo"
	inventoryName = "inventory.json"
	varsName      = "vars.json"
	secretsName   = "secrets.json"
	ansibleCfg    = "ansible.cfg"
	agentSock     = "agent.sock"
	gitAgentSock  = "git.sock"
)

// controlDirPattern 控制目录的名字：c + 执行 ID 的末 12 个十六进制字符。
var controlDirPattern = regexp.MustCompile(`^c[0-9a-f]{12}$`)

// controlDirFor 一次执行的 ssh 连接复用控制目录。它不放在作业目录里，而是工作目录下的一个短名字：
// 控制套接字的路径要再加 27 个字符，UNIX 套接字路径又有 104 / 108 字节的上限，放在
// <工作目录>/<36 字符执行 ID>/ 之下会把工作目录的可用长度压到三十来个字节。
// 名字取执行 ID（UUIDv7）的末 12 个字符——随机部分，同一节点上的在途作业之间不会相同。
func controlDirFor(workDir, executionID string) string {
	if !idgen.IsUUID(executionID) {
		return "" // 调用方（newJob）已拒绝不合法的 ID；这里只保证不因过短的 ID 越界
	}
	return filepath.Join(workDir, "c"+executionID[len(executionID)-12:])
}

// hostVarPattern 平台注入的主机变量名（cmdb_id / cmdb_name / cmdb_env 及今后的同前缀变量）。
var hostVarPattern = regexp.MustCompile(`^cmdb_[a-z0-9_]{1,56}$`)

// validateTask 使用任务之前按契约口径复核（纵深防御：引擎派发前已校验）。任务内容会进入命令行、
// 清单与文件路径，任何一项不合规都拒绝执行。错误信息不含凭据。按作业类型分支：作业模板复核仓库、
// 分支、Playbook 与参数；快速执行复核命令 / 脚本 / 文件的内容（Adhoc.Validate）。
func validateTask(t *jobcontract.Task) error {
	if !idgen.IsUUID(t.ExecutionID) {
		return errors.New("执行 ID 不合法")
	}
	if t.LeaseToken == "" {
		return errors.New("缺少领取令牌")
	}
	kind := taskKind(t)
	scheme := jobcontract.RepoInvalid
	switch {
	case !jobcontract.ValidKind(kind):
		return errors.New("作业类型不合法")
	case kind == jobcontract.KindPlaybook:
		scheme = jobcontract.ParseRepoURL(t.RepoURL)
		switch {
		case scheme == jobcontract.RepoInvalid:
			return errors.New("仓库地址不合法")
		case !jobcontract.ValidRef(t.Ref):
			return errors.New("分支或标签名不合法")
		case !jobcontract.ValidPlaybook(t.Playbook):
			return errors.New("指定的 Playbook 路径不合法")
		case len(t.ExtraVars) > jobcontract.MaxParams:
			return errors.New("参数个数超出上限")
		case t.Adhoc != nil || len(t.FileContent) > 0:
			return errors.New("作业模板不应带有快速执行的内容")
		}
	default:
		if err := validateAdhoc(t, kind); err != nil {
			return err
		}
	}
	switch {
	case len(t.Hosts) == 0:
		return errors.New("没有目标主机")
	case len(t.Hosts) > jobcontract.MaxHosts:
		return errors.New("目标主机数超出上限")
	}
	seen := make(map[string]struct{}, len(t.Hosts))
	for _, h := range t.Hosts {
		addr, ok := jobcontract.NormalizeAddress(h.Address)
		if !ok || addr != h.Address || h.Port < 1 || h.Port > 65535 {
			return errors.New("主机地址或端口不合法")
		}
		if _, dup := seen[addr]; dup {
			return errors.New("主机地址重复")
		}
		seen[addr] = struct{}{}
		for name := range h.Vars {
			if !hostVarPattern.MatchString(name) {
				return errors.New("主机变量名不合法")
			}
		}
	}
	for name := range t.ExtraVars {
		if !jobcontract.ValidVarName(name) {
			return errors.New("参数名不合法")
		}
	}
	opts := t.Options
	if err := opts.Normalize(); err != nil {
		return errors.New("执行选项不合法")
	}
	c := t.Credential
	switch {
	case c.Type != jobcontract.CredentialSSHKey && c.Type != jobcontract.CredentialSSHPassword:
		return errors.New("凭据类型不合法")
	case !textx.ValidLogin(c.Username):
		return errors.New("凭据的登录用户名不合法")
	case c.Secret == "":
		return errors.New("凭据内容为空")
	case !jobcontract.ValidBecomeMethod(c.BecomeMethod):
		return errors.New("提权方式不合法")
	case c.BecomeUser != "" && !textx.ValidLogin(c.BecomeUser):
		return errors.New("提权用户不合法")
	}
	if kind != jobcontract.KindPlaybook {
		return nil
	}
	switch t.Git.Type {
	case jobcontract.GitAuthNone:
	case jobcontract.GitAuthToken:
		if scheme != jobcontract.RepoHTTP || t.Git.Secret == "" || strings.ContainsAny(t.Git.Username, "\r\n") {
			return errors.New("仓库认证信息与仓库地址不匹配")
		}
	case jobcontract.GitAuthSSHKey:
		if scheme != jobcontract.RepoSSH || t.Git.Secret == "" {
			return errors.New("仓库认证信息与仓库地址不匹配")
		}
	default:
		return errors.New("仓库认证方式不合法")
	}
	return nil
}

// sshCommonArgs 连接目标主机与 Git 服务器共用的 ssh 选项：首次连接自动记录主机公钥，
// 之后公钥变化即拒绝连接；公钥记在节点持久化的 known_hosts 里。
func sshCommonArgs(knownHosts string) string {
	return "-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=" + knownHosts
}

// buildInventory 生成 Ansible 清单（YAML 流式写法，见 encodeVars）：all.hosts 下每台主机带连接地址、
// 端口与平台注入的 cmdb_* 变量；all.vars 带登录用户、ssh 选项与提权设置。凭据不在清单里。
func buildInventory(t *jobcontract.Task, knownHosts string) ([]byte, error) {
	hosts := make(map[string]any, len(t.Hosts))
	for _, h := range t.Hosts {
		vars := map[string]any{"ansible_host": plain(h.Address), "ansible_port": h.Port}
		for k, v := range h.Vars {
			vars[k] = v
		}
		hosts[h.Address] = vars
	}
	vars := map[string]any{
		"ansible_user":            t.Credential.Username,
		"ansible_ssh_common_args": plain(sshCommonArgs(knownHosts)),
	}
	if t.Options.Become {
		method := t.Credential.BecomeMethod
		if method == "" {
			method = jobcontract.BecomeSudo
		}
		user := t.Credential.BecomeUser
		if user == "" {
			user = "root"
		}
		vars["ansible_become"] = true
		vars["ansible_become_method"] = plain(method)
		vars["ansible_become_user"] = user
	}
	return encodeVars(map[string]any{"all": map[string]any{"hosts": hosts, "vars": vars}})
}

// buildVars 生成执行参数文件（-e @vars.json）：参数经文件传递，绝不拼进命令行。
func buildVars(t *jobcontract.Task) ([]byte, error) {
	vars := make(map[string]any, len(t.ExtraVars))
	for k, v := range t.ExtraVars {
		vars[k] = v
	}
	return encodeVars(vars)
}

// buildSecrets 生成口令变量文件（-e @secrets.json）：SSH 密码与提权密码。没有口令返回 nil。
// 这是凭据唯一落盘的地方（Ansible 没有不落盘的传法）：文件 0600，随作业目录删除。
func buildSecrets(t *jobcontract.Task) ([]byte, error) {
	secrets := map[string]any{}
	if t.Credential.Type == jobcontract.CredentialSSHPassword {
		secrets["ansible_password"] = t.Credential.Secret
	}
	if t.Options.Become && t.Credential.BecomeSecret != "" {
		secrets["ansible_become_password"] = t.Credential.BecomeSecret
	}
	if len(secrets) == 0 {
		return nil, nil
	}
	return encodeVars(secrets)
}

// buildAnsibleCfg 平台写的 ansible.cfg（经 ANSIBLE_CONFIG 指定，仓库里的 ansible.cfg 因此不生效）：
// 主机公钥校验保持开启、不写重试文件、标准输出用缺省回调（PLAY RECAP 的格式由它决定）、
// Python 解释器自动发现但不逐台主机告警、
// 开启 pipelining 与 ssh 连接复用（控制套接字放在每次执行独占的控制目录里）。
func buildAnsibleCfg(controlDir string) []byte {
	return []byte(strings.Join([]string{
		"[defaults]",
		"host_key_checking = True",
		"retry_files_enabled = False",
		"nocows = True",
		"stdout_callback = default",
		"interpreter_python = auto_silent",
		"",
		"[ssh_connection]",
		"pipelining = True",
		"ssh_args = -o ControlMaster=auto -o ControlPersist=60s",
		"control_path_dir = " + controlDir,
		"",
	}, "\n"))
}

// ansibleArgs ansible-playbook 的参数。清单与变量文件在作业目录（绝对路径）。playbook：作业模板是仓库内的
// 相对路径（工作目录 = 仓库目录，契约保证它不以 - 开头），快速执行是平台生成的文件的绝对路径。
// limitFile 非空时只在其中列出的主机上执行（滚动执行的一批）。
func ansibleArgs(t *jobcontract.Task, jobDir, playbook string, withSecrets bool, limitFile string) []string {
	args := []string{
		"-i", filepath.Join(jobDir, inventoryName),
		playbook,
		"-e", "@" + filepath.Join(jobDir, varsName),
	}
	if withSecrets {
		args = append(args, "-e", "@"+filepath.Join(jobDir, secretsName))
	}
	args = append(args, "--forks", strconv.Itoa(int(t.Options.Forks)))
	if limitFile != "" {
		args = append(args, "--limit", "@"+limitFile)
	}
	if t.Options.Check {
		args = append(args, "--check")
	}
	if t.Options.Diff {
		args = append(args, "--diff")
	}
	if v := t.Options.Verbosity; v > 0 {
		args = append(args, "-"+strings.Repeat("v", int(v)))
	}
	return args
}

// splitBatches 按任务里的主机顺序切成每批 size 台的若干批；size ≤ 0 或不小于主机数时只有一批。
func splitBatches(hosts []jobcontract.TaskHost, size int) [][]jobcontract.TaskHost {
	if size <= 0 || size >= len(hosts) {
		return [][]jobcontract.TaskHost{hosts}
	}
	batches := make([][]jobcontract.TaskHost, 0, (len(hosts)+size-1)/size)
	for start := 0; start < len(hosts); start += size {
		batches = append(batches, hosts[start:min(start+size, len(hosts))])
	}
	return batches
}

// limitFileName 第 n 批（从 1 起）的主机限定文件名。
func limitFileName(n int) string { return "limit-" + strconv.Itoa(n) + ".txt" }

// buildLimit 一批主机的限定文件（--limit @文件）：每行一个清单主机名。地址已按契约校验（IP 或主机名，
// 不含空白、逗号、通配符与方括号），作为主机模式只会精确匹配它自己。
func buildLimit(batch []jobcontract.TaskHost) []byte {
	var b strings.Builder
	for _, h := range batch {
		b.WriteString(h.Address)
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// baseEnv 子进程的最小环境：PATH、HOME（作业目录）与 UTF-8 locale。不继承节点进程的环境——
// 其中有接入令牌。
func baseEnv(jobDir string) []string {
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	// Ansible 要求 UTF-8 locale；节点自身的 locale 是 UTF-8 就沿用（C.UTF-8 并非处处都有）。
	lang := os.Getenv("LANG")
	if up := strings.ToUpper(lang); !strings.HasSuffix(up, "UTF-8") && !strings.HasSuffix(up, "UTF8") {
		lang = "C.UTF-8"
	}
	return []string{"PATH=" + path, "HOME=" + jobDir, "LANG=" + lang, "LC_ALL=" + lang}
}

// ansibleEnv ansible-playbook 的环境。agentSock 非空时带上 SSH_AUTH_SOCK。
func ansibleEnv(jobDir, agentSock string) []string {
	env := append(baseEnv(jobDir),
		"ANSIBLE_CONFIG="+filepath.Join(jobDir, ansibleCfg),
		"ANSIBLE_FORCE_COLOR=0",
		"ANSIBLE_NOCOLOR=1",
		"PYTHONUNBUFFERED=1",
	)
	if agentSock != "" {
		env = append(env, "SSH_AUTH_SOCK="+agentSock)
	}
	return env
}

// gitEnv git 的环境：不交互、只允许配置的传输协议、不读系统与用户级配置。令牌认证经环境变量里的
// git 配置（http.extraHeader）传递——不进命令行、不进仓库地址；并禁止跟随重定向，认证头不会被带到
// 别的主机。私钥认证经 agentSock 指向的 ssh-agent。
func gitEnv(cfg *Config, jobDir string, auth jobcontract.GitAuth, agentSock string) []string {
	env := append(baseEnv(jobDir),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ALLOW_PROTOCOL="+cfg.GitAllowProtocols,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_SSH_COMMAND=ssh "+sshCommonArgs(cfg.KnownHosts)+" -o BatchMode=yes",
	)
	if auth.Type == jobcontract.GitAuthToken {
		env = append(env,
			"GIT_CONFIG_COUNT=2",
			"GIT_CONFIG_KEY_0=http.extraHeader",
			"GIT_CONFIG_VALUE_0=Authorization: Basic "+basicAuth(gitUser(auth), auth.Secret),
			"GIT_CONFIG_KEY_1=http.followRedirects",
			"GIT_CONFIG_VALUE_1=false",
		)
	}
	if agentSock != "" {
		env = append(env, "SSH_AUTH_SOCK="+agentSock)
	}
	return env
}

// cloneArgs git clone 的参数：浅克隆指定分支 / 标签到作业目录下的 repo。`--` 之后才是仓库地址。
func cloneArgs(t *jobcontract.Task) []string {
	return []string{"clone", "--depth", "1", "--single-branch", "--branch", t.Ref, "--", t.RepoURL, repoDirName}
}

// resolvePlaybook 确认 Playbook 是仓库内的普通文件：解析符号链接之后仍须落在仓库目录之内。
func resolvePlaybook(repoDir, playbook string) error {
	root, err := filepath.EvalSymlinks(repoDir)
	if err != nil {
		return fmt.Errorf("resolve repository dir: %w", err)
	}
	target, err := filepath.EvalSymlinks(filepath.Join(root, filepath.FromSlash(playbook)))
	if err != nil {
		return errors.New("仓库中不存在该 Playbook")
	}
	if !strings.HasPrefix(target, root+string(filepath.Separator)) {
		return errors.New("指定的 Playbook 指向仓库之外")
	}
	info, err := os.Stat(target)
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("指定的 Playbook 不是普通文件")
	}
	return nil
}

// writePrivate 以 0600 新建文件（已存在即失败：作业目录是全新的，出现同名文件说明有问题）。
func writePrivate(path string, data []byte) error { return writeFile(path, data, 0o600) }

// writeFile 以给定权限新建文件（已存在即失败）。
func writeFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm) //nolint:gosec // 路径由平台构造（作业目录 + 固定或已校验的文件名）
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
