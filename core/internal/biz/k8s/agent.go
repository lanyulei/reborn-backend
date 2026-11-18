package biz

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
	"text/template"
)

// agentConfig 生成 Agent 安装物所需的启动配置（conf.k8s.agent）。
type agentConfig struct {
	ServerURL string
	Image     string
}

// check 两项都配置且地址合法才能生成安装物。
func (a agentConfig) check() error {
	if a.ServerURL == "" || a.Image == "" {
		return ErrAgentUnconfigured
	}
	u, err := url.Parse(a.ServerURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return detail(ErrAgentUnconfigured, "k8s.agent.server_url is not a valid http(s) URL")
	}
	if strings.ContainsAny(a.Image, " \t\r\n\"'`$\\") {
		return detail(ErrAgentUnconfigured, "k8s.agent.image contains illegal characters")
	}
	return nil
}

// Agent 读取的环境变量（与 backend/k8s 的 cmd/agent 约定）。
const (
	AgentEnvServerURL     = "REBORN_SERVER_URL"
	AgentEnvRegisterToken = "REBORN_REGISTER_TOKEN"
	AgentEnvKubeconfig    = "REBORN_KUBECONFIG"
	AgentEnvStateDir      = "REBORN_STATE_DIR"
	AgentEnvNamespace     = "REBORN_NAMESPACE"
	// AgentNamespace 集群内部署时 Agent 所在的命名空间。
	AgentNamespace = "reborn-system"
)

// yamlString 把任意字符串渲染为 YAML 双引号标量（JSON 字符串是合法的 YAML 双引号标量）。
func yamlString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// shellQuote POSIX shell 单引号转义。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

var agentFuncs = template.FuncMap{"yaml": yamlString, "sh": shellQuote}

// manifestTemplate 集群内部署的清单。Agent 以 cluster-admin 的服务账号访问 API Server（Impersonation
// 需要）；长期令牌由 Agent 自己存为本命名空间的 Secret，所以还需要对 Secret 的读写——cluster-admin 已涵盖。
// 容器以非 root、只读根文件系统运行，不挂任何宿主机路径。
var manifestTemplate = template.Must(template.New("manifest").Funcs(agentFuncs).Parse(`# reborn k8s agent — cluster {{.Cluster}}
# 应用：kubectl apply -f <本文件>。注册令牌一次性且 24 小时内有效，请勿提交到代码仓库。
apiVersion: v1
kind: Namespace
metadata:
  name: {{.Namespace}}
  labels:
    app.kubernetes.io/managed-by: reborn
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: reborn-agent
  namespace: {{.Namespace}}
  labels:
    app.kubernetes.io/managed-by: reborn
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: reborn:agent
  labels:
    app.kubernetes.io/managed-by: reborn
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: reborn-agent
    namespace: {{.Namespace}}
---
apiVersion: v1
kind: Secret
metadata:
  name: reborn-agent-register
  namespace: {{.Namespace}}
  labels:
    app.kubernetes.io/managed-by: reborn
type: Opaque
stringData:
  token: {{yaml .Token}}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reborn-agent
  namespace: {{.Namespace}}
  labels:
    app.kubernetes.io/name: reborn-agent
    app.kubernetes.io/managed-by: reborn
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: reborn-agent
  template:
    metadata:
      labels:
        app.kubernetes.io/name: reborn-agent
    spec:
      serviceAccountName: reborn-agent
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        runAsGroup: 65532
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: agent
          image: {{yaml .Image}}
          imagePullPolicy: IfNotPresent
          env:
            - name: {{.EnvServerURL}}
              value: {{yaml .ServerURL}}
            - name: {{.EnvRegisterToken}}
              valueFrom:
                secretKeyRef:
                  name: reborn-agent-register
                  key: token
            - name: {{.EnvNamespace}}
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: "1"
              memory: 512Mi
`))

// relayTemplate 转发节点上的启动命令：Agent 用挂载进来的 kubeconfig 访问集群（须为集群管理员权限），
// 长期令牌存在数据卷里（容器重建后不需要重新注册）。
var relayTemplate = template.Must(template.New("relay").Funcs(agentFuncs).Parse(`# reborn k8s agent — cluster {{.Cluster}}（转发节点）
# 先把该集群的 kubeconfig（集群管理员权限）放到 /etc/reborn/{{.Cluster}}.kubeconfig（权限 0600），再执行：
docker run -d --name {{sh (print "reborn-agent-" .Cluster)}} --restart unless-stopped \
  --read-only --cap-drop ALL --security-opt no-new-privileges --user 65532:65532 \
  -v {{sh (print "/etc/reborn/" .Cluster ".kubeconfig:/etc/reborn/kubeconfig:ro")}} \
  -v {{sh (print "reborn-agent-" .Cluster ":/var/lib/reborn-agent")}} \
  -e {{.EnvServerURL}}={{sh .ServerURL}} \
  -e {{.EnvRegisterToken}}={{sh .Token}} \
  -e {{.EnvKubeconfig}}=/etc/reborn/kubeconfig \
  -e {{.EnvStateDir}}=/var/lib/reborn-agent \
  {{sh .Image}}
`))

type agentTemplateData struct {
	Cluster, Namespace, Image, ServerURL, Token                              string
	EnvServerURL, EnvRegisterToken, EnvKubeconfig, EnvStateDir, EnvNamespace string
}

func (a agentConfig) data(cluster, token string) agentTemplateData {
	return agentTemplateData{
		Cluster: cluster, Namespace: AgentNamespace, Image: a.Image, ServerURL: a.ServerURL, Token: token,
		EnvServerURL: AgentEnvServerURL, EnvRegisterToken: AgentEnvRegisterToken, EnvKubeconfig: AgentEnvKubeconfig,
		EnvStateDir: AgentEnvStateDir, EnvNamespace: AgentEnvNamespace,
	}
}

// manifest 渲染集群内部署的清单。cluster 已由名称规则约束为 [a-z0-9-]。
func (a agentConfig) manifest(cluster, token string) (string, error) {
	var buf bytes.Buffer
	if err := manifestTemplate.Execute(&buf, a.data(cluster, token)); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// relayCommand 渲染转发节点上的启动命令。
func (a agentConfig) relayCommand(cluster, token string) (string, error) {
	var buf bytes.Buffer
	if err := relayTemplate.Execute(&buf, a.data(cluster, token)); err != nil {
		return "", err
	}
	return buf.String(), nil
}
