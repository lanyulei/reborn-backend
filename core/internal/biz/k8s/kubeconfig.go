package biz

import (
	"encoding/base64"
	"strings"

	"gopkg.in/yaml.v3"
)

// Kubeconfig 解析结果：可直接填入直连集群表单的连接信息。
type Kubeconfig struct {
	Contexts   []string
	Context    string
	APIServer  string
	CACert     string
	ProxyURL   string
	Credential CredentialInput
}

// kubeconfigFile kubeconfig 中用到的最小字段集。core 不引入 client-go：这里只读内联的 *-data 字段，
// 任何需要读文件或执行外部命令才能拿到凭据的写法一律拒绝（core 也不该替用户执行命令）。
type kubeconfigFile struct {
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server    string `yaml:"server"`
			CAData    string `yaml:"certificate-authority-data"`
			CAFile    string `yaml:"certificate-authority"`
			ProxyURL  string `yaml:"proxy-url"`
			Insecure  bool   `yaml:"insecure-skip-tls-verify"`
			TLSServer string `yaml:"tls-server-name"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster string `yaml:"cluster"`
			User    string `yaml:"user"`
		} `yaml:"context"`
	} `yaml:"contexts"`
	Users []struct {
		Name string `yaml:"name"`
		User struct {
			Token        string    `yaml:"token"`
			TokenFile    string    `yaml:"tokenFile"`
			CertData     string    `yaml:"client-certificate-data"`
			CertFile     string    `yaml:"client-certificate"`
			KeyData      string    `yaml:"client-key-data"`
			KeyFile      string    `yaml:"client-key"`
			Username     string    `yaml:"username"`
			Password     string    `yaml:"password"`
			Exec         yaml.Node `yaml:"exec"`
			AuthProvider yaml.Node `yaml:"auth-provider"`
		} `yaml:"user"`
	} `yaml:"users"`
}

const useAgentHint = "; use agent access for this cluster instead"

// ParseKubeconfig 解析 kubeconfig。contextName 为空取 current-context。错误说明不含凭据内容。
func ParseKubeconfig(raw, contextName string) (*Kubeconfig, error) {
	var f kubeconfigFile
	if err := yaml.Unmarshal([]byte(raw), &f); err != nil {
		return nil, detail(ErrClusterInvalid, "kubeconfig is not valid YAML")
	}
	if len(f.Contexts) == 0 || len(f.Clusters) == 0 {
		return nil, detail(ErrClusterInvalid, "kubeconfig has no contexts or clusters")
	}
	out := &Kubeconfig{Contexts: make([]string, 0, len(f.Contexts))}
	for _, c := range f.Contexts {
		out.Contexts = append(out.Contexts, c.Name)
	}
	name := strings.TrimSpace(contextName)
	if name == "" {
		name = f.CurrentContext
	}
	if name == "" && len(f.Contexts) == 1 {
		name = f.Contexts[0].Name
	}
	var clusterName, userName string
	found := false
	for _, c := range f.Contexts {
		if c.Name == name {
			clusterName, userName, found = c.Context.Cluster, c.Context.User, true
			break
		}
	}
	if !found {
		return nil, detail(ErrClusterInvalid, "kubeconfig context %q not found; choose one of the listed contexts", name)
	}
	out.Context = name

	found = false
	for _, c := range f.Clusters {
		if c.Name != clusterName {
			continue
		}
		found = true
		switch {
		case c.Cluster.Insecure:
			return nil, detail(ErrClusterInvalid, "kubeconfig disables TLS verification (insecure-skip-tls-verify), which is not supported")
		case c.Cluster.CAFile != "" && c.Cluster.CAData == "":
			return nil, detail(ErrClusterInvalid, "kubeconfig references a CA certificate file; embed it as certificate-authority-data (kubectl config view --raw --flatten)")
		}
		out.APIServer, out.ProxyURL = strings.TrimSpace(c.Cluster.Server), strings.TrimSpace(c.Cluster.ProxyURL)
		if c.Cluster.CAData != "" {
			ca, err := base64.StdEncoding.DecodeString(strings.TrimSpace(c.Cluster.CAData))
			if err != nil {
				return nil, detail(ErrClusterInvalid, "kubeconfig certificate-authority-data is not valid base64")
			}
			out.CACert = string(ca)
		}
	}
	if !found {
		return nil, detail(ErrClusterInvalid, "kubeconfig cluster %q not found", clusterName)
	}

	found = false
	for _, u := range f.Users {
		if u.Name != userName {
			continue
		}
		found = true
		switch {
		case !u.User.Exec.IsZero():
			return nil, detail(ErrClusterInvalid, "kubeconfig obtains credentials by running an external command (exec), which is not supported"+useAgentHint)
		case !u.User.AuthProvider.IsZero():
			return nil, detail(ErrClusterInvalid, "kubeconfig uses an auth provider plugin, which is not supported"+useAgentHint)
		case u.User.TokenFile != "" || (u.User.CertFile != "" && u.User.CertData == "") || (u.User.KeyFile != "" && u.User.KeyData == ""):
			return nil, detail(ErrClusterInvalid, "kubeconfig references credential files; embed them (kubectl config view --raw --flatten)"+useAgentHint)
		case u.User.Username != "" || u.User.Password != "":
			return nil, detail(ErrClusterInvalid, "kubeconfig uses basic authentication, which is not supported"+useAgentHint)
		}
		out.Credential.Token = strings.TrimSpace(u.User.Token)
		if u.User.CertData != "" || u.User.KeyData != "" {
			cert, err1 := base64.StdEncoding.DecodeString(strings.TrimSpace(u.User.CertData))
			key, err2 := base64.StdEncoding.DecodeString(strings.TrimSpace(u.User.KeyData))
			if err1 != nil || err2 != nil {
				return nil, detail(ErrClusterInvalid, "kubeconfig client certificate data is not valid base64")
			}
			out.Credential.ClientCert, out.Credential.ClientKey = string(cert), string(key)
		}
	}
	if !found {
		return nil, detail(ErrClusterInvalid, "kubeconfig user %q not found", userName)
	}
	// 证书优先：同时带令牌与证书的 kubeconfig 以证书为准（直连凭据二选一）。
	if out.Credential.ClientCert != "" && out.Credential.Token != "" {
		out.Credential.Token = ""
	}
	if out.Credential.empty() {
		return nil, detail(ErrClusterInvalid, "kubeconfig user %q has no token or client certificate"+useAgentHint, userName)
	}
	probe := ClusterInput{Name: "probe", AccessMode: 2, APIServer: out.APIServer, ProxyURL: out.ProxyURL, CACert: out.CACert}
	if err := probe.normalize(); err != nil {
		return nil, err
	}
	if _, err := normalizeCredential(&out.Credential); err != nil {
		return nil, detail(ErrClusterInvalid, "kubeconfig credential: %s", err)
	}
	return out, nil
}
