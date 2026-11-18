package v1

import (
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// 访问日志脱敏：Kratos 的 logging 中间件把整个请求的 proto 文本写进 args。kit/httpx 的按字段名
// 打码只认 token / secret 等少数名字，且对「字符串里的 YAML」无能为力——kubeconfig、对象 YAML、
// Helm values、客户端私钥、带账号的代理地址都会原样进日志。这些请求实现 logging.Redacter，
// 在序列化前把敏感字段整体替换掉（保留其余字段供排查）。

// redactedFields 需要整体替换的字段名（任意嵌套层级）。
var redactedFields = map[protoreflect.Name]struct{}{
	"token": {}, "client_cert": {}, "client_key": {}, "kubeconfig": {}, "proxy_url": {},
	"yaml": {}, "values": {}, "secret": {}, "password": {},
}

const redactedValue = "***"

func redact(m proto.Message) string {
	c := proto.Clone(m)
	scrub(c.ProtoReflect())
	return prototext.MarshalOptions{}.Format(c)
}

func scrub(m protoreflect.Message) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		switch {
		case fd.IsList() || fd.IsMap():
		case fd.Kind() == protoreflect.StringKind:
			if _, ok := redactedFields[fd.Name()]; ok && v.String() != "" {
				m.Set(fd, protoreflect.ValueOfString(redactedValue))
			}
		case fd.Kind() == protoreflect.MessageKind:
			scrub(v.Message())
		}
		return true
	})
}

// Redact implements logging.Redacter.
func (x *CreateClusterRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *UpdateClusterRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *ParseKubeconfigRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *TestClusterRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *CreateHelmRepoRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *UpdateHelmRepoRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *TestHelmRepoRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *PreviewHelmReleaseRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *InstallHelmReleaseRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *UpgradeHelmReleaseRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *PreviewObjectRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *ApplyObjectRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *PreviewManifestRequest) Redact() string { return redact(x) }

// Redact implements logging.Redacter.
func (x *ApplyManifestRequest) Redact() string { return redact(x) }
