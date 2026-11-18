package k8scontract

// GVR 资源类型标识。
type GVR struct {
	Group    string
	Version  string
	Resource string
}

// 定制页面用到的资源类型。
var (
	GVRPod          = GVR{"", "v1", "pods"}
	GVRNode         = GVR{"", "v1", "nodes"}
	GVRNamespace    = GVR{"", "v1", "namespaces"}
	GVRService      = GVR{"", "v1", "services"}
	GVRConfigMap    = GVR{"", "v1", "configmaps"}
	GVRSecret       = GVR{"", "v1", "secrets"}
	GVREvent        = GVR{"", "v1", "events"}
	GVRPVC          = GVR{"", "v1", "persistentvolumeclaims"}
	GVRPV           = GVR{"", "v1", "persistentvolumes"}
	GVRQuota        = GVR{"", "v1", "resourcequotas"}
	GVRLimitRange   = GVR{"", "v1", "limitranges"}
	GVRDeployment   = GVR{"apps", "v1", "deployments"}
	GVRStatefulSet  = GVR{"apps", "v1", "statefulsets"}
	GVRDaemonSet    = GVR{"apps", "v1", "daemonsets"}
	GVRReplicaSet   = GVR{"apps", "v1", "replicasets"}
	GVRJob          = GVR{"batch", "v1", "jobs"}
	GVRCronJob      = GVR{"batch", "v1", "cronjobs"}
	GVRIngress      = GVR{"networking.k8s.io", "v1", "ingresses"}
	GVRStorageClass = GVR{"storage.k8s.io", "v1", "storageclasses"}
)

// cachedResources 走服务端缓存的资源（技术设计 §4.5）。ConfigMap 与 Secret 只缓存元数据。
var cachedResources = map[GVR]struct{}{
	GVRPod: {}, GVRNode: {}, GVRNamespace: {}, GVRService: {}, GVRConfigMap: {}, GVRSecret: {},
	GVRPVC: {}, GVRPV: {}, GVRDeployment: {}, GVRStatefulSet: {}, GVRDaemonSet: {}, GVRReplicaSet: {},
	GVRJob: {}, GVRCronJob: {}, GVRIngress: {}, GVRStorageClass: {},
}

// Cached 报告该资源是否走服务端缓存。
func Cached(g GVR) bool {
	_, ok := cachedResources[g]
	return ok
}

// CachedResources 走服务端缓存的资源清单（拷贝）。
func CachedResources() []GVR {
	out := make([]GVR, 0, len(cachedResources))
	for g := range cachedResources {
		out = append(out, g)
	}
	return out
}

// MetadataOnly 报告该资源是否只缓存元数据（内容可能很大或敏感）。
func MetadataOnly(g GVR) bool { return g == GVRConfigMap || g == GVRSecret }

// GVR 取出 Ref 的资源类型。
func (r Ref) GVR() GVR { return GVR{r.Group, r.Version, r.Resource} }

// GVR 取出列表请求的资源类型。
func (r ListRequest) GVR() GVR { return GVR{r.Group, r.Version, r.Resource} }
