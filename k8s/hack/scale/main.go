//go:build scale

// Command scale 规模验证工具（docs/k8s-scale-report.md）：往 kwok 集群里造对象、制造变更、
// 并通过数据面的内部接口压测列表与变更订阅。只在显式带 -tags scale 时编译：
//
//	go run -tags scale ./hack/scale <gen|resize|churn|setup|bench|watch> [flags]
//
// 绝不要指向真实集群：gen 会创建上千个假节点与数万个 Pod。
//
//nolint:gosec // 压测工具：假数据里的占位凭据、伪随机抽样与小整数转换都与安全无关
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lanyulei/reborn/backend/kit/k8scontract"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

const (
	clusterID = "018f3e1a-5ca1-7000-8000-000000000001"
	adminID   = "018f3e1a-5ca1-7000-8000-0000000000a1"
	userID    = "018f3e1a-5ca1-7000-8000-0000000000b1"
	roleID    = "018f3e1a-5ca1-7000-8000-0000000000c1"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: scale <gen|resize|churn|setup|bench|watch> [flags]")
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "gen":
		err = gen(args)
	case "resize":
		err = resize(args)
	case "churn":
		err = churn(args)
	case "setup":
		err = setup(args)
	case "bench":
		err = bench(args)
	case "watch":
		err = watchers(args)
	case "warm":
		err = warm(args)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func kube(path string) (*kubernetes.Clientset, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, err
	}
	cfg.QPS, cfg.Burst = 500, 1000
	return kubernetes.NewForConfig(cfg)
}

// parallel 并发执行 n 个任务（已存在视为成功）。
func parallel(n, workers int, fn func(i int) error) error {
	var wg sync.WaitGroup
	var firstErr atomic.Value
	ch := make(chan int)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				if err := fn(i); err != nil && !apierrors.IsAlreadyExists(err) {
					firstErr.CompareAndSwap(nil, err)
				}
			}
		}()
	}
	for i := range n {
		ch <- i
	}
	close(ch)
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok {
		return err
	}
	return nil
}

var fakeSelector = map[string]string{"type": "kwok"}

var fakeTolerations = []corev1.Toleration{{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}

// podTemplate 接近真实业务 Pod 的模板：两个容器、环境变量、挂载、探针、资源、标签与注解。
func podTemplate(app, tier string) corev1.PodTemplateSpec {
	env := []corev1.EnvVar{
		{Name: "SPRING_PROFILES_ACTIVE", Value: "production"},
		{Name: "JAVA_OPTS", Value: "-Xms1g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"},
		{Name: "DB_HOST", Value: "mysql-primary.database.svc.cluster.local"},
		{Name: "DB_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: app + "-secret"}, Key: "password"}}},
		{Name: "REDIS_ADDR", Value: "redis-cluster.cache.svc.cluster.local:6379"},
		{Name: "KAFKA_BROKERS", Value: "kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092"},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
		{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://otel-collector.observability:4317"},
		{Name: "LOG_LEVEL", Value: "info"},
	}
	probe := &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/actuator/health", Port: intstr.FromInt32(8080)}},
		InitialDelaySeconds: 30, PeriodSeconds: 10, TimeoutSeconds: 3, FailureThreshold: 3,
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": app, "tier": tier, "app.kubernetes.io/name": app, "app.kubernetes.io/part-of": "scale", "version": "v1.42.7"},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true", "prometheus.io/port": "8080", "prometheus.io/path": "/actuator/prometheus",
				"sidecar.istio.io/inject": "false", "kubectl.kubernetes.io/restartedAt": "2026-11-01T08:00:00+08:00",
			},
		},
		Spec: corev1.PodSpec{
			NodeSelector: fakeSelector, Tolerations: fakeTolerations,
			TerminationGracePeriodSeconds: ptr(int64(30)),
			Containers: []corev1.Container{
				{
					Name: "app", Image: "registry.example.com/platform/" + app + ":v1.42.7-20261101.3",
					Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}, {Name: "grpc", ContainerPort: 9090}},
					Env:   env,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("2Gi")},
					},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "config", MountPath: "/etc/app"}, {Name: "secret", MountPath: "/etc/secret", ReadOnly: true}, {Name: "tmp", MountPath: "/tmp"},
					},
					LivenessProbe: probe, ReadinessProbe: probe,
				},
				{
					Name: "log-agent", Image: "registry.example.com/infra/fluent-bit:3.2.1",
					Env: []corev1.EnvVar{{Name: "FLUENT_ES_HOST", Value: "es.logging.svc"}, {Name: "APP_NAME", Value: app}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("50m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
						Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
					},
					VolumeMounts: []corev1.VolumeMount{{Name: "tmp", MountPath: "/var/log/app"}},
				},
			},
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: app + "-config"}}}},
				{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: app + "-secret"}}},
				{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		},
	}
}

func ptr[T any](v T) *T { return &v }

func nsName(i int) string { return fmt.Sprintf("scale-ns-%03d", i) }

// gen 造节点、命名空间与各类对象。Deployment 的副本数由 resize 调整（分级加载）。
func gen(args []string) error {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	kc := fs.String("kubeconfig", "", "kwok cluster kubeconfig")
	nodes := fs.Int("nodes", 1200, "fake nodes")
	nss := fs.Int("namespaces", 200, "namespaces")
	deploys := fs.Int("deployments", 2500, "deployments in total")
	replicas := fs.Int("replicas", 2, "replicas per deployment")
	_ = fs.Parse(args)
	cs, err := kube(*kc)
	if err != nil {
		return err
	}
	ctx := context.Background()
	start := time.Now()
	err = parallel(*nodes, 32, func(i int) error {
		zone := fmt.Sprintf("cn-east-1%c", 'a'+i%3)
		n := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("kwok-node-%04d", i),
				Labels: map[string]string{
					"type": "kwok", "kubernetes.io/role": "worker", "node-role.kubernetes.io/worker": "", "kubernetes.io/arch": "amd64", "kubernetes.io/os": "linux",
					"kubernetes.io/hostname": fmt.Sprintf("kwok-node-%04d", i), "topology.kubernetes.io/region": "cn-east-1", "topology.kubernetes.io/zone": zone,
					"node.kubernetes.io/instance-type": "ecs.g7.16xlarge", "nodepool": fmt.Sprintf("pool-%d", i%8),
				},
				Annotations: map[string]string{"kwok.x-k8s.io/node": "fake", "node.alpha.kubernetes.io/ttl": "0"},
			},
			Spec: corev1.NodeSpec{
				ProviderID: fmt.Sprintf("alicloud://cn-east-1.i-%012d", i),
				Taints:     []corev1.Taint{{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule}},
			},
			Status: corev1.NodeStatus{
				Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("64"), corev1.ResourceMemory: resource.MustParse("256Gi"), corev1.ResourcePods: resource.MustParse("110")},
				Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("63"), corev1.ResourceMemory: resource.MustParse("250Gi"), corev1.ResourcePods: resource.MustParse("110")},
				NodeInfo:    corev1.NodeSystemInfo{KubeletVersion: "v1.35.1", OSImage: "Alibaba Cloud Linux 3", KernelVersion: "5.10.134-16.al8.x86_64", ContainerRuntimeVersion: "containerd://1.7.24", Architecture: "amd64", OperatingSystem: "linux"},
			},
		}
		_, err := cs.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("nodes: %w", err)
	}
	fmt.Printf("nodes: %d in %s\n", *nodes, time.Since(start).Round(time.Millisecond))
	if err := parallel(*nss, 16, func(i int) error {
		_, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: nsName(i), Labels: map[string]string{"team": fmt.Sprintf("team-%02d", i%25), "scale": "true"}}}, metav1.CreateOptions{})
		return err
	}); err != nil {
		return fmt.Errorf("namespaces: %w", err)
	}
	tiers := []string{"frontend", "backend", "worker", "gateway"}
	err = parallel(*deploys, 32, func(i int) error {
		ns, app, tier := nsName(i%*nss), fmt.Sprintf("svc-%04d", i), tiers[i%len(tiers)]
		tpl := podTemplate(app, tier)
		sel := map[string]string{"app": app}
		if _, err := cs.CoreV1().ConfigMaps(ns).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: app + "-config", Labels: sel}, Data: map[string]string{"application.yaml": strings.Repeat("key: value\n", 60)}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		if _, err := cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: app + "-secret", Labels: sel}, StringData: map[string]string{"password": "not-a-real-secret"}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		if _, err := cs.CoreV1().Services(ns).Create(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: app, Labels: sel}, Spec: corev1.ServiceSpec{Selector: sel, Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080)}}}}, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		if i%6 == 0 {
			pt := netv1.PathTypePrefix
			ing := &netv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: app, Labels: sel}, Spec: netv1.IngressSpec{Rules: []netv1.IngressRule{{Host: app + ".example.com", IngressRuleValue: netv1.IngressRuleValue{HTTP: &netv1.HTTPIngressRuleValue{Paths: []netv1.HTTPIngressPath{{Path: "/", PathType: &pt, Backend: netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: app, Port: netv1.ServiceBackendPort{Number: 80}}}}}}}}}}}
			if _, err := cs.NetworkingV1().Ingresses(ns).Create(ctx, ing, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		}
		if i%12 == 0 {
			cj := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: app + "-cron", Labels: sel}, Spec: batchv1.CronJobSpec{Schedule: "*/30 * * * *", Suspend: ptr(true), JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, NodeSelector: fakeSelector, Tolerations: fakeTolerations, Containers: []corev1.Container{{Name: "job", Image: "registry.example.com/platform/job:v1"}}}}}}}}
			if _, err := cs.BatchV1().CronJobs(ns).Create(ctx, cj, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
			st := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: app + "-db", Labels: sel}, Spec: appsv1.StatefulSetSpec{Replicas: ptr(int32(2)), ServiceName: app, PodManagementPolicy: appsv1.ParallelPodManagement, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app + "-db"}}, Template: podTemplate(app+"-db", "data")}}
			if _, err := cs.AppsV1().StatefulSets(ns).Create(ctx, st, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
				return err
			}
		}
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: app, Labels: tpl.Labels, Annotations: map[string]string{"deployment.kubernetes.io/owner": "team-" + tier, "kubernetes.io/change-cause": "release v1.42.7"}},
			Spec:       appsv1.DeploymentSpec{Replicas: ptr(int32(*replicas)), Selector: &metav1.LabelSelector{MatchLabels: sel}, Template: tpl},
		}
		_, err := cs.AppsV1().Deployments(ns).Create(ctx, d, metav1.CreateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("workloads: %w", err)
	}
	fmt.Printf("objects created in %s\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// resize 把全部 Deployment 的副本数改为 replicas，并等待 Pod 总数到位。
func resize(args []string) error {
	fs := flag.NewFlagSet("resize", flag.ExitOnError)
	kc := fs.String("kubeconfig", "", "kwok cluster kubeconfig")
	replicas := fs.Int("replicas", 8, "replicas per deployment")
	want := fs.Int("wait-running", 0, "wait until this many pods are Running (0 = do not wait)")
	_ = fs.Parse(args)
	cs, err := kube(*kc)
	if err != nil {
		return err
	}
	ctx := context.Background()
	list, err := cs.AppsV1().Deployments("").List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/part-of=scale"})
	if err != nil {
		return err
	}
	patch := fmt.Appendf(nil, `{"spec":{"replicas":%d}}`, *replicas)
	start := time.Now()
	if err := parallel(len(list.Items), 32, func(i int) error {
		d := &list.Items[i]
		_, err := cs.AppsV1().Deployments(d.Namespace).Patch(ctx, d.Name, types.MergePatchType, patch, metav1.PatchOptions{})
		return err
	}); err != nil {
		return err
	}
	fmt.Printf("patched %d deployments in %s\n", len(list.Items), time.Since(start).Round(time.Millisecond))
	for *want > 0 {
		pods, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{FieldSelector: "status.phase=Running", ResourceVersion: "0", Limit: 1})
		if err != nil {
			return err
		}
		n := len(pods.Items)
		if pods.RemainingItemCount != nil {
			n += int(*pods.RemainingItemCount)
		}
		fmt.Printf("  running pods: %d (%s)\n", n, time.Since(start).Round(time.Second))
		if n >= *want {
			break
		}
		time.Sleep(10 * time.Second)
	}
	return nil
}

// churn 以固定速率给随机 Pod 打注解（每次都是一次 Pod 更新事件）。
func churn(args []string) error {
	fs := flag.NewFlagSet("churn", flag.ExitOnError)
	kc := fs.String("kubeconfig", "", "kwok cluster kubeconfig")
	rate := fs.Int("rate", 100, "pod updates per second")
	dur := fs.Duration("duration", 3*time.Minute, "how long")
	_ = fs.Parse(args)
	cs, err := kube(*kc)
	if err != nil {
		return err
	}
	ctx := context.Background()
	type key struct{ ns, name string }
	var keys []key
	pods, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/part-of=scale", ResourceVersion: "0"})
	if err != nil {
		return err
	}
	for i := range pods.Items {
		keys = append(keys, key{pods.Items[i].Namespace, pods.Items[i].Name})
	}
	pods = nil
	fmt.Printf("churning %d pods at %d/s for %s\n", len(keys), *rate, *dur)
	var done, failed atomic.Int64
	jobs := make(chan key, 1024)
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range jobs {
				patch := fmt.Appendf(nil, `{"metadata":{"annotations":{"scale/churn":"%d"}}}`, time.Now().UnixNano())
				if _, err := cs.CoreV1().Pods(k.ns).Patch(ctx, k.name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
					failed.Add(1)
				}
				done.Add(1)
			}
		}()
	}
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	end := time.After(*dur)
loop:
	for {
		select {
		case <-end:
			break loop
		case <-tick.C:
			select {
			case jobs <- keys[rand.IntN(len(keys))]:
			default:
			}
		}
	}
	close(jobs)
	wg.Wait()
	fmt.Printf("churn done: %d updates, %d failed, effective %.0f/s\n", done.Load(), failed.Load(), float64(done.Load())/dur.Seconds())
	return nil
}

// setup 在数据面的库里登记 kwok 集群，并给非管理员角色授予若干命名空间的只读范围。
func setup(args []string) error {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	kcPath := fs.String("kubeconfig", "", "kwok cluster kubeconfig")
	dsn := fs.String("dsn", os.Getenv("SCALE_DSN"), "database DSN (or SCALE_DSN)")
	master := fs.String("master-key", os.Getenv("SCALE_MASTER_KEY"), "secretbox master key (or SCALE_MASTER_KEY)")
	scoped := fs.Int("scoped-namespaces", 5, "namespaces granted to the non-admin role")
	_ = fs.Parse(args)
	raw, err := os.ReadFile(*kcPath)
	if err != nil {
		return err
	}
	var kc struct {
		Clusters []struct {
			Cluster struct {
				Server string `json:"server"`
				CA     string `json:"certificate-authority-data"`
			} `json:"cluster"`
		} `json:"clusters"`
		Users []struct {
			User struct {
				Cert string `json:"client-certificate-data"`
				Key  string `json:"client-key-data"`
			} `json:"user"`
		} `json:"users"`
	}
	if err := yaml.Unmarshal(raw, &kc); err != nil || len(kc.Clusters) == 0 || len(kc.Users) == 0 {
		return fmt.Errorf("parse kubeconfig: %v", err)
	}
	b64 := func(s string) string { out, _ := base64.StdEncoding.DecodeString(s); return string(out) }
	plain, _ := json.Marshal(k8scontract.Credential{ClientCert: b64(kc.Users[0].User.Cert), ClientKey: b64(kc.Users[0].User.Key)})
	box, err := secretbox.New(*master)
	if err != nil {
		return err
	}
	sealed, err := box.Seal(string(plain))
	if err != nil {
		return err
	}
	ctx := context.Background()
	db, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close(ctx) }()
	if _, err := db.Exec(ctx, `INSERT INTO k8s_cluster (id, name, access_mode, agent_location, api_server, ca_cert, credential, env)
		VALUES ($1, 'scale', 2, 0, $2, $3, $4, 'test')
		ON CONFLICT (id) DO UPDATE SET api_server = EXCLUDED.api_server, ca_cert = EXCLUDED.ca_cert, credential = EXCLUDED.credential, version = k8s_cluster.version + 1, update_time = now()`,
		clusterID, kc.Clusters[0].Cluster.Server, b64(kc.Clusters[0].Cluster.CA), sealed); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `DELETE FROM k8s_role_scope WHERE cluster_id = $1`, clusterID); err != nil {
		return err
	}
	for i := range *scoped {
		if _, err := db.Exec(ctx, `INSERT INTO k8s_role_scope (role_id, cluster_id, namespace, level) VALUES ($1, $2, $3, 1)`, roleID, clusterID, nsName(i*7)); err != nil {
			return err
		}
	}
	_, err = db.Exec(ctx, `UPDATE k8s_cfg_version SET version = version + 1`)
	return err
}

type client struct {
	url string
	key []byte
	hc  *http.Client
}

func newClient(fs *flag.FlagSet) (*client, func()) {
	url := fs.String("plane", "http://127.0.0.1:18260", "plane internal URL")
	key := fs.String("internal-key", os.Getenv("SCALE_INTERNAL_KEY"), "plane internal key (or SCALE_INTERNAL_KEY)")
	c := &client{hc: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512, MaxConnsPerHost: 0}}}
	return c, func() { c.url, c.key = strings.TrimRight(*url, "/"), []byte(*key) }
}

var (
	admin = k8scontract.Identity{UserID: adminID, Username: "admin", IsAdmin: true, ClientIP: "127.0.0.1", RequestID: "scale"}
	user  = k8scontract.Identity{UserID: userID, Username: "dev", RoleIDs: []string{roleID}, ClientIP: "127.0.0.1", RequestID: "scale"}
)

func (c *client) post(ctx context.Context, id k8scontract.Identity, path string, body any) (*http.Response, error) {
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, c.url+path, bytes.NewReader(raw))
	id.IssuedAt = time.Now().Unix()
	if err := k8scontract.SignIdentity(req.Header, c.key, id); err != nil {
		return nil, err
	}
	req.Header.Set(k8scontract.ClusterHeader, clusterID)
	return c.hc.Do(req)
}

// list 发一次列表请求，返回耗时、响应字节数与总数。
func (c *client) list(id k8scontract.Identity, req k8scontract.ListRequest) (time.Duration, int, int64, error) {
	start := time.Now()
	resp, err := c.post(context.Background(), id, k8scontract.InternalOpPath+k8scontract.OpList, req)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	d := time.Since(start)
	if resp.StatusCode != http.StatusOK {
		return d, len(raw), 0, fmt.Errorf("status %d: %.200s", resp.StatusCode, raw)
	}
	var out struct {
		Total int64 `json:"total"`
	}
	_ = json.Unmarshal(raw, &out)
	return d, len(raw), out.Total, nil
}

type scenario struct {
	name string
	id   k8scontract.Identity
	req  func(i int) k8scontract.ListRequest
}

func podReq(mod func(r *k8scontract.ListRequest, i int)) func(i int) k8scontract.ListRequest {
	return func(i int) k8scontract.ListRequest {
		r := k8scontract.ListRequest{Version: "v1", Resource: "pods", Page: 1, PageSize: 20}
		mod(&r, i)
		return r
	}
}

func scenarios() []scenario {
	return []scenario{
		{"全部命名空间·第 1 页（缺省按名称）", admin, podReq(func(*k8scontract.ListRequest, int) {})},
		{"全部命名空间·深翻页（随机页）", admin, podReq(func(r *k8scontract.ListRequest, i int) { r.Page = 1 + (i*37)%2000 })},
		{"全部命名空间·每页 200", admin, podReq(func(r *k8scontract.ListRequest, _ int) { r.PageSize = 200 })},
		{"全部命名空间·关键字（命中约 20）", admin, podReq(func(r *k8scontract.ListRequest, i int) { r.Keyword = fmt.Sprintf("svc-%04d", i%2500) })},
		{"全部命名空间·关键字（命中上千）", admin, podReq(func(r *k8scontract.ListRequest, i int) { r.Keyword = fmt.Sprintf("svc-0%d", i%10) })},
		{"全部命名空间·状态筛选 ok", admin, podReq(func(r *k8scontract.ListRequest, _ int) {
			r.Filters = map[string]string{k8scontract.FilterState: k8scontract.StateOK}
		})},
		{"全部命名空间·状态筛选 abnormal", admin, podReq(func(r *k8scontract.ListRequest, _ int) {
			r.Filters = map[string]string{k8scontract.FilterState: k8scontract.StateAbnormal}
		})},
		{"全部命名空间·按节点筛选", admin, podReq(func(r *k8scontract.ListRequest, i int) {
			r.Filters = map[string]string{k8scontract.FilterNode: fmt.Sprintf("kwok-node-%04d", i%1200)}
		})},
		{"全部命名空间·标签选择器", admin, podReq(func(r *k8scontract.ListRequest, i int) { r.LabelSelector = "tier in (frontend,gateway),app!=svc-0001" })},
		{"全部命名空间·按重启次数降序", admin, podReq(func(r *k8scontract.ListRequest, _ int) { r.SortBy, r.SortDesc = "restarts", true })},
		{"全部命名空间·按节点排序", admin, podReq(func(r *k8scontract.ListRequest, _ int) { r.SortBy = "node" })},
		{"全部命名空间·按状态排序", admin, podReq(func(r *k8scontract.ListRequest, _ int) { r.SortBy = "status" })},
		{"全部命名空间·按创建时间降序", admin, podReq(func(r *k8scontract.ListRequest, _ int) { r.SortBy, r.SortDesc = "createTime", true })},
		{"全部命名空间·按 IP 排序", admin, podReq(func(r *k8scontract.ListRequest, _ int) { r.SortBy = "ip" })},
		{"单命名空间·第 1 页", admin, podReq(func(r *k8scontract.ListRequest, i int) { r.Namespace = nsName(i % 200) })},
		{"单命名空间·关键字 + 排序", admin, podReq(func(r *k8scontract.ListRequest, i int) {
			r.Namespace, r.Keyword, r.SortBy = nsName(i%200), "svc", "restarts"
		})},
		{"非管理员（5 个命名空间）·全部命名空间", user, podReq(func(*k8scontract.ListRequest, int) {})},
		{"非管理员·全部命名空间·关键字 + 排序", user, podReq(func(r *k8scontract.ListRequest, _ int) { r.Keyword, r.SortBy = "svc", "node" })},
		{"非管理员·单命名空间", user, podReq(func(r *k8scontract.ListRequest, i int) { r.Namespace = nsName((i % 5) * 7) })},
		{"节点列表（1200）·按名称", admin, func(int) k8scontract.ListRequest {
			return k8scontract.ListRequest{Version: "v1", Resource: "nodes", Page: 1, PageSize: 50}
		}},
		{"Deployment 列表·全部命名空间·关键字", admin, func(i int) k8scontract.ListRequest {
			return k8scontract.ListRequest{Group: "apps", Version: "v1", Resource: "deployments", Page: 1, PageSize: 20, Keyword: fmt.Sprintf("svc-1%d", i%10)}
		}},
	}
}

func pct(d []time.Duration, p float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	return d[min(len(d)-1, int(float64(len(d))*p))]
}

// bench 逐个场景压测列表：每个场景 n 次请求、c 路并发，输出 p50 / p95 / p99 / max；-mix 再跑一轮混合负载。
func bench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	c, bind := newClient(fs)
	n := fs.Int("n", 200, "requests per scenario")
	conc := fs.Int("c", 8, "concurrency")
	mix := fs.Duration("mix", 0, "also run a mixed workload for this long")
	_ = fs.Parse(args)
	bind()
	run := func(sc scenario, n, conc int) ([]time.Duration, int, int64, error) {
		durs := make([]time.Duration, n)
		var size atomic.Int64
		var total atomic.Int64
		err := parallel(n, conc, func(i int) error {
			d, sz, t, err := c.list(sc.id, sc.req(i))
			durs[i] = d
			size.Store(int64(sz))
			total.Store(t)
			return err
		})
		sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
		return durs, int(size.Load()), total.Load(), err
	}
	fmt.Printf("| 场景 | 总数 | p50 | p95 | p99 | max | 响应 |\n|---|---|---|---|---|---|---|\n")
	for _, sc := range scenarios() {
		if _, _, _, err := run(sc, 2, 1); err != nil { // 预热（缓存建立、权限询问缓存）
			fmt.Printf("| %s | 失败：%v | | | | | |\n", sc.name, err)
			continue
		}
		durs, size, total, err := run(sc, *n, *conc)
		if err != nil {
			fmt.Printf("| %s | 失败：%v | | | | | |\n", sc.name, err)
			continue
		}
		ms := func(d time.Duration) string { return fmt.Sprintf("%.0f ms", float64(d.Microseconds())/1000) }
		fmt.Printf("| %s | %d | %s | %s | %s | %s | %d KB |\n", sc.name, total, ms(pct(durs, 0.5)), ms(pct(durs, 0.95)), ms(pct(durs, 0.99)), ms(durs[len(durs)-1]), size/1024)
	}
	if *mix > 0 {
		scs := scenarios()
		var mu sync.Mutex
		var durs []time.Duration
		var slowest time.Duration
		var slowestName string
		var errs atomic.Int64
		end := time.Now().Add(*mix)
		var wg sync.WaitGroup
		for w := range *conc {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := w; time.Now().Before(end); i++ {
					sc := scs[rand.IntN(len(scs))]
					d, _, _, err := c.list(sc.id, sc.req(i))
					if err != nil {
						errs.Add(1)
						continue
					}
					mu.Lock()
					durs = append(durs, d)
					if d > slowest {
						slowest, slowestName = d, sc.name
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
		fmt.Printf("\n混合负载 %s，并发 %d：%d 次请求（%.0f 次/秒），失败 %d，p50 %s p95 %s p99 %s max %s\n", *mix, *conc, len(durs),
			float64(len(durs))/mix.Seconds(), errs.Load(), pct(durs, 0.5).Round(time.Millisecond), pct(durs, 0.95).Round(time.Millisecond), pct(durs, 0.99).Round(time.Millisecond), durs[max(len(durs)-1, 0)].Round(time.Millisecond))
		fmt.Printf("最慢的一次：%s（%s）\n", slowest.Round(time.Millisecond), slowestName)
	}
	return nil
}

// watchers 打开 n 条 Pod 变更订阅，统计每条每秒收到的通知数。
func watchers(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	c, bind := newClient(fs)
	n := fs.Int("n", 200, "subscribers")
	dur := fs.Duration("duration", time.Minute, "how long")
	_ = fs.Parse(args)
	bind()
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()
	counts := make([]atomic.Int64, *n)
	var opened, failed atomic.Int64
	var maxPerSec atomic.Int64
	var wg sync.WaitGroup
	for i := range *n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := k8scontract.WatchRequest{Version: "v1", Resource: "pods"}
			if i%2 == 1 {
				req.Namespace = nsName(i % 200)
			}
			resp, err := c.post(ctx, admin, k8scontract.InternalStreamPath+k8scontract.StreamWatch, req)
			if err != nil {
				failed.Add(1)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				failed.Add(1)
				return
			}
			opened.Add(1)
			buf := make([]byte, 4096)
			var window time.Time
			var inWindow int64
			for {
				m, err := resp.Body.Read(buf)
				if k := int64(bytes.Count(buf[:m], []byte("event: "+k8scontract.EventChange))); k > 0 {
					counts[i].Add(k)
					if now := time.Now(); now.Sub(window) >= time.Second {
						window, inWindow = now, k
					} else {
						inWindow += k
					}
					for {
						cur := maxPerSec.Load()
						if inWindow <= cur || maxPerSec.CompareAndSwap(cur, inWindow) {
							break
						}
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	var all, nsOnly int64
	for i := range counts {
		if i%2 == 0 {
			all += counts[i].Load()
		} else {
			nsOnly += counts[i].Load()
		}
	}
	half := float64(*n) / 2
	fmt.Printf("订阅 %d 条（打开 %d，失败 %d），持续 %s：全部命名空间的订阅平均 %.2f 次/秒，单命名空间的订阅平均 %.3f 次/秒，任一订阅 1 秒内最多 %d 次\n",
		*n, opened.Load(), failed.Load(), *dur, float64(all)/half/dur.Seconds(), float64(nsOnly)/half/dur.Seconds(), maxPerSec.Load())
	return nil
}

// warm 把全部走缓存的资源各列一次（建立全部 16 类缓存），输出每类的对象数与首次耗时。
func warm(args []string) error {
	fs := flag.NewFlagSet("warm", flag.ExitOnError)
	c, bind := newClient(fs)
	_ = fs.Parse(args)
	bind()
	gvrs := k8scontract.CachedResources()
	sort.Slice(gvrs, func(i, j int) bool { return gvrs[i].Resource < gvrs[j].Resource })
	for _, g := range gvrs {
		var d time.Duration
		var total int64
		var err error
		for range 20 { // 缓存建立中（503）时重试
			d, _, total, err = c.list(admin, k8scontract.ListRequest{Group: g.Group, Version: g.Version, Resource: g.Resource, Page: 1, PageSize: 1})
			if err == nil {
				break
			}
			time.Sleep(2 * time.Second)
		}
		if err != nil {
			fmt.Printf("%-24s 失败：%v\n", g.Resource, err)
			continue
		}
		fmt.Printf("%-24s %6d 个  首次 %s\n", g.Resource, total, d.Round(time.Millisecond))
	}
	return nil
}
