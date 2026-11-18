package kube

import (
	"encoding/json"
	"flag"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// scalePodJSON 一个接近真实业务 Pod 的对象（两个容器、环境变量、挂载、探针、状态；约 6KB，不含 managedFields），
// 结构取自 kwok 集群里由 hack/scale 生成的 Pod。
const scalePodJSON = `{"apiVersion":"v1","kind":"Pod","metadata":{"name":"%[1]s","generateName":"%[2]s-7d9f8c6b5-","namespace":"%[3]s","uid":"%[4]s",
"resourceVersion":"123456","creationTimestamp":"2026-11-01T08:00:00Z",
"labels":{"app":"%[2]s","tier":"backend","app.kubernetes.io/name":"%[2]s","app.kubernetes.io/part-of":"scale","version":"v1.42.7","pod-template-hash":"7d9f8c6b5"},
"annotations":{"prometheus.io/scrape":"true","prometheus.io/port":"8080","prometheus.io/path":"/actuator/prometheus","sidecar.istio.io/inject":"false","kubectl.kubernetes.io/restartedAt":"2026-11-01T08:00:00+08:00"},
"ownerReferences":[{"apiVersion":"apps/v1","kind":"ReplicaSet","name":"%[2]s-7d9f8c6b5","uid":"rs-%[2]s","controller":true,"blockOwnerDeletion":true}]},
"spec":{"nodeName":"%[5]s","restartPolicy":"Always","terminationGracePeriodSeconds":30,"dnsPolicy":"ClusterFirst","serviceAccountName":"default","schedulerName":"default-scheduler","priority":0,
"containers":[{"name":"app","image":"registry.example.com/platform/%[2]s:v1.42.7-20261101.3","imagePullPolicy":"IfNotPresent",
"ports":[{"name":"http","containerPort":8080,"protocol":"TCP"},{"name":"grpc","containerPort":9090,"protocol":"TCP"}],
"env":[{"name":"SPRING_PROFILES_ACTIVE","value":"production"},{"name":"JAVA_OPTS","value":"-Xms1g -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Dfile.encoding=UTF-8"},{"name":"DB_HOST","value":"mysql-primary.database.svc.cluster.local"},
{"name":"DB_PASSWORD","valueFrom":{"secretKeyRef":{"name":"%[2]s-secret","key":"password"}}},{"name":"REDIS_ADDR","value":"redis-cluster.cache.svc.cluster.local:6379"},
{"name":"KAFKA_BROKERS","value":"kafka-0.kafka:9092,kafka-1.kafka:9092,kafka-2.kafka:9092"},{"name":"POD_NAME","valueFrom":{"fieldRef":{"apiVersion":"v1","fieldPath":"metadata.name"}}},
{"name":"OTEL_EXPORTER_OTLP_ENDPOINT","value":"http://otel-collector.observability:4317"},{"name":"LOG_LEVEL","value":"info"}],
"resources":{"requests":{"cpu":"500m","memory":"1Gi"},"limits":{"cpu":"2","memory":"2Gi"}},
"volumeMounts":[{"name":"config","mountPath":"/etc/app"},{"name":"secret","mountPath":"/etc/secret","readOnly":true},{"name":"tmp","mountPath":"/tmp"},{"name":"kube-api-access-x7k2p","mountPath":"/var/run/secrets/kubernetes.io/serviceaccount","readOnly":true}],
"livenessProbe":{"httpGet":{"path":"/actuator/health","port":8080,"scheme":"HTTP"},"initialDelaySeconds":30,"periodSeconds":10,"timeoutSeconds":3,"failureThreshold":3,"successThreshold":1},
"readinessProbe":{"httpGet":{"path":"/actuator/health","port":8080,"scheme":"HTTP"},"initialDelaySeconds":30,"periodSeconds":10,"timeoutSeconds":3,"failureThreshold":3,"successThreshold":1},
"terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File"},
{"name":"log-agent","image":"registry.example.com/infra/fluent-bit:3.2.1","imagePullPolicy":"IfNotPresent","env":[{"name":"FLUENT_ES_HOST","value":"es.logging.svc"},{"name":"APP_NAME","value":"%[2]s"}],
"resources":{"requests":{"cpu":"50m","memory":"64Mi"},"limits":{"cpu":"200m","memory":"128Mi"}},"volumeMounts":[{"name":"tmp","mountPath":"/var/log/app"}],"terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File"}],
"volumes":[{"name":"config","configMap":{"name":"%[2]s-config","defaultMode":420}},{"name":"secret","secret":{"secretName":"%[2]s-secret","defaultMode":420}},{"name":"tmp","emptyDir":{}},
{"name":"kube-api-access-x7k2p","projected":{"defaultMode":420,"sources":[{"serviceAccountToken":{"expirationSeconds":3607,"path":"token"}},{"configMap":{"name":"kube-root-ca.crt","items":[{"key":"ca.crt","path":"ca.crt"}]}}]}}],
"tolerations":[{"key":"node.kubernetes.io/not-ready","operator":"Exists","effect":"NoExecute","tolerationSeconds":300},{"key":"node.kubernetes.io/unreachable","operator":"Exists","effect":"NoExecute","tolerationSeconds":300}]},
"status":{"phase":"Running","hostIP":"10.0.%[6]d.%[7]d","podIP":"10.%[8]d.%[6]d.%[7]d","podIPs":[{"ip":"10.%[8]d.%[6]d.%[7]d"}],"qosClass":"Burstable","startTime":"2026-11-01T08:00:05Z",
"conditions":[{"type":"PodReadyToStartContainers","status":"True","lastTransitionTime":"2026-11-01T08:00:07Z"},{"type":"Initialized","status":"True","lastTransitionTime":"2026-11-01T08:00:05Z"},
{"type":"Ready","status":"True","lastTransitionTime":"2026-11-01T08:00:40Z"},{"type":"ContainersReady","status":"True","lastTransitionTime":"2026-11-01T08:00:40Z"},{"type":"PodScheduled","status":"True","lastTransitionTime":"2026-11-01T08:00:05Z"}],
"containerStatuses":[{"name":"app","ready":true,"started":true,"restartCount":%[9]d,"image":"registry.example.com/platform/%[2]s:v1.42.7-20261101.3","imageID":"registry.example.com/platform/%[2]s@sha256:5b8a2c0d7e1f4a3b9c6d8e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d","containerID":"containerd://0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","state":{"running":{"startedAt":"2026-11-01T08:00:08Z"}},"lastState":{}},
{"name":"log-agent","ready":true,"started":true,"restartCount":0,"image":"registry.example.com/infra/fluent-bit:3.2.1","imageID":"registry.example.com/infra/fluent-bit@sha256:aa8a2c0d7e1f4a3b9c6d8e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d","containerID":"containerd://1f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0","state":{"running":{"startedAt":"2026-11-01T08:00:09Z"}},"lastState":{}}]}}`

// scaleFlag 打开耗时的规模测试。
var scaleFlag = flag.Bool("scale", false, "run the scale memory-model test")

func scalePod(tb testing.TB, i int) *unstructured.Unstructured {
	app := fmt.Sprintf("svc-%04d", i%2500)
	raw := fmt.Sprintf(scalePodJSON, fmt.Sprintf("%s-7d9f8c6b5-%05x", app, i), app, fmt.Sprintf("scale-ns-%03d", i%200),
		fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i), fmt.Sprintf("kwok-node-%04d", (i*31)%1200), i>>8&255, i&255, 128+i>>16&63, i%17)
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(raw), &u.Object); err != nil {
		tb.Fatal(err)
	}
	return u
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// fillPodStore 经与 reflector 相同的入口（Add → 转换 → 落入 entryStore）装入 n 个 Pod。
func fillPodStore(tb testing.TB, n int) (*entryStore, int) {
	store := newEntryStore(k8scontract.GVRPod)
	rawBytes := 0
	for i := range n {
		u := scalePod(tb, i)
		if i == 0 {
			raw, _ := json.Marshal(u.Object)
			rawBytes = len(raw)
		}
		if err := store.Add(u); err != nil {
			tb.Fatal(err)
		}
	}
	return store, rawBytes
}

// TestCacheMemoryModel 规模验证的内存模型（docs/k8s-scale-report.md）：5 万个 Pod 装入缓存后每个条目占多少堆内存。
// 缺省跳过（耗时约 10 秒）：go test ./internal/data/kube -run TestCacheMemoryModel -scale -v
func TestCacheMemoryModel(t *testing.T) {
	if !*scaleFlag {
		t.Skip("pass -scale to run")
	}
	for _, n := range []int{5000, 20000, 50000} {
		before := liveHeap()
		store, rawBytes := fillPodStore(t, n)
		after := liveHeap()
		per := float64(after-before) / float64(n)
		t.Logf("pods=%d  object JSON=%d B  cached heap=%.1f MiB  per pod=%.0f B (%.0f%% of the object)", n, rawBytes, float64(after-before)/(1<<20), per, per*100/float64(rawBytes))
		runtime.KeepAlive(store)
	}
}

// BenchmarkListCached50k 5 万个 Pod 上一次完整的缓存列表：快照 + 筛选排序分页 + 取行 + JSON 编码
// （不含权限询问与网络）。
func BenchmarkListCached50k(b *testing.B) {
	store, _ := fillPodStore(b, 50000)
	cases := []struct {
		name string
		ns   []string
		q    biz.ListQuery
	}{
		{"all-ns/default", nil, biz.ListQuery{Page: 1, PageSize: 20}},
		{"all-ns/page-size-200", nil, biz.ListQuery{Page: 1, PageSize: 200}},
		{"all-ns/keyword", nil, biz.ListQuery{Keyword: "svc-00", Page: 1, PageSize: 20}},
		{"all-ns/sort-node", nil, biz.ListQuery{SortBy: "node", Page: 1, PageSize: 20}},
		{"all-ns/sort-restarts-desc", nil, biz.ListQuery{SortBy: "restarts", SortDesc: true, Page: 1, PageSize: 20}},
		{"one-ns/default", []string{"scale-ns-007"}, biz.ListQuery{Page: 1, PageSize: 20}},
		{"five-ns/sort-node", []string{"scale-ns-000", "scale-ns-007", "scale-ns-014", "scale-ns-021", "scale-ns-028"}, biz.ListQuery{SortBy: "node", Page: 1, PageSize: 20}},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				page, total := biz.SelectEntries(store.snapshot(c.ns), c.q)
				out := k8scontract.ListResponse{Total: int64(total), Rows: make([]k8scontract.Row, 0, len(page))}
				for _, e := range page {
					out.Rows = append(out.Rows, e.Row)
				}
				if _, err := json.Marshal(out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkCacheUpdate 一次 Pod 更新事件进入缓存的成本（转换 + 落库 + 通知 200 个订阅者）。
func BenchmarkCacheUpdate(b *testing.B) {
	store, _ := fillPodStore(b, 50000)
	for i := range 200 {
		ns := ""
		if i%2 == 1 {
			ns = fmt.Sprintf("scale-ns-%03d", i%200)
		}
		store.subscribe(ns, nil)
	}
	pods := make([]*unstructured.Unstructured, 1000)
	for i := range pods {
		pods[i] = scalePod(b, i*7)
	}
	b.ReportAllocs()
	i := 0
	start := time.Now()
	for b.Loop() {
		_ = store.Update(pods[i%len(pods)])
		i++
	}
	b.ReportMetric(float64(i)/time.Since(start).Seconds(), "updates/s")
}

// BenchmarkCacheIngest 首次同步时每个 Pod 在数据面一侧的成本：JSON 解码（含 managedFields，约 13KB）+ 转换 + 落入缓存。
// 5 万个 Pod 的冷启动里数据面自身的 CPU 时间 ≈ 该值 × 5 万。
func BenchmarkCacheIngest(b *testing.B) {
	u := scalePod(b, 1)
	// 真实对象带 managedFields（与规格几乎等长）：补上一份等量的内容，使解码量贴近真实。
	u.Object["metadata"].(map[string]any)["managedFields"] = []any{map[string]any{"manager": "kube-controller-manager", "operation": "Update", "fieldsV1": u.Object["spec"]}, map[string]any{"manager": "kubelet", "operation": "Update", "subresource": "status", "fieldsV1": u.Object["status"]}}
	raw, err := json.Marshal(u.Object)
	if err != nil {
		b.Fatal(err)
	}
	store := newEntryStore(k8scontract.GVRPod)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for b.Loop() {
		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(raw); err != nil {
			b.Fatal(err)
		}
		_ = store.Add(obj)
	}
}
