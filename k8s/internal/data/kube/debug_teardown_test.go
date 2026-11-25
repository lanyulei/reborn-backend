package kube

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	"github.com/coder/websocket"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// debugFixture 假集群：Pod web 接受临时容器并立刻报告它在运行；终端的 exec（tty=true）一律 403，
// 其余 exec（拆除调试容器的那一次）接受升级并立即以 Success 结束。
type debugFixture struct {
	mu    sync.Mutex
	name  string     // 已创建的临时容器名
	kills [][]string // 每一次非终端 exec：[container, command...]
}

func (f *debugFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const podPath = "/api/v1/namespaces/a/pods/web"
	pod := corev1.Pod{
		TypeMeta:   metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "a", UID: "u-1"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx"}}},
	}
	switch {
	case r.Method == http.MethodGet && r.URL.Path == podPath:
		f.mu.Lock()
		if f.name != "" {
			pod.Status.EphemeralContainerStatuses = []corev1.ContainerStatus{{
				Name: f.name, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}}
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(pod)
	case r.Method == http.MethodPut && r.URL.Path == podPath+"/ephemeralcontainers":
		var in corev1.Pod
		if err := decodeClusterBody(r, &in); err != nil || len(in.Spec.EphemeralContainers) != 1 {
			http.Error(w, "bad ephemeral containers", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.name = in.Spec.EphemeralContainers[0].Name
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(in)
	case r.Method == http.MethodGet && r.URL.Path == podPath+"/exec":
		q := r.URL.Query()
		if q.Get("tty") == "true" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"pods/exec is forbidden"}`)
			return
		}
		f.mu.Lock()
		f.kills = append(f.kills, append([]string{q.Get("container")}, q["command"]...))
		f.mu.Unlock()
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{execProtocol}})
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		_ = c.Write(ctx, websocket.MessageBinary, append([]byte{chError}, `{"status":"Success"}`...))
	default:
		clusterNotFound(w)
	}
}

func (f *debugFixture) snapshot() (string, [][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.name, append([][]string(nil), f.kills...)
}

// 调试容器已经启动、随后终端被集群拒绝：必须把调试容器拆掉（对它再 exec 一次 kill），
// 否则它空转到会话时长上限。返回给调用方的仍是终端失败的原因。
func TestOpenDebugTearsDownContainerWhenTerminalFails(t *testing.T) {
	t.Parallel()
	f := &debugFixture{}
	u := fakeClusterUser(t, f)
	_, err := u.c.m.Open(context.Background(), &biz.Cluster{ID: "fake"}, u.id, k8scontract.ExecParams{
		Namespace: "a", Pod: "web", Kind: k8scontract.ExecKindDebug,
	})
	var be *biz.Error
	if err == nil || !errors.As(err, &be) || be.Code != http.StatusForbidden {
		t.Fatalf("the terminal failure must be reported as is (403): %v", err)
	}
	name, kills := f.snapshot()
	if name == "" {
		t.Fatal("the debug container was never created")
	}
	if len(kills) != 1 {
		t.Fatalf("exactly one teardown exec expected, got %d: %v", len(kills), kills)
	}
	if kills[0][0] != name {
		t.Fatalf("teardown must target the debug container %q, got %q", name, kills[0][0])
	}
	if cmd := strings.Join(kills[0][1:], " "); !strings.Contains(cmd, "kill -TERM") || !strings.Contains(cmd, debugPIDFile) {
		t.Fatalf("teardown must end the debug container's main process: %q", cmd)
	}
}

// 普通终端失败：没有调试容器，不发任何多余的 exec。
func TestOpenShellFailureDoesNotExecAgain(t *testing.T) {
	t.Parallel()
	f := &debugFixture{}
	u := fakeClusterUser(t, f)
	if _, err := u.c.m.Open(context.Background(), &biz.Cluster{ID: "fake"}, u.id, k8scontract.ExecParams{Namespace: "a", Pod: "web"}); err == nil {
		t.Fatal("the terminal must fail (exec is forbidden)")
	}
	if name, kills := f.snapshot(); name != "" || len(kills) != 0 {
		t.Fatalf("a plain terminal must not create or tear down anything: name=%q kills=%v", name, kills)
	}
}

// 调试容器主进程的看门狗与终端脚本的约定：进程号写成功才装看门狗；看门狗只在没有终端接入（无标记）时
// 结束主进程自己（$$$$ → K8s 还原成 $$）；终端一接入先落标记。
func TestDebugScripts(t *testing.T) {
	t.Parallel()
	main := debugMainScript("/p", "/m", 300*time.Second)
	want := "if echo $$$$ > /p; then ( sleep 300; [ -e /m ] || kill -TERM $$$$ ) & fi; trap 'exit 0' TERM INT; sleep 28800 & wait"
	if main != want {
		t.Fatalf("main script:\n got %q\nwant %q", main, want)
	}
	shell := debugShellScript("/p", "/m")
	if touch, sh := strings.Index(shell, "touch /m"), strings.Index(shell, "bash"); touch < 0 || sh < 0 || touch > sh {
		t.Fatalf("the terminal must leave the marker before starting the shell: %q", shell)
	}
	if !strings.Contains(shell, `kill -TERM "$(cat /p 2>/dev/null)"`) || !strings.Contains(debugKillScript("/p"), `kill -TERM "$(cat /p 2>/dev/null)"`) {
		t.Fatal("both the terminal and the teardown must end the main process recorded in the pid file")
	}
	if debugAttachWindow <= debugStartTimeout+execDialTimeout {
		t.Fatal("the watchdog window must outlast container start + terminal handshake, or it would kill a terminal that is still connecting")
	}
}
