package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"time"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultDebugImage 未配置允许清单时的调试镜像。
const DefaultDebugImage = "docker.io/library/busybox:1.36"

const (
	// debugStartTimeout 等待调试容器启动（含拉镜像）的时限。
	debugStartTimeout = 90 * time.Second
	// maxEphemeralContainers 一个 Pod 上的临时容器上限：临时容器无法删除，避免无限累积。
	maxEphemeralContainers = 8
)

// debugCommand 调试容器的主进程：保持存活到会话时长上限，由终端 exec 进入；收到 TERM 立即退出。
// 它把自己的进程号写进 debugPIDFile，终端里的 shell 退出时据此结束它（见 debugShellCommand），
// 临时容器不会在会话结束后继续空转。
//
// 注意 K8s 会展开容器 command 里的 $(VAR) 并把 $$ 还原成 $：要让 shell 拿到 $$ 得写成 $$$$。
var debugCommand = []string{"/bin/sh", "-c", "echo $$$$ > " + debugPIDFile + "; trap 'exit 0' TERM INT; sleep 28800 & wait"}

const debugPIDFile = "/tmp/.reborn-debug.pid"

// debugShellCommand 调试容器里的终端：shell 退出后结束调试容器的主进程。
var debugShellCommand = []string{"/bin/sh", "-c", "TERM=xterm-256color; export TERM; if command -v bash >/dev/null 2>&1; then bash; else sh; fi; " +
	"kill -TERM \"$(cat " + debugPIDFile + " 2>/dev/null)\" 2>/dev/null; exit 0"}

// SetDebugImages 设置调试容器的镜像允许清单（空 = 只允许 DefaultDebugImage）。启动期调用。
func (m *Manager) SetDebugImages(images []string) {
	m.debugImages = slices.Clone(images)
}

func (m *Manager) debugImage(want string) (string, error) {
	allowed := m.debugImages
	if len(allowed) == 0 {
		allowed = []string{DefaultDebugImage}
	}
	if want == "" {
		return allowed[0], nil
	}
	if !slices.Contains(allowed, want) {
		return "", biz.ErrInvalid("debug image %q is not in the allow-list", want)
	}
	return want, nil
}

// startDebug 以用户身份在 Pod 里启动一个临时容器（等价于 kubectl debug --target），等它运行后返回其名称。
// 能否启动由集群判定（pods/ephemeralcontainers 的权限）。临时容器共享目标容器的进程命名空间。
func (u *userClients) startDebug(pod *corev1.Pod, p k8scontract.ExecParams) (string, error) {
	image, err := u.c.m.debugImage(p.DebugImage)
	if err != nil {
		return "", err
	}
	target := p.Container
	if target == "" && len(pod.Spec.Containers) > 0 {
		target = pod.Spec.Containers[0].Name
	}
	if !slices.ContainsFunc(pod.Spec.Containers, func(c corev1.Container) bool { return c.Name == target }) {
		return "", biz.ErrInvalid("container %q not found in pod", target)
	}
	if len(pod.Spec.EphemeralContainers) >= maxEphemeralContainers {
		return "", biz.ErrInvalid("pod already has %d debug containers; recreate the pod to clear them", len(pod.Spec.EphemeralContainers))
	}
	buf := make([]byte, 3)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	name := "debugger-" + hex.EncodeToString(buf)
	next := pod.DeepCopy()
	next.Spec.EphemeralContainers = append(next.Spec.EphemeralContainers, corev1.EphemeralContainer{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name: name, Image: image, Command: debugCommand, ImagePullPolicy: corev1.PullIfNotPresent,
			TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		},
		TargetContainerName: target,
	})
	pods := u.kube.CoreV1().Pods(pod.Namespace)
	if _, err := pods.UpdateEphemeralContainers(u.ctx, pod.Name, next, metav1.UpdateOptions{}); err != nil {
		return "", mapError(err)
	}
	ctx, cancel := context.WithTimeout(u.ctx, debugStartTimeout)
	defer cancel()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		cur, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return "", mapError(err)
		}
		for _, st := range cur.Status.EphemeralContainerStatuses {
			if st.Name != name {
				continue
			}
			switch {
			case st.State.Running != nil:
				return name, nil
			case st.State.Terminated != nil:
				return "", biz.ErrInvalid("debug container exited immediately: %s", st.State.Terminated.Reason)
			case st.State.Waiting != nil && (st.State.Waiting.Reason == "ErrImagePull" || st.State.Waiting.Reason == "ImagePullBackOff" ||
				st.State.Waiting.Reason == "InvalidImageName" || st.State.Waiting.Reason == "CreateContainerError"):
				return "", biz.ErrInvalid("debug container cannot start: %s: %s", st.State.Waiting.Reason, st.State.Waiting.Message)
			}
		}
		select {
		case <-ctx.Done():
			return "", biz.NewError(504, k8scontract.ReasonUpstream, "debug container did not start within %s", debugStartTimeout)
		case <-tick.C:
		}
	}
}
