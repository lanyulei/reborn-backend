package kube

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strconv"
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
	// debugAttachWindow 调试容器启动后，等待终端接入的时限：超过而没有任何终端接入就自行退出。
	debugAttachWindow = 300 * time.Second
	// debugSessionLimit 调试容器的存活上限（会话最长时长）。
	debugSessionLimit = 8 * time.Hour
	// debugStopTimeout 终端没能建立时，结束调试容器主进程的那次 exec 的时限。
	debugStopTimeout = 5 * time.Second
)

const (
	debugPIDFile    = "/tmp/.reborn-debug.pid"
	debugMarkerFile = "/tmp/.reborn-debug.attached"
)

// debugMainScript 调试容器的主进程：保持存活到会话时长上限，由终端 exec 进入；收到 TERM 立即退出。
// 它把自己的进程号写进 pidFile，终端里的 shell 退出时据此结束它（见 debugShellScript）。
//
// 看门狗：启动后 window 内没有终端接入（没看到 marker）就结束自己。终端建不起来的时候——没有 pods/exec
// 权限、握手失败、等启动的 90 秒里客户端断开——平台侧的拆除（stopDebug）是尽力而为、可能做不成，
// 没有兜底的话临时容器会空转到会话时长上限，一直占着目标容器的 CPU / 内存配额。
// 只在 pidFile 写入成功时才装看门狗：写不进去说明容器的文件系统不可写，此时 marker 也写不了，
// 看门狗会误杀正在使用的终端——这种情况退回到「不装看门狗」（与改动前的行为一致）。
// 结束的是自己（$$ 在子 shell 里仍是主进程的 PID），不能是 1：临时容器共享目标容器的进程命名空间，
// 那里的 1 号是目标容器的主进程。
//
// 注意 K8s 会展开容器 command 里的 $(VAR) 并把 $$ 还原成 $：要让 shell 拿到 $$ 得写成 $$$$。
func debugMainScript(pidFile, marker string, window time.Duration) string {
	return "if echo $$$$ > " + pidFile + "; then ( sleep " + strconv.Itoa(int(window/time.Second)) +
		"; [ -e " + marker + " ] || kill -TERM $$$$ ) & fi;" +
		" trap 'exit 0' TERM INT; sleep " + strconv.Itoa(int(debugSessionLimit/time.Second)) + " & wait"
}

// debugShellScript 调试容器里的终端：记下「已有终端接入」（解除主进程的看门狗）；shell 退出后结束调试容器的主进程。
func debugShellScript(pidFile, marker string) string {
	return "TERM=xterm-256color; export TERM; touch " + marker + ";" +
		" if command -v bash >/dev/null 2>&1; then bash; else sh; fi;" +
		" kill -TERM \"$(cat " + pidFile + " 2>/dev/null)\" 2>/dev/null; exit 0"
}

// debugKillScript 终端没能建立时，结束调试容器主进程用的命令（以用户身份对调试容器再 exec 一次）。
func debugKillScript(pidFile string) string {
	return "kill -TERM \"$(cat " + pidFile + " 2>/dev/null)\" 2>/dev/null; exit 0"
}

var (
	debugCommand      = []string{"/bin/sh", "-c", debugMainScript(debugPIDFile, debugMarkerFile, debugAttachWindow)}
	debugShellCommand = []string{"/bin/sh", "-c", debugShellScript(debugPIDFile, debugMarkerFile)}
	debugKillCommand  = []string{"/bin/sh", "-c", debugKillScript(debugPIDFile)}
)

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
