package server

import (
	"net/http"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/conf"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"
	"github.com/lanyulei/reborn/backend/k8s/internal/service"
	"github.com/lanyulei/reborn/backend/kit/httpx"
	"github.com/lanyulei/reborn/backend/kit/k8scontract"
)

// GovSetup 治理类功能的配置已注入（调试镜像、文件传输上限、终端录制、定时巡检）；
// 作为依赖出现在 NewHTTPServer 的参数里，保证在接口对外之前生效。
type GovSetup struct{}

// NewGovSetup 把 plane 配置注入各用例；未设置或 ≤0 的项保持缺省值。
func NewGovSetup(c *conf.Plane, kubes *kube.Manager, files *biz.FileTransfer, exec *biz.ExecManager, inspect *biz.InspectionUsecase) GovSetup {
	kubes.SetDebugImages(c.GetDebugImages())
	files.WithMaxBytes(int64(c.GetMaxFileMb()) << 20)
	switch {
	case c.GetExecRecordDisabled():
		exec.WithRecording(0)
	case c.GetExecRecordMaxMb() > 0:
		exec.WithRecording(int64(c.GetExecRecordMaxMb()) << 20)
	default:
		exec.WithRecording(k8scontract.DefaultMaxRecordBytes)
	}
	inspect.WithInterval(c.GetInspectionInterval().AsDuration(), c.GetInspectionDisabled())
	return GovSetup{}
}

// limitBody 请求体上限：通用的 8MiB；文件上传路径除外（它按文件传输上限另行约束）。
func limitBody(next http.Handler) http.Handler {
	limited := httpx.LimitRequestBody(next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if service.UnlimitedBody(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		limited.ServeHTTP(w, r)
	})
}
