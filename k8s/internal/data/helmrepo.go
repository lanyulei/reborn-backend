package data

import (
	"context"
	"errors"
	"fmt"

	"github.com/lanyulei/reborn/backend/k8s/internal/biz"
	"github.com/lanyulei/reborn/backend/k8s/internal/data/kube"

	"github.com/jackc/pgx/v5"
)

var _ kube.HelmRepoReader = (*clusterRepo)(nil)

// HelmRepo implements kube.HelmRepoReader：读取 Helm 仓库（含凭据密文，只在数据面解密）。
func (r *clusterRepo) HelmRepo(ctx context.Context, id string) (*kube.HelmRepo, error) {
	var h kube.HelmRepo
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id::text, name, type, url, username, secret, ca_cert, insecure_skip_verify, enabled, version
		 FROM k8s_helm_repo WHERE id = $1 AND delete_time IS NULL`, id).
		Scan(&h.ID, &h.Name, &h.Type, &h.URL, &h.Username, &h.Secret, &h.CACert, &h.Insecure, &h.Enabled, &h.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrNotFound("helm repository not found")
	}
	if err != nil {
		return nil, fmt.Errorf("get helm repo: %w", err)
	}
	return &h, nil
}
