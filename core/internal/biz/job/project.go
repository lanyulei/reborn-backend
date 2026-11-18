package biz

import (
	"context"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// defaultGitUsername 令牌认证未填用户名时的缺省值（多数 Git 服务只校验令牌）。
const defaultGitUsername = "git"

// ProjectUsecase 项目用例。项目是全局资源，访问控制同凭据。
type ProjectUsecase struct {
	reader ProjectReader
	writer ProjectWriter
	sealer SecretSealer
	dir    Directory
}

// NewProjectUsecase new a ProjectUsecase.
func NewProjectUsecase(reader ProjectReader, writer ProjectWriter, sealer SecretSealer, dir Directory) *ProjectUsecase {
	return &ProjectUsecase{reader: reader, writer: writer, sealer: sealer, dir: dir}
}

// List 分页查询项目。
func (uc *ProjectUsecase) List(ctx context.Context, keyword string, page, pageSize int64) ([]*Project, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	items, total, err := uc.reader.List(ctx, strings.TrimSpace(keyword), page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, items)
}

// Get 查询项目详情。
func (uc *ProjectUsecase) Get(ctx context.Context, id string) (*Project, error) {
	p, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return p, uc.fillNames(ctx, []*Project{p})
}

// ListOptions 编辑模板用的最小列表。
func (uc *ProjectUsecase) ListOptions(ctx context.Context) ([]*Project, error) {
	return uc.reader.ListOptions(ctx)
}

func (uc *ProjectUsecase) fillNames(ctx context.Context, items []*Project) error {
	ids := make([]string, 0, len(items))
	for _, p := range items {
		ids = append(ids, p.UpdateUser)
	}
	if ids = uniq(ids); len(ids) == 0 {
		return nil
	}
	names, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, p := range items {
		p.UpdateUserName = names[p.UpdateUser]
	}
	return nil
}

// normalize 规整并校验入参（不含密文）：仓库地址只接受 http(s) 与 ssh，认证方式须与地址的传输方式匹配。
func (in *ProjectInput) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	in.RepoURL = strings.TrimSpace(in.RepoURL)
	in.Ref = strings.TrimSpace(in.Ref)
	in.Username = strings.TrimSpace(in.Username)
	in.Description = strings.TrimSpace(in.Description)
	if in.Name == "" {
		return detail(ErrProjectInvalid, "name must not be blank")
	}
	scheme := jc.ParseRepoURL(in.RepoURL)
	if scheme == jc.RepoInvalid {
		return detail(ErrProjectInvalid, "repository url must be http(s)://host/path, ssh://user@host/path or user@host:path, without embedded credentials")
	}
	if !jc.ValidRef(in.Ref) {
		return detail(ErrProjectInvalid, "invalid branch or tag name")
	}
	switch in.AuthType {
	case jc.GitAuthNone:
		in.Username = ""
	case jc.GitAuthToken:
		if scheme != jc.RepoHTTP {
			return detail(ErrProjectInvalid, "token authentication requires an http(s) repository url")
		}
		if in.Username == "" {
			in.Username = defaultGitUsername
		}
		if strings.ContainsAny(in.Username, ":\r\n") {
			return detail(ErrProjectInvalid, "username must not contain a colon or line breaks")
		}
	case jc.GitAuthSSHKey:
		if scheme != jc.RepoSSH {
			return detail(ErrProjectInvalid, "ssh key authentication requires an ssh repository url")
		}
		in.Username = ""
	default:
		return detail(ErrProjectInvalid, "unknown authentication type")
	}
	return nil
}

// sealSecret 校验并加密仓库凭据。
func (uc *ProjectUsecase) sealSecret(authType int32, secret string) ([]byte, error) {
	if authType == jc.GitAuthSSHKey {
		key, _, err := normalizePrivateKey(secret)
		if err != nil {
			return nil, detail(ErrProjectInvalid, "%s", err.Error())
		}
		secret = key
	} else if err := checkPassword(secret, "token"); err != nil {
		return nil, detail(ErrProjectInvalid, "%s", err.Error())
	}
	return uc.sealer.Seal(secret)
}

func (in *ProjectInput) apply(p *Project) {
	p.Name, p.RepoURL, p.Ref = in.Name, in.RepoURL, in.Ref
	p.AuthType, p.Username, p.Description = in.AuthType, in.Username, in.Description
}

// Create 新建项目。
func (uc *ProjectUsecase) Create(ctx context.Context, userID string, in ProjectInput) (*Project, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	var sealed []byte
	if in.AuthType != jc.GitAuthNone {
		if in.Secret == nil {
			return nil, detail(ErrProjectInvalid, "token or private key is required for the selected authentication type")
		}
		var err error
		if sealed, err = uc.sealSecret(in.AuthType, *in.Secret); err != nil {
			return nil, err
		}
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	p := &Project{ID: id}
	in.apply(p)
	if err := uc.writer.Create(ctx, p, sealed, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Update 编辑项目（整体替换）。不传凭据 = 保留已保存的；更换认证方式时必须重新填写；
// 认证方式改为「无」时清除已保存的凭据。
func (uc *ProjectUsecase) Update(ctx context.Context, userID, id string, in ProjectInput, version int32) (*Project, error) {
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := in.normalize(); err != nil {
		return nil, err
	}
	var secret SecretChange
	switch {
	case in.AuthType == jc.GitAuthNone:
		secret = SecretChange{Set: true}
	case in.Secret != nil:
		sealed, err := uc.sealSecret(in.AuthType, *in.Secret)
		if err != nil {
			return nil, err
		}
		secret = SecretChange{Set: true, Sealed: sealed}
	case in.RepoURL != cur.RepoURL:
		// 换了仓库地址却沿用已保存的凭据 = 把令牌 / 私钥发给新地址：只会改地址的人不该拿得到它。
		return nil, detail(ErrProjectInvalid, "changing the repository url requires entering the token or private key again")
	case in.AuthType != cur.AuthType || !cur.HasSecret:
		return nil, detail(ErrProjectInvalid, "token or private key is required for the selected authentication type")
	}
	p := &Project{ID: id}
	in.apply(p)
	if err := uc.writer.Update(ctx, p, secret, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Delete 删除项目；仍被作业模板使用时拒绝。
func (uc *ProjectUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.writer.Delete(ctx, id, userID)
}
