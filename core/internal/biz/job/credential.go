package biz

import (
	"context"
	"strings"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

// CredentialUsecase 凭据用例。凭据是全局资源：谁能维护、谁能在模板里引用由接口授权决定
// （job.credential 菜单及其元素、模板编辑相关接口）。密文只写不读。
type CredentialUsecase struct {
	reader CredentialReader
	writer CredentialWriter
	sealer SecretSealer
	dir    Directory
}

// NewCredentialUsecase new a CredentialUsecase.
func NewCredentialUsecase(reader CredentialReader, writer CredentialWriter, sealer SecretSealer, dir Directory) *CredentialUsecase {
	return &CredentialUsecase{reader: reader, writer: writer, sealer: sealer, dir: dir}
}

// List 分页查询凭据。
func (uc *CredentialUsecase) List(ctx context.Context, filter CredentialFilter, page, pageSize int64) ([]*Credential, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, items)
}

// Get 查询凭据详情。
func (uc *CredentialUsecase) Get(ctx context.Context, id string) (*Credential, error) {
	c, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return c, uc.fillNames(ctx, []*Credential{c})
}

// ListOptions 编辑模板用的最小列表。
func (uc *CredentialUsecase) ListOptions(ctx context.Context) ([]*Credential, error) {
	return uc.reader.ListOptions(ctx)
}

func (uc *CredentialUsecase) fillNames(ctx context.Context, items []*Credential) error {
	ids := make([]string, 0, len(items))
	for _, c := range items {
		ids = append(ids, c.UpdateUser)
	}
	if ids = uniq(ids); len(ids) == 0 {
		return nil
	}
	names, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, c := range items {
		c.UpdateUserName = names[c.UpdateUser]
	}
	return nil
}

// normalize 规整并校验入参（不含密文）。
func (in *CredentialInput) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	in.Username = strings.TrimSpace(in.Username)
	in.BecomeUser = strings.TrimSpace(in.BecomeUser)
	in.Description = strings.TrimSpace(in.Description)
	switch {
	case in.Name == "":
		return detail(ErrCredentialInvalid, "name must not be blank")
	case in.Type != jc.CredentialSSHKey && in.Type != jc.CredentialSSHPassword:
		return detail(ErrCredentialInvalid, "unknown credential type")
	case in.Username == "" || strings.HasPrefix(in.Username, "-"):
		return detail(ErrCredentialInvalid, "username must not be blank or start with a hyphen")
	case !jc.ValidBecomeMethod(in.BecomeMethod):
		return detail(ErrCredentialInvalid, "unknown become method")
	case strings.HasPrefix(in.BecomeUser, "-"):
		return detail(ErrCredentialInvalid, "become user must not start with a hyphen")
	}
	if in.BecomeMethod == "" {
		// 不提权：提权用户与提权密码都没有意义，一并清掉。
		in.BecomeUser = ""
		empty := ""
		in.BecomeSecret = &empty
	}
	return nil
}

// sealSecret 校验并加密主密文（私钥或密码），返回密文与指纹。
func (uc *CredentialUsecase) sealSecret(typ int32, secret string) (sealed []byte, fingerprint string, err error) {
	if typ == jc.CredentialSSHKey {
		key, fp, err := normalizePrivateKey(secret)
		if err != nil {
			return nil, "", detail(ErrCredentialInvalid, "%s", err.Error())
		}
		secret, fingerprint = key, fp
	} else if err := checkPassword(secret, "password"); err != nil {
		return nil, "", detail(ErrCredentialInvalid, "%s", err.Error())
	}
	sealed, err = uc.sealer.Seal(secret)
	return sealed, fingerprint, err
}

// sealBecome 提权密码的变更：nil 保留，空串清除，其余校验后加密。
func (uc *CredentialUsecase) sealBecome(secret *string) (SecretChange, error) {
	switch {
	case secret == nil:
		return SecretChange{}, nil
	case *secret == "":
		return SecretChange{Set: true}, nil
	}
	if err := checkPassword(*secret, "become password"); err != nil {
		return SecretChange{}, detail(ErrCredentialInvalid, "%s", err.Error())
	}
	sealed, err := uc.sealer.Seal(*secret)
	return SecretChange{Set: true, Sealed: sealed}, err
}

func (in *CredentialInput) apply(c *Credential) {
	c.Name, c.Type, c.Username = in.Name, in.Type, in.Username
	c.BecomeMethod, c.BecomeUser, c.Description = in.BecomeMethod, in.BecomeUser, in.Description
}

// Create 新建凭据。主密钥未配置时返回 ErrSecretUnavailable。
func (uc *CredentialUsecase) Create(ctx context.Context, userID string, in CredentialInput) (*Credential, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	if in.Secret == nil {
		return nil, detail(ErrCredentialInvalid, "private key or password is required")
	}
	sealed, fingerprint, err := uc.sealSecret(in.Type, *in.Secret)
	if err != nil {
		return nil, err
	}
	become, err := uc.sealBecome(in.BecomeSecret)
	if err != nil {
		return nil, err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	c := &Credential{ID: id, Fingerprint: fingerprint}
	in.apply(c)
	if err := uc.writer.Create(ctx, c, sealed, become.Sealed, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Update 编辑凭据（整体替换）。不传密文 = 保留已保存的；更换类型时必须重新填写密文
// （私钥与密码不可互相沿用）。
func (uc *CredentialUsecase) Update(ctx context.Context, userID, id string, in CredentialInput, version int32) (*Credential, error) {
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := in.normalize(); err != nil {
		return nil, err
	}
	c := &Credential{ID: id, Fingerprint: cur.Fingerprint}
	in.apply(c)
	var secret SecretChange
	switch {
	case in.Secret != nil:
		sealed, fingerprint, err := uc.sealSecret(in.Type, *in.Secret)
		if err != nil {
			return nil, err
		}
		secret, c.Fingerprint = SecretChange{Set: true, Sealed: sealed}, fingerprint
	case in.Type != cur.Type:
		return nil, detail(ErrCredentialInvalid, "changing the credential type requires a new private key or password")
	}
	become, err := uc.sealBecome(in.BecomeSecret)
	if err != nil {
		return nil, err
	}
	if err := uc.writer.Update(ctx, c, secret, become, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Delete 删除凭据；仍被作业模板使用时拒绝。
func (uc *CredentialUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.writer.Delete(ctx, id, userID)
}
