package data

import (
	"context"
	"errors"
	"fmt"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/job"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.CredentialReader = (*credentialRepo)(nil)
	_ biz.CredentialWriter = (*credentialRepo)(nil)
)

// credentialColumns 凭据查询列序。密文不读出，只取提权密码是否已设置。
//
//nolint:gosec // G101 误报：这是查询列清单，不含任何凭据
const credentialColumns = `c.id::text, c.name, c.type, c.username, c.become_method, c.become_user, c.become_secret IS NOT NULL,
	c.fingerprint, c.description,
	(SELECT count(*) FROM job_template t WHERE t.credential_id = c.id AND t.delete_time IS NULL),
	c.version, c.create_time, c.update_time, COALESCE(c.update_user::text, '')`

func scanCredential(scan func(dest ...any) error) (*biz.Credential, error) {
	var c biz.Credential
	if err := scan(&c.ID, &c.Name, &c.Type, &c.Username, &c.BecomeMethod, &c.BecomeUser, &c.HasBecomeSecret,
		&c.Fingerprint, &c.Description, &c.TemplateCount, &c.Version, &c.CreateTime, &c.UpdateTime, &c.UpdateUser); err != nil {
		return nil, err
	}
	return &c, nil
}

type credentialRepo struct {
	data *basedata.Data
}

// NewCredentialRepo new a credentialRepo.
func NewCredentialRepo(data *basedata.Data) *credentialRepo { return &credentialRepo{data: data} }

// List implements biz.CredentialReader.
func (r *credentialRepo) List(ctx context.Context, filter biz.CredentialFilter, page, pageSize int64) ([]*biz.Credential, int64, error) {
	var a sqlArgs
	where := "c.delete_time IS NULL"
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (c.name ILIKE " + p + " OR c.username ILIKE " + p + " OR c.description ILIKE " + p + ")"
	}
	if filter.Type != nil {
		where += " AND c.type = " + a.add(*filter.Type)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM job_credential c WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count job credentials: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+credentialColumns+" FROM job_credential c WHERE "+where+
		" ORDER BY c.create_time DESC, c.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list job credentials: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Credential, 0, pageSize)
	for rows.Next() {
		c, err := scanCredential(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan job credential: %w", err)
		}
		items = append(items, c)
	}
	return items, total, rows.Err()
}

// Get implements biz.CredentialReader.
func (r *credentialRepo) Get(ctx context.Context, id string) (*biz.Credential, error) {
	c, err := scanCredential(r.data.Pool.QueryRow(ctx,
		"SELECT "+credentialColumns+" FROM job_credential c WHERE c.id = $1 AND c.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrCredentialNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job credential: %w", err)
	}
	return c, nil
}

// ListOptions implements biz.CredentialReader.
func (r *credentialRepo) ListOptions(ctx context.Context) ([]*biz.Credential, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, name, type, username, become_method FROM job_credential
		 WHERE delete_time IS NULL ORDER BY name, id LIMIT $1`, maxOptions)
	if err != nil {
		return nil, fmt.Errorf("list job credential options: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Credential, 0, 16)
	for rows.Next() {
		var c biz.Credential
		if err := rows.Scan(&c.ID, &c.Name, &c.Type, &c.Username, &c.BecomeMethod); err != nil {
			return nil, fmt.Errorf("scan job credential option: %w", err)
		}
		items = append(items, &c)
	}
	return items, rows.Err()
}

func credentialWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_job_credential_name") {
		return biz.ErrCredentialNameExists
	}
	return fmt.Errorf("%s job credential: %w", what, err)
}

// Create implements biz.CredentialWriter.
func (r *credentialRepo) Create(ctx context.Context, c *biz.Credential, secret, becomeSecret []byte, actor string) error {
	_, err := r.data.Pool.Exec(ctx,
		`INSERT INTO job_credential (id, name, type, username, secret, become_method, become_user, become_secret,
		   fingerprint, description, create_user, update_user)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
		c.ID, c.Name, c.Type, c.Username, secret, c.BecomeMethod, c.BecomeUser, dbx.NilIfEmptyBytes(becomeSecret),
		c.Fingerprint, c.Description, dbx.NilIfEmpty(actor))
	if err != nil {
		return credentialWriteErr(err, "insert")
	}
	return nil
}

// Update implements biz.CredentialWriter。主密文为 NOT NULL：secret.Set 为真时调用方保证 Sealed 非空。
func (r *credentialRepo) Update(ctx context.Context, c *biz.Credential, secret, becomeSecret biz.SecretChange, expectVersion int32, actor string) error {
	if secret.Set && len(secret.Sealed) == 0 {
		return errors.New("update job credential: empty secret")
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockVersion(ctx, tx, "job_credential", c.ID, expectVersion, biz.ErrCredentialNotFound); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE job_credential SET name = $2, type = $3, username = $4,
			   secret = CASE WHEN $5 THEN $6::bytea ELSE secret END, become_method = $7, become_user = $8,
			   become_secret = CASE WHEN $9 THEN $10::bytea ELSE become_secret END, fingerprint = $11, description = $12,
			   version = version + 1, update_time = now(), update_user = $13
			 WHERE id = $1`,
			c.ID, c.Name, c.Type, c.Username, secret.Set, dbx.NilIfEmptyBytes(secret.Sealed), c.BecomeMethod, c.BecomeUser,
			becomeSecret.Set, dbx.NilIfEmptyBytes(becomeSecret.Sealed), c.Fingerprint, c.Description, dbx.NilIfEmpty(actor)); err != nil {
			return credentialWriteErr(err, "update")
		}
		return nil
	})
}

// Delete implements biz.CredentialWriter。先行锁凭据行再查引用：模板保存时对其引用的凭据行加共享锁，
// 与这里互斥。主密文列为 NOT NULL，软删时以空字节覆盖：已删除的凭据不该再留着可解密的密文。
func (r *credentialRepo) Delete(ctx context.Context, id, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockForDelete(ctx, tx, "job_credential", id, biz.ErrCredentialNotFound); err != nil {
			return err
		}
		names, total, err := templatesUsing(ctx, tx, "credential_id", id)
		if err != nil {
			return err
		}
		if total > 0 {
			return biz.InUse(biz.ErrCredentialInUse, names, total)
		}
		if _, err := tx.Exec(ctx,
			`UPDATE job_credential SET delete_time = now(), delete_user = $2, secret = ''::bytea, become_secret = NULL,
			   version = version + 1 WHERE id = $1`, id, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("delete job credential: %w", err)
		}
		return nil
	})
}
