package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/file"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"
	"github.com/lanyulei/reborn/backend/kit/dbx"

	"github.com/jackc/pgx/v5"
)

// 编译期断言锁住 biz 契约。
var (
	_ biz.Repo      = (*fileRepo)(nil)
	_ biz.BindRepo  = (*fileRepo)(nil)
	_ biz.PurgeRepo = (*fileRepo)(nil)
)

// fileRepo 是文件元数据仓储的 PostgreSQL 实现。
type fileRepo struct {
	data *basedata.Data
}

// NewFileRepo new a fileRepo.
func NewFileRepo(data *basedata.Data) *fileRepo {
	return &fileRepo{data: data}
}

// Create implements biz.Repo：单语句完成配额判定与写入。并发上传时计数可能略有超出
// （各语句各自的快照），配额是防滥用的软上限，不要求精确。
func (r *fileRepo) Create(ctx context.Context, f *biz.File, maxTemp int) error {
	tag, err := r.data.Pool.Exec(ctx,
		`INSERT INTO file_object (id, driver, object_key, filename, size, mime, sha256, status, create_user, create_time)
		 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		 WHERE (SELECT count(*) FROM file_object WHERE create_user = $9 AND status = 1 AND delete_time IS NULL) < $11`,
		f.ID, f.Driver, f.ObjectKey, f.Filename, f.Size, f.Mime, f.SHA256, f.Status, f.CreateUser, f.CreateTime, maxTemp)
	if err != nil {
		return fmt.Errorf("insert file: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return biz.ErrFileQuotaExceeded
	}
	return nil
}

// Discard implements biz.Repo：只删未绑定的临时行（已绑定的文件不受影响）。
func (r *fileRepo) Discard(ctx context.Context, id string) error {
	if _, err := r.data.Pool.Exec(ctx, "DELETE FROM file_object WHERE id = $1 AND status = 1", id); err != nil {
		return fmt.Errorf("discard file: %w", err)
	}
	return nil
}

// Get implements biz.Repo.
func (r *fileRepo) Get(ctx context.Context, id string) (*biz.File, error) {
	var (
		f              biz.File
		status         int16
		bizType, bizID *string
	)
	err := r.data.Pool.QueryRow(ctx,
		`SELECT id, driver, object_key, filename, size, mime, sha256, status, biz_type, biz_id, create_user, create_time
		 FROM file_object WHERE id = $1 AND delete_time IS NULL`, id).
		Scan(&f.ID, &f.Driver, &f.ObjectKey, &f.Filename, &f.Size, &f.Mime, &f.SHA256, &status, &bizType, &bizID, &f.CreateUser, &f.CreateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrFileNotFound
		}
		return nil, fmt.Errorf("get file: %w", err)
	}
	f.Status = int32(status)
	if bizType != nil {
		f.BizType = *bizType
	}
	if bizID != nil {
		f.BizID = *bizID
	}
	return &f, nil
}

// BindTx implements biz.BindRepo：单语句条件更新——只命中「临时 + 上传者匹配 + 未删除」
// 的行，重复绑定 / 绑定他人文件 / 已被清理的文件都不命中，由调用方按行数判定。
func (r *fileRepo) BindTx(ctx context.Context, tx txrun.Tx, uploader, bizType, bizID string, fileIDs []string) (int64, error) {
	n, err := tx.Exec(ctx,
		`UPDATE file_object SET status = 2, biz_type = $2, biz_id = $3, bind_time = now()
		 WHERE id IN (SELECT unnest($4::text[])::uuid) AND status = 1 AND create_user = $1 AND delete_time IS NULL`,
		uploader, bizType, bizID, fileIDs)
	if err != nil {
		return 0, fmt.Errorf("bind files: %w", err)
	}
	return n, nil
}

// ListByBiz implements biz.BindRepo（走 idx_file_object_biz）。
func (r *fileRepo) ListByBiz(ctx context.Context, bizType, bizID string, limit int) ([]*biz.File, error) {
	rows, err := basedata.QueryerFrom(ctx, r.data.Pool).Query(ctx,
		`SELECT id, filename, size, mime, create_user, create_time FROM file_object
		 WHERE biz_type = $1 AND biz_id = $2 AND status = 2 AND delete_time IS NULL ORDER BY id LIMIT $3`,
		bizType, bizID, limit)
	if err != nil {
		return nil, fmt.Errorf("list files by biz: %w", err)
	}
	files, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.File, error) {
		f := biz.File{Status: biz.StatusBound, BizType: bizType, BizID: bizID}
		return &f, row.Scan(&f.ID, &f.Filename, &f.Size, &f.Mime, &f.CreateUser, &f.CreateTime)
	})
	if err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}
	return files, nil
}

// ReleaseTx implements biz.BindRepo.
func (r *fileRepo) ReleaseTx(ctx context.Context, tx txrun.Tx, bizType, bizID string) error {
	if _, err := tx.Exec(ctx,
		`UPDATE file_object SET delete_time = now()
		 WHERE biz_type = $1 AND biz_id = $2 AND status = 2 AND delete_time IS NULL`, bizType, bizID); err != nil {
		return fmt.Errorf("release files: %w", err)
	}
	return nil
}

// PromoteSingleton implements biz.BindRepo：事务级咨询锁按业务对象串行——并发的两次替换
// 不会各自看不见对方而留下两份。
func (r *fileRepo) PromoteSingleton(ctx context.Context, id, bizType, bizID string, ifAbsent bool) (bool, error) {
	done := false
	err := pgx.BeginFunc(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2, 0))", bizType, bizID); err != nil {
			return fmt.Errorf("lock singleton file: %w", err)
		}
		const live = "biz_type = $1 AND biz_id = $2 AND status = 2 AND delete_time IS NULL"
		if ifAbsent {
			var exists bool
			if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM file_object WHERE "+live+")", bizType, bizID).Scan(&exists); err != nil {
				return fmt.Errorf("check singleton file: %w", err)
			}
			if exists {
				return nil
			}
		}
		if _, err := tx.Exec(ctx, "UPDATE file_object SET delete_time = now() WHERE "+live, bizType, bizID); err != nil {
			return fmt.Errorf("release singleton file: %w", err)
		}
		tag, err := tx.Exec(ctx,
			`UPDATE file_object SET status = 2, biz_type = $2, biz_id = $3, bind_time = now()
			 WHERE id = $1 AND status = 1 AND delete_time IS NULL`, id, bizType, bizID)
		if err != nil {
			return fmt.Errorf("promote singleton file: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return biz.ErrFileNotFound
		}
		done = true
		return nil
	})
	return done, err
}

// LatestByBiz implements biz.BindRepo（走 idx_file_object_biz；id 为 UUIDv7，倒序即最新）。
func (r *fileRepo) LatestByBiz(ctx context.Context, bizType, bizID string) (*biz.File, error) {
	f := biz.File{Status: biz.StatusBound, BizType: bizType, BizID: bizID}
	err := basedata.QueryerFrom(ctx, r.data.Pool).QueryRow(ctx,
		`SELECT id, driver, object_key, filename, size, mime, create_user, create_time FROM file_object
		 WHERE biz_type = $1 AND biz_id = $2 AND status = 2 AND delete_time IS NULL ORDER BY id DESC LIMIT 1`,
		bizType, bizID).Scan(&f.ID, &f.Driver, &f.ObjectKey, &f.Filename, &f.Size, &f.Mime, &f.CreateUser, &f.CreateTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, biz.ErrFileNotFound
		}
		return nil, fmt.Errorf("get latest file by biz: %w", err)
	}
	return &f, nil
}

// ReleaseFilesTx implements biz.BindRepo：业务类型与对象 ID 都在条件里——传入别的对象的文件 ID
// 不命中任何行。
func (r *fileRepo) ReleaseFilesTx(ctx context.Context, tx txrun.Tx, bizType, bizID string, fileIDs []string) error {
	if _, err := tx.Exec(ctx,
		`UPDATE file_object SET delete_time = now()
		 WHERE biz_type = $1 AND biz_id = $2 AND status = 2 AND delete_time IS NULL
		   AND id IN (SELECT unnest($3::text[])::uuid)`, bizType, bizID, fileIDs); err != nil {
		return fmt.Errorf("release files: %w", err)
	}
	return nil
}

// PurgeExpired implements biz.PurgeRepo：事务内以 SKIP LOCKED 领取一批（多实例各取
// 各的，互不阻塞），逐个删除对象，再物理删除对象已删掉的元数据行。删除对象失败的
// 文件保留元数据并记下失败时间，retryAfter 之内不再领取——否则一批坏行（驱动被下线、
// 权限错误）会每轮都排在最前，把后面的文件永远挡住。事务中途失败则整批回滚，下一轮
// 重试（对象删除幂等）。持有行锁期间与并发的绑定互斥：绑定方等待后重判为不命中。
func (r *fileRepo) PurgeExpired(ctx context.Context, tempBefore, deletedBefore time.Time, retryAfter time.Duration, limit int, remove func(ctx context.Context, f *biz.File) error) (int, error) {
	var (
		purged    int
		removeErr error
	)
	err := dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id, driver, object_key FROM file_object
			 WHERE ((status = 1 AND delete_time IS NULL AND create_time < $1)
			    OR (delete_time IS NOT NULL AND delete_time < $2))
			   AND (purge_time IS NULL OR purge_time < now() - make_interval(secs => $4))
			 ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED`, tempBefore, deletedBefore, limit, retryAfter.Seconds())
		if err != nil {
			return fmt.Errorf("claim expired files: %w", err)
		}
		files, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*biz.File, error) {
			var f biz.File
			return &f, row.Scan(&f.ID, &f.Driver, &f.ObjectKey)
		})
		if err != nil {
			return fmt.Errorf("scan expired files: %w", err)
		}
		done := make([]string, 0, len(files))
		var failed []string
		for _, f := range files {
			if err := remove(ctx, f); err != nil {
				removeErr = errors.Join(removeErr, fmt.Errorf("remove object of file %s: %w", f.ID, err))
				failed = append(failed, f.ID)
				continue
			}
			done = append(done, f.ID)
		}
		if len(failed) > 0 {
			if _, err := tx.Exec(ctx,
				"UPDATE file_object SET purge_time = now() WHERE id IN (SELECT unnest($1::text[])::uuid)", failed); err != nil {
				return fmt.Errorf("mark failed purges: %w", err)
			}
		}
		if len(done) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx,
			"DELETE FROM file_object WHERE id IN (SELECT unnest($1::text[])::uuid)", done); err != nil {
			return fmt.Errorf("delete expired files: %w", err)
		}
		purged = len(done)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return purged, removeErr
}
