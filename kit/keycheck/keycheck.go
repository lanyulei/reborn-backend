// Package keycheck 在启动时核对「各服务必须相同」的密钥是否真的相同。
//
// 做法：core 把每个密钥的指纹写进 system_key_check 表（首次写入者为准），其余服务启动时
// 读出来与本机配置的密钥比对，不一致直接启动失败——否则要等到运行期解密凭据、校验内部签名
// 时才以业务错误的形式暴露出来。指纹 = HMAC-SHA256(密钥, 固定标签) 的前 16 字节，不可逆。
//
// 轮换密钥：先停掉相关服务，删除 system_key_check 里对应的行，再用新密钥启动 core。
// 仅 cmd 层 import（含 pgx 驱动）。
package keycheck

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 密钥名（system_key_check.name）。
const (
	// Master 凭据加密主密钥（notify.master.key），全部服务相同。
	Master = "master"
	// K8sInternal core 与容器平台数据面之间的身份头签名密钥。
	K8sInternal = "k8s_internal"
)

// timeout 整个核对过程的时间上限。
const timeout = 5 * time.Second

// ErrMismatch 本机密钥与库里登记的指纹不一致。
var ErrMismatch = errors.New("keycheck: key does not match the fingerprint registered in system_key_check")

// Key 一个待核对的密钥。Secret 为空 = 本机未配置（对应能力停用），不参与核对。
type Key struct {
	Name   string
	Secret string
}

// 核对结果。
const (
	// StatusMatched 与登记的指纹一致。
	StatusMatched = "matched"
	// StatusRegistered 首次登记（仅 owner）。
	StatusRegistered = "registered"
	// StatusUnset 本机未配置该密钥。
	StatusUnset = "unset"
	// StatusUnregistered 库里还没有指纹（core 尚未以该密钥启动过），无从比对。
	StatusUnregistered = "unregistered"
	// StatusUnavailable 指纹表不可读（迁移未执行或账号无权限），跳过核对。
	StatusUnavailable = "unavailable"
)

// Result 单个密钥的核对结果。
type Result struct {
	Name   string
	Status string
}

// Fingerprint 密钥指纹（32 位十六进制）；name 参与计算，不同用途的密钥即使取值相同指纹也不同。
func Fingerprint(name, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("reborn/keycheck/v1/" + name))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// Run 用一条临时连接核对 keys。owner（core）负责登记：库里没有指纹时写入本机的；其余服务只读。
// 任一密钥不一致返回 ErrMismatch（包装了密钥名）。dsn 为空或没有已配置的密钥时不连接数据库。
func Run(ctx context.Context, dsn string, owner bool, keys ...Key) ([]Result, error) {
	results := make([]Result, 0, len(keys))
	pending := make([]Key, 0, len(keys))
	for _, k := range keys {
		if k.Secret == "" {
			results = append(results, Result{Name: k.Name, Status: StatusUnset})
			continue
		}
		pending = append(pending, k)
	}
	if dsn == "" || len(pending) == 0 {
		return results, nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("keycheck connect: %w", err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	for _, k := range pending {
		status, err := check(ctx, conn, owner, k)
		if err != nil {
			return nil, err
		}
		results = append(results, Result{Name: k.Name, Status: status})
	}
	return results, nil
}

func check(ctx context.Context, conn *pgx.Conn, owner bool, k Key) (string, error) {
	want := Fingerprint(k.Name, k.Secret)
	status := StatusMatched
	if owner {
		tag, err := conn.Exec(ctx,
			`INSERT INTO system_key_check (name, fingerprint) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`,
			k.Name, want)
		if err != nil {
			if unavailable(err) {
				return StatusUnavailable, nil
			}
			return "", fmt.Errorf("keycheck register %s: %w", k.Name, err)
		}
		if tag.RowsAffected() == 1 {
			status = StatusRegistered
		}
	}
	var got string
	err := conn.QueryRow(ctx, `SELECT fingerprint FROM system_key_check WHERE name = $1`, k.Name).Scan(&got)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return StatusUnregistered, nil
	case err != nil && unavailable(err):
		return StatusUnavailable, nil
	case err != nil:
		return "", fmt.Errorf("keycheck read %s: %w", k.Name, err)
	}
	if !hmac.Equal([]byte(got), []byte(want)) {
		return "", fmt.Errorf("%w: %s", ErrMismatch, k.Name)
	}
	return status, nil
}

// unavailable 指纹表不存在（迁移未执行）或当前账号无权访问：核对是附加保障，不因此阻断启动。
func unavailable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "42501")
}

// Startup 是各服务 main 的入口：执行核对并把每个密钥的结果写进日志；不一致时返回错误（调用方中止启动）。
func Startup(ctx context.Context, logger *slog.Logger, dsn string, owner bool, keys ...Key) error {
	results, err := Run(ctx, dsn, owner, keys...)
	if err != nil {
		return err
	}
	for _, r := range results {
		switch r.Status {
		case StatusUnregistered, StatusUnavailable:
			logger.Warn("shared key not verified", "key", r.Name, "status", r.Status)
		default:
			logger.Info("shared key checked", "key", r.Name, "status", r.Status)
		}
	}
	return nil
}
