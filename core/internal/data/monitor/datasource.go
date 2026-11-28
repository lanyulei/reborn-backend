package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/monitor"
	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/kit/dbx"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/promclient"
	"github.com/lanyulei/reborn/backend/kit/secretbox"

	"github.com/jackc/pgx/v5"
)

var (
	_ biz.DatasourceReader = (*datasourceRepo)(nil)
	_ biz.DatasourceLookup = (*datasourceRepo)(nil)
	_ biz.DatasourceWriter = (*datasourceRepo)(nil)
	_ biz.SecretCipher     = (*SecretBox)(nil)
	_ biz.QueryClient      = (*QueryClient)(nil)
)

// partialWarning 数据源声明结果不完整（VictoriaMetrics 集群版部分存储节点不可用）时附加的警告。
const partialWarning = "数据源返回的是部分结果（部分存储节点不可用），数据可能不完整"

// maxDatasourceOptions 数据源选项列表的条数上限。
const maxDatasourceOptions = 500

// inUseListed 删除被拒时列出的引用规则名个数。
const inUseListed = 5

// datasourceColumns 数据源查询列序。凭据密文不读出，只取是否已设置；引用数只算明确指定它的
// 未删除规则（范围为「全部数据源」的规则不算）。
const datasourceColumns = `d.id::text, d.name, d.type, d.url, d.auth_type, d.username, d.secret IS NOT NULL, d.headers,
	d.timeout_ms, d.tls_skip_verify, d.remote_write_url, d.description, d.is_default, d.enabled,
	d.health_status, d.health_error, d.health_time,
	(SELECT count(*) FROM monitor_rule r WHERE r.datasource_ids @> ARRAY[d.id] AND r.delete_time IS NULL),
	d.version, d.create_time, d.update_time, COALESCE(d.update_user::text, '')`

func scanDatasource(scan func(dest ...any) error) (*biz.Datasource, error) {
	var d biz.Datasource
	var headers []byte
	if err := scan(&d.ID, &d.Name, &d.Type, &d.URL, &d.AuthType, &d.Username, &d.HasSecret, &headers,
		&d.TimeoutMS, &d.TLSSkipVerify, &d.RemoteWriteURL, &d.Description, &d.IsDefault, &d.Enabled,
		&d.HealthStatus, &d.HealthError, &d.HealthTime, &d.RuleCount, &d.Version, &d.CreateTime, &d.UpdateTime, &d.UpdateUser); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(headers, &d.Headers); err != nil {
		return nil, fmt.Errorf("decode datasource headers: %w", err)
	}
	return &d, nil
}

type datasourceRepo struct {
	data *basedata.Data
}

// NewDatasourceRepo new a datasourceRepo.
func NewDatasourceRepo(data *basedata.Data) *datasourceRepo { return &datasourceRepo{data: data} }

// List implements biz.DatasourceReader.
func (r *datasourceRepo) List(ctx context.Context, filter biz.DatasourceFilter, page, pageSize int64) ([]*biz.Datasource, int64, error) {
	var a sqlArgs
	where := "d.delete_time IS NULL"
	if filter.Keyword != "" {
		p := a.add(dbx.LikeContains(filter.Keyword))
		where += " AND (d.name ILIKE " + p + " OR d.url ILIKE " + p + " OR d.description ILIKE " + p + ")"
	}
	if filter.Enabled != nil {
		where += " AND d.enabled = " + a.add(*filter.Enabled)
	}
	if filter.HealthStatus != nil {
		where += " AND d.health_status = " + a.add(*filter.HealthStatus)
	}
	var total int64
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM monitor_datasource d WHERE "+where, a.args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count monitor datasources: %w", err)
	}
	limit, offset := a.add(pageSize), a.add((page-1)*pageSize)
	rows, err := r.data.Pool.Query(ctx, "SELECT "+datasourceColumns+" FROM monitor_datasource d WHERE "+where+
		" ORDER BY d.is_default DESC, d.create_time DESC, d.id LIMIT "+limit+" OFFSET "+offset, a.args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list monitor datasources: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Datasource, 0, pageSize)
	for rows.Next() {
		d, err := scanDatasource(rows.Scan)
		if err != nil {
			return nil, 0, fmt.Errorf("scan monitor datasource: %w", err)
		}
		items = append(items, d)
	}
	return items, total, rows.Err()
}

// Get implements biz.DatasourceReader.
func (r *datasourceRepo) Get(ctx context.Context, id string) (*biz.Datasource, error) {
	d, err := scanDatasource(r.data.Pool.QueryRow(ctx,
		"SELECT "+datasourceColumns+" FROM monitor_datasource d WHERE d.id = $1 AND d.delete_time IS NULL", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrDatasourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor datasource: %w", err)
	}
	return d, nil
}

// ListOptions implements biz.DatasourceReader.
func (r *datasourceRepo) ListOptions(ctx context.Context, enabledOnly bool) ([]*biz.Datasource, error) {
	rows, err := r.data.Pool.Query(ctx,
		`SELECT id::text, name, is_default, enabled, health_status FROM monitor_datasource
		 WHERE delete_time IS NULL AND (enabled OR NOT $1) ORDER BY is_default DESC, name, id LIMIT $2`, enabledOnly, maxDatasourceOptions)
	if err != nil {
		return nil, fmt.Errorf("list monitor datasource options: %w", err)
	}
	defer rows.Close()
	items := make([]*biz.Datasource, 0, 16)
	for rows.Next() {
		var d biz.Datasource
		if err := rows.Scan(&d.ID, &d.Name, &d.IsDefault, &d.Enabled, &d.HealthStatus); err != nil {
			return nil, fmt.Errorf("scan monitor datasource option: %w", err)
		}
		items = append(items, &d)
	}
	return items, rows.Err()
}

// Briefs implements biz.DatasourceLookup.
func (r *datasourceRepo) Briefs(ctx context.Context, ids []string) (map[string]biz.DatasourceBrief, error) {
	out := make(map[string]biz.DatasourceBrief, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := r.data.Pool.Query(ctx,
		"SELECT id::text, name, enabled FROM monitor_datasource WHERE id = ANY($1::uuid[]) AND delete_time IS NULL", ids)
	if err != nil {
		return nil, fmt.Errorf("list monitor datasource briefs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var b biz.DatasourceBrief
		if err := rows.Scan(&id, &b.Name, &b.Enabled); err != nil {
			return nil, fmt.Errorf("scan monitor datasource brief: %w", err)
		}
		out[id] = b
	}
	return out, rows.Err()
}

// GetSecret implements biz.DatasourceLookup.
func (r *datasourceRepo) GetSecret(ctx context.Context, id string) ([]byte, error) {
	var sealed []byte
	err := r.data.Pool.QueryRow(ctx, "SELECT secret FROM monitor_datasource WHERE id = $1 AND delete_time IS NULL", id).Scan(&sealed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrDatasourceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get monitor datasource secret: %w", err)
	}
	return sealed, nil
}

// clearDefault 取消其他数据源的缺省标记。被取消的行版本 +1：正开着它的编辑表单的人保存时会得到
// 版本冲突，而不是把缺省标记悄悄抢回去。调用方已持有配置版本行的行锁，这里的「先清后设」不会并发。
func clearDefault(ctx context.Context, tx pgx.Tx, keepID, actor string) error {
	_, err := tx.Exec(ctx,
		`UPDATE monitor_datasource SET is_default = false, version = version + 1, update_time = now(), update_user = $2
		 WHERE is_default AND delete_time IS NULL AND id <> $1`, keepID, dbx.NilIfEmpty(actor))
	if err != nil {
		return fmt.Errorf("clear default monitor datasource: %w", err)
	}
	return nil
}

func encodeHeaders(headers map[string]string) ([]byte, error) {
	if headers == nil {
		headers = map[string]string{}
	}
	raw, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("encode datasource headers: %w", err)
	}
	return raw, nil
}

func datasourceWriteErr(err error, what string) error {
	if isUniqueViolation(err, "uk_monitor_datasource_name") {
		return biz.ErrDatasourceNameExists
	}
	return fmt.Errorf("%s monitor datasource: %w", what, err)
}

// Create implements biz.DatasourceWriter.
func (r *datasourceRepo) Create(ctx context.Context, d *biz.Datasource, sealed []byte, actor string) error {
	headers, err := encodeHeaders(d.Headers)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCfg(ctx, tx, mc.CfgDatasource); err != nil {
			return err
		}
		if d.IsDefault {
			if err := clearDefault(ctx, tx, d.ID, actor); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx,
			`INSERT INTO monitor_datasource (id, name, type, url, auth_type, username, secret, headers, timeout_ms, tls_skip_verify,
			   remote_write_url, description, is_default, enabled, create_user, update_user)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $15)`,
			d.ID, d.Name, d.Type, d.URL, d.AuthType, d.Username, dbx.NilIfEmptyBytes(sealed), headers, d.TimeoutMS, d.TLSSkipVerify,
			d.RemoteWriteURL, d.Description, d.IsDefault, d.Enabled, dbx.NilIfEmpty(actor))
		if err != nil {
			return datasourceWriteErr(err, "insert")
		}
		return bumpCfg(ctx, tx, mc.CfgDatasource)
	})
}

// Update implements biz.DatasourceWriter。type 不可改；健康状态三列归评估引擎，不在此写。
func (r *datasourceRepo) Update(ctx context.Context, d *biz.Datasource, secret biz.SecretChange, expectVersion int32, actor string) error {
	headers, err := encodeHeaders(d.Headers)
	if err != nil {
		return err
	}
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCfg(ctx, tx, mc.CfgDatasource); err != nil {
			return err
		}
		// 先行锁校验版本，再清其他数据源的缺省标记，最后写自己：顺序反过来的话，版本不一致的请求
		// 也会把别人的缺省标记清掉。
		var version int32
		err := tx.QueryRow(ctx, "SELECT version FROM monitor_datasource WHERE id = $1 AND delete_time IS NULL FOR UPDATE", d.ID).Scan(&version)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrDatasourceNotFound
		}
		if err != nil {
			return fmt.Errorf("lock monitor datasource: %w", err)
		}
		if version != expectVersion {
			return biz.ErrVersionConflict
		}
		// 清除 Remote Write 地址：仍有启用的记录规则经它写回时拒绝。记录规则的写事务对数据源行加共享锁
		// （lockRecordRefs），与这里持有的行锁互斥，所以这次检查之后不会再冒出新的启用规则。
		if d.RemoteWriteURL == "" {
			names, total, err := recordRulesUsing(ctx, tx, d.ID, true)
			if err != nil {
				return err
			}
			if total > 0 {
				return biz.DatasourceInUseByRecordRules(names, total)
			}
		}
		if d.IsDefault {
			if err := clearDefault(ctx, tx, d.ID, actor); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx,
			`UPDATE monitor_datasource SET name = $2, url = $3, auth_type = $4, username = $5,
			   secret = CASE WHEN $6 THEN $7::bytea ELSE secret END, headers = $8, timeout_ms = $9, tls_skip_verify = $10,
			   remote_write_url = $11, description = $12, is_default = $13, enabled = $14,
			   version = version + 1, update_time = now(), update_user = $15
			 WHERE id = $1`,
			d.ID, d.Name, d.URL, d.AuthType, d.Username, secret.Set, dbx.NilIfEmptyBytes(secret.Sealed), headers,
			d.TimeoutMS, d.TLSSkipVerify, d.RemoteWriteURL, d.Description, d.IsDefault, d.Enabled, dbx.NilIfEmpty(actor)); err != nil {
			return datasourceWriteErr(err, "update")
		}
		return bumpCfg(ctx, tx, mc.CfgDatasource)
	})
}

// Delete implements biz.DatasourceWriter。先行锁数据源行再查引用：规则 / 记录规则保存时对其指定的数据源行加
// 共享锁（lockDatasources / lockRecordRefs），与这里互斥。与 Create / Update 一样先锁配置版本行：加锁顺序不一致时，
// 并发的 Delete（数据源行 → 版本行）与 Update / 设为缺省的 Create（版本行 → 数据源行）会死锁。
func (r *datasourceRepo) Delete(ctx context.Context, id, actor string) error {
	return dbx.WithTx(ctx, r.data.Pool, func(tx pgx.Tx) error {
		if err := lockCfg(ctx, tx, mc.CfgDatasource); err != nil {
			return err
		}
		var locked string
		err := tx.QueryRow(ctx, "SELECT id::text FROM monitor_datasource WHERE id = $1 AND delete_time IS NULL FOR UPDATE", id).Scan(&locked)
		if errors.Is(err, pgx.ErrNoRows) {
			return biz.ErrDatasourceNotFound
		}
		if err != nil {
			return fmt.Errorf("lock monitor datasource: %w", err)
		}
		rows, err := tx.Query(ctx,
			`SELECT name, count(*) OVER () FROM monitor_rule
			 WHERE datasource_ids @> ARRAY[$1::uuid] AND delete_time IS NULL ORDER BY name LIMIT $2`, id, inUseListed)
		if err != nil {
			return fmt.Errorf("list rules referencing monitor datasource: %w", err)
		}
		var names []string
		var total int
		for rows.Next() {
			var name string
			if err := rows.Scan(&name, &total); err != nil {
				rows.Close()
				return fmt.Errorf("scan rule referencing monitor datasource: %w", err)
			}
			names = append(names, name)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("list rules referencing monitor datasource: %w", err)
		}
		if total > 0 {
			return biz.DatasourceInUse(names, total)
		}
		// 记录规则必须指定一个数据源：还有记录规则（不论启停）用着它就不能删。
		recordNames, recordTotal, err := recordRulesUsing(ctx, tx, id, false)
		if err != nil {
			return err
		}
		if recordTotal > 0 {
			return biz.DatasourceInUseByRecordRules(recordNames, recordTotal)
		}
		// 软删同时摘掉缺省标记并清除凭据密文：已删除的数据源不该再留着可解密的凭据。
		if _, err := tx.Exec(ctx,
			`UPDATE monitor_datasource SET delete_time = now(), delete_user = $2, is_default = false, secret = NULL, version = version + 1
			 WHERE id = $1`, id, dbx.NilIfEmpty(actor)); err != nil {
			return fmt.Errorf("delete monitor datasource: %w", err)
		}
		return bumpCfg(ctx, tx, mc.CfgDatasource)
	})
}

// lockDatasources 在写事务内确认这些数据源都存在并加共享锁（与数据源删除互斥）。
func lockDatasources(ctx context.Context, tx pgx.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx,
		"SELECT id::text FROM monitor_datasource WHERE id = ANY($1::uuid[]) AND delete_time IS NULL ORDER BY id FOR SHARE", ids)
	if err != nil {
		return fmt.Errorf("lock monitor datasources: %w", err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("lock monitor datasources: %w", err)
	}
	if n != len(ids) {
		return biz.ErrRuleDatasourceMissing
	}
	return nil
}

// SecretBox 数据源凭据加解密：kit/secretbox（AES-256-GCM），主密钥与通知渠道凭据相同
// （KRATOS_NOTIFY_MASTER_KEY）。core 需要解密：测试连接与即时查询由 core 直接访问数据源。
type SecretBox struct {
	box *secretbox.Box
}

// NewSecretBox 由启动配置构造；主密钥未配置时构造成功，Seal / Open 返回 ErrSecretUnavailable。
func NewSecretBox(c *conf.Notify) (*SecretBox, error) {
	box, err := secretbox.New(c.GetMaster().GetKey())
	if err != nil {
		return nil, err
	}
	return &SecretBox{box: box}, nil
}

// Seal implements biz.SecretCipher.
func (b *SecretBox) Seal(plaintext string) ([]byte, error) {
	out, err := b.box.Seal(plaintext)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return nil, biz.ErrSecretUnavailable
	}
	return out, err
}

// Open implements biz.SecretCipher.
func (b *SecretBox) Open(sealed []byte) (string, error) {
	out, err := b.box.Open(sealed)
	if errors.Is(err, secretbox.ErrUnavailable) {
		return "", biz.ErrSecretUnavailable
	}
	if err != nil {
		// 主密钥已更换或密文损坏：不向调用方暴露细节。
		return "", fmt.Errorf("open monitor datasource secret: %w", err)
	}
	return out, nil
}

// QueryClient 以 kit/promclient 访问数据源：超时、响应体上限、不跟随跳转、凭据不进错误信息都由
// promclient 保证。每次调用新建客户端并在返回前释放连接——只用于人工触发的测试连接与即时查询，
// 并发由用例层限制。
//
// 出站地址限制（数据源指向内网是常态，不拦私网）：链路本地 / 云元数据地址恒拒绝，回环地址按配置
// monitor.block_loopback 拒绝。判定在每次建立连接时对解析出的真实地址进行（promclient 的
// DialGuard，免疫 DNS 重绑定）；地址字面量另在 Validate 里先查一遍，保存配置时就给出明确错误。
// 地址清单只有 promclient.GuardLinkLocal 一份，评估引擎用的是同一个。
type QueryClient struct {
	blockLoopback bool
}

// NewQueryClient new a QueryClient.
func NewQueryClient(c *conf.Monitor) *QueryClient {
	return &QueryClient{blockLoopback: c.GetBlockLoopback()}
}

// errLoopback 回环地址被配置禁止。
var errLoopback = errors.New("loopback address is not allowed by configuration (monitor.block_loopback)")

// guard 连接前的地址检查（promclient.Config.DialGuard）。
func (q *QueryClient) guard(ip net.IP) error {
	if err := promclient.GuardLinkLocal(ip); err != nil {
		return err
	}
	if q.blockLoopback && ip.IsLoopback() {
		return errLoopback
	}
	return nil
}

// checkLiteralHost 地址里的主机是字面 IP（或 localhost）时直接判定，不等到连接时。主机名不在这里解析。
func (q *QueryClient) checkLiteralHost(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil // 地址格式由 promclient.New 判定
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if q.blockLoopback && (host == "localhost" || strings.HasSuffix(host, ".localhost")) {
		return biz.DatasourceAddressBlocked(errLoopback.Error())
	}
	if ip := net.ParseIP(host); ip != nil {
		if err := q.guard(ip); err != nil {
			return biz.DatasourceAddressBlocked(err.Error())
		}
	}
	return nil
}

func (q *QueryClient) newPromClient(conn biz.Connection, maxSeries int) (*promclient.Client, error) {
	c, err := promclient.New(promclient.Config{
		BaseURL: conn.URL, AuthType: conn.AuthType, Username: conn.Username, Secret: conn.Secret, Headers: conn.Headers,
		Timeout: time.Duration(conn.TimeoutMS) * time.Millisecond, TLSSkipVerify: conn.TLSSkipVerify, MaxSeries: maxSeries,
		DialGuard: q.guard,
	})
	if err != nil {
		// promclient 的配置错误说明是固定文案（不回显地址与凭据）。
		return nil, biz.DatasourceInvalid("%s", err.Error())
	}
	if err := q.checkLiteralHost(conn.URL); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

// Validate implements biz.QueryClient.
func (q *QueryClient) Validate(conn biz.Connection) error {
	c, err := q.newPromClient(conn, 0)
	if err != nil {
		return err
	}
	c.Close()
	return nil
}

// Instant implements biz.QueryClient.
func (q *QueryClient) Instant(ctx context.Context, conn biz.Connection, expr string, ts time.Time, maxSeries int) (*biz.QueryResult, error) {
	c, err := q.newPromClient(conn, maxSeries)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	res, err := c.Query(ctx, expr, ts)
	return queryResult(res, err)
}

// Range implements biz.QueryClient.
func (q *QueryClient) Range(ctx context.Context, conn biz.Connection, expr string, start, end time.Time, step time.Duration, maxSeries int) (*biz.QueryResult, error) {
	c, err := q.newPromClient(conn, maxSeries)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	res, err := c.QueryRange(ctx, expr, start, end, step)
	return queryResult(res, err)
}

// MetricNames implements biz.QueryClient.
func (q *QueryClient) MetricNames(ctx context.Context, conn biz.Connection, limit int) ([]string, error) {
	c, err := q.newPromClient(conn, 0)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.MetricNames(ctx, limit)
}

// LabelNames implements biz.QueryClient.
func (q *QueryClient) LabelNames(ctx context.Context, conn biz.Connection, limit int) ([]string, error) {
	c, err := q.newPromClient(conn, 0)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.LabelNames(ctx, limit)
}

// LabelValues implements biz.QueryClient.
func (q *QueryClient) LabelValues(ctx context.Context, conn biz.Connection, label string, limit int) ([]string, error) {
	c, err := q.newPromClient(conn, 0)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.LabelValues(ctx, label, limit)
}

func queryResult(res *promclient.Result, err error) (*biz.QueryResult, error) {
	if err != nil {
		var pe *promclient.Error
		switch {
		case !errors.As(err, &pe):
			return nil, biz.QueryFailed(false, "query failed")
		case pe.ErrorType == promclient.ErrTypeBlocked:
			return nil, biz.DatasourceAddressBlocked(pe.Message)
		}
		return nil, biz.QueryFailed(pe.BadQuery(), pe.ErrorType+": "+pe.Message)
	}
	out := &biz.QueryResult{Type: res.Type, Truncated: res.Truncated, Warnings: res.Warnings, Series: make([]biz.QuerySeries, 0, len(res.Series))}
	if res.Partial {
		// 接口契约里没有单独的「部分结果」字段：并入 warnings，前端已有的警告展示即可提示用户。
		out.Warnings = append(out.Warnings, partialWarning)
	}
	for _, s := range res.Series {
		points := make([]biz.QueryPoint, 0, len(s.Points))
		for _, p := range s.Points {
			points = append(points, biz.QueryPoint{Time: p.Time, Value: p.Value})
		}
		out.Series = append(out.Series, biz.QuerySeries{Labels: s.Labels, Points: points})
	}
	return out, nil
}
