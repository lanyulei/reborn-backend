package biz

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"

	kerrors "github.com/go-kratos/kratos/v3/errors"
)

// 即时查询的限额（出站请求安全，设计 §5.4）。
const (
	// maxQuerySeries 返回给前端的序列数上限，超出截断并标记。
	maxQuerySeries = 500
	// maxRangeSteps 区间查询的步数上限（(end-start)/step），超出直接拒绝。
	maxRangeSteps = 11000
	// maxQueryPoints 一次查询返回的采样点总数上限，超出截断并标记。
	maxQueryPoints = 200000
	// defaultCompletionLimit 补全接口（指标名 / 标签名 / 标签值）的缺省返回条数。
	defaultCompletionLimit = 200
	// maxCompletionLimit 补全接口允许的最大返回条数：超过即夹紧，防止一次性拉回超大标签集。
	maxCompletionLimit = 1000
	// maxConcurrentQueries 本进程同时在途的测试连接 / 即时查询数。这是实例的自我保护（防止慢
	// 数据源占满出站连接与内存），不是全局配额：N 个实例就是 N 份额度。
	maxConcurrentQueries = 8
	// placeholderSecret 校验「沿用已保存凭据」的配置时代入的占位值（只做格式校验，不发请求）。
	placeholderSecret = "placeholder"
)

// HeaderMask 响应里自定义请求头值的掩码：请求头的值可能是密钥（如 X-API-Key），所以只返回键名。
// 编辑 / 测试时把掩码原样传回表示沿用该键已保存的值（接口文档以字面量写明，前端依赖它）。
const HeaderMask = "******"

// DatasourceUsecase 数据源用例。数据源是全局资源，不按分组隔离：谁能维护、谁能查询由接口授权
// 决定（monitor.datasource / monitor.explore 及规则编辑相关接口）。
type DatasourceUsecase struct {
	reader DatasourceReader
	lookup DatasourceLookup
	writer DatasourceWriter
	cipher SecretCipher
	client QueryClient
	dir    Directory
	perms  Permissions
	// slots 并发额度（带缓冲的 channel 当信号量用；只保护本进程）。
	slots chan struct{}
	// writeURLs Remote Write 地址的校验（格式 + 出站地址策略）；nil 时只做格式校验（单元测试的装配）。
	writeURLs WriteURLValidator
}

// WriteURLValidator 校验 Remote Write 地址（data 层以 kit/promclient 实现，与查询地址同一份地址策略）。
type WriteURLValidator interface {
	// ValidateWriteURL 不发起网络请求。格式不合法返回 ErrDatasourceInvalid；地址字面量指向不允许访问的
	// 地址（链路本地 / 云元数据，按配置还有回环）返回 ErrDatasourceAddressBlocked。
	ValidateWriteURL(raw string) error
}

// ProvideDatasourceUsecase 装配用：在 NewDatasourceUsecase 之上接入 Remote Write 地址的校验。
func ProvideDatasourceUsecase(reader DatasourceReader, lookup DatasourceLookup, writer DatasourceWriter, cipher SecretCipher, client QueryClient,
	dir Directory, perms Permissions, writeURLs WriteURLValidator,
) *DatasourceUsecase {
	uc := NewDatasourceUsecase(reader, lookup, writer, cipher, client, dir, perms)
	uc.writeURLs = writeURLs
	return uc
}

// NewDatasourceUsecase new a DatasourceUsecase.
func NewDatasourceUsecase(reader DatasourceReader, lookup DatasourceLookup, writer DatasourceWriter, cipher SecretCipher, client QueryClient,
	dir Directory, perms Permissions,
) *DatasourceUsecase {
	return &DatasourceUsecase{
		reader: reader, lookup: lookup, writer: writer, cipher: cipher, client: client, dir: dir, perms: perms,
		slots: make(chan struct{}, maxConcurrentQueries),
	}
}

// List 分页查询数据源。
func (uc *DatasourceUsecase) List(ctx context.Context, filter DatasourceFilter, page, pageSize int64) ([]*Datasource, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.reader.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	for _, d := range items {
		d.Headers = maskHeaders(d.Headers)
	}
	return items, total, uc.fillNames(ctx, items)
}

// Get 查询数据源详情。请求头的值以 HeaderMask 返回——本用例对外的返回值一律如此，需要真实值的
// 内部路径（savedConnection）直接读仓储。
func (uc *DatasourceUsecase) Get(ctx context.Context, id string) (*Datasource, error) {
	d, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	d.Headers = maskHeaders(d.Headers)
	return d, uc.fillNames(ctx, []*Datasource{d})
}

// maskHeaders 返回只保留键名的副本。
func maskHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for k := range headers {
		out[k] = HeaderMask
	}
	return out
}

// restoreHeaders 把入参里值为 HeaderMask 的请求头换回已保存的值，返回新 map 与是否沿用了已保存的值。
// 掩码对应的键没有保存过（含新建，saved 为 nil）返回 ErrDatasourceSecretRequired。
func restoreHeaders(in, saved map[string]string) (map[string]string, bool, error) {
	out := make(map[string]string, len(in))
	reused := false
	for k, v := range in {
		if v == HeaderMask {
			old, ok := saved[k]
			if !ok {
				return nil, false, detail(ErrDatasourceSecretRequired, "header %q has no saved value, please enter it", k)
			}
			v, reused = old, true
		}
		out[k] = v
	}
	return out, reused, nil
}

// errOriginChanged 地址换了主机：已保存的凭据 / 请求头值只属于原来那个地址，不能带到别处。
var errOriginChanged = detail(ErrDatasourceSecretRequired,
	"the address differs from the saved one (scheme, host or port): please enter the credential and masked header values again")

// sameOrigin 两个查询地址的协议、主机、端口是否一致（缺省端口按协议补齐，主机不区分大小写）。
// 任一地址无法解析都算不一致。
func sameOrigin(a, b string) bool {
	ua, errA := url.Parse(strings.TrimSpace(a))
	ub, errB := url.Parse(strings.TrimSpace(b))
	if errA != nil || errB != nil || ua.Hostname() == "" {
		return false
	}
	return strings.EqualFold(ua.Scheme, ub.Scheme) && strings.EqualFold(ua.Hostname(), ub.Hostname()) && originPort(ua) == originPort(ub)
}

func originPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	if strings.EqualFold(u.Scheme, "https") {
		return "443"
	}
	return "80"
}

// ListOptions 建规则用的最小列表。
func (uc *DatasourceUsecase) ListOptions(ctx context.Context, enabledOnly bool) ([]*Datasource, error) {
	return uc.reader.ListOptions(ctx, enabledOnly)
}

func (uc *DatasourceUsecase) fillNames(ctx context.Context, items []*Datasource) error {
	ids := make([]string, 0, len(items))
	for _, d := range items {
		ids = append(ids, d.UpdateUser)
	}
	if ids = uniq(ids); len(ids) == 0 {
		return nil
	}
	userNames, err := uc.dir.UserNames(ctx, ids)
	if err != nil {
		return err
	}
	for _, d := range items {
		d.UpdateUserName = userNames[d.UpdateUser]
	}
	return nil
}

// normalize 规整并校验入参；hasSecret 表示保存后凭据是否存在（新传入的，或沿用已保存的）。
func (uc *DatasourceUsecase) normalize(in *DatasourceInput, hasSecret bool) error {
	in.Name = strings.TrimSpace(in.Name)
	in.Conn.URL = strings.TrimSpace(in.Conn.URL)
	in.Conn.Username = strings.TrimSpace(in.Conn.Username)
	in.Description = strings.TrimSpace(in.Description)
	in.RemoteWriteURL = strings.TrimSpace(in.RemoteWriteURL)
	if in.Name == "" {
		return detail(ErrDatasourceInvalid, "name must not be blank")
	}
	if in.Conn.TimeoutMS == 0 {
		in.Conn.TimeoutMS = 10000
	}
	if in.Conn.TimeoutMS < 1000 || in.Conn.TimeoutMS > 30000 {
		return detail(ErrDatasourceInvalid, "timeout_ms must be between 1000 and 30000")
	}
	if err := uc.checkRemoteWriteURL(in.RemoteWriteURL); err != nil {
		return err
	}
	if in.Conn.AuthType != mc.AuthBasic {
		in.Conn.Username = ""
	}
	if in.Conn.AuthType == mc.AuthBearer && !hasSecret {
		return detail(ErrDatasourceInvalid, "bearer auth requires a token")
	}
	conn := in.Conn
	if in.Secret != nil {
		conn.Secret = *in.Secret
	} else if hasSecret {
		conn.Secret = placeholderSecret
	}
	return uc.client.Validate(conn)
}

// checkRemoteWriteURL 校验 Remote Write 地址（记录规则经它写回，评估引擎会带着数据源的凭据与自定义请求头
// 访问它）。取定：
//   - 格式：不带凭据、不带 query / fragment 的 http(s) 地址。kit/promclient 的写入地址本身允许带 query，
//     这里从严——地址会原样落库并随详情接口返回，query 里夹带的令牌就成了明文存放、任何能看数据源的人
//     都读得到的凭据；需要给写回的序列加标签用记录规则的附加标签；
//   - 出站地址策略与查询地址相同（WriteURLValidator）：链路本地 / 云元数据地址的字面量保存即拒绝，回环
//     地址按 monitor.block_loopback；主机名解析出的真实地址由引擎在每次建立连接时检查。
func (uc *DatasourceUsecase) checkRemoteWriteURL(raw string) error {
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return detail(ErrDatasourceInvalid, "remote write url must be an http(s) address")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return detail(ErrDatasourceInvalid, "remote write url must not contain credentials, query or fragment")
	}
	if uc.writeURLs == nil {
		return nil
	}
	return uc.writeURLs.ValidateWriteURL(raw)
}

// errWriteOriginChanged 写入地址换到了另一台主机，而已保存的凭据 / 请求头值会随写入请求发往那里。
var errWriteOriginChanged = detail(ErrDatasourceSecretRequired,
	"the remote write url points to a different host than the query address and than the saved remote write url: "+
		"please enter the credential and masked header values again")

// writeOriginAllowed 沿用已保存的凭据 / 请求头值时，Remote Write 地址是否可以接受。
//
// 记录规则写回时，数据源的凭据与自定义请求头会发往写入地址的主机（kit/promclient.Write）。写入地址与查询
// 地址同源时没有新的去向；不同源时（集群版的 vminsert 与 vmselect 本就是两台主机）只有两种情况可以放行：
// 调用方在这次保存里重新填写了全部凭据（他本来就知道它们），或者写入地址的源与已保存的写入地址相同
// （去向没有变）。否则「只改写入地址、不重输密码」就能把已保存的密码发到调用方指定的任意主机——与查询
// 地址的同源规则（errOriginChanged）是同一个道理。新建数据源时凭据都是当场填写的，不受此限。
func writeOriginAllowed(writeURL, queryURL, savedWriteURL string) bool {
	return writeURL == "" || sameOrigin(writeURL, queryURL) || (savedWriteURL != "" && sameOrigin(writeURL, savedWriteURL))
}

// DatasourceInUseByRecordRules 数据源仍被记录规则引用（data 层在写事务内取到规则名）：删除数据源、或清除
// 仍有启用的记录规则在用的 Remote Write 地址时返回。
func DatasourceInUseByRecordRules(ruleNames []string, total int) error {
	return detail(ErrDatasourceInUse, "datasource is still used by %d record rule(s): %s", total, joinNames(ruleNames, total))
}

func (in *DatasourceInput) apply(d *Datasource) {
	d.Name, d.Type = in.Name, mc.DatasourceTypePrometheus
	d.URL, d.AuthType, d.Username = in.Conn.URL, in.Conn.AuthType, in.Conn.Username
	d.Headers, d.TimeoutMS, d.TLSSkipVerify = in.Conn.Headers, in.Conn.TimeoutMS, in.Conn.TLSSkipVerify
	d.RemoteWriteURL, d.Description, d.IsDefault, d.Enabled = in.RemoteWriteURL, in.Description, in.IsDefault, in.Enabled
	if d.Headers == nil {
		d.Headers = map[string]string{}
	}
}

// Create 新建数据源。带凭据而主密钥未配置时返回 ErrSecretUnavailable（不带凭据的写入不受影响）。
func (uc *DatasourceUsecase) Create(ctx context.Context, userID string, in DatasourceInput) (*Datasource, error) {
	if in.Conn.AuthType == mc.AuthNone {
		in.Secret = nil
	}
	var err error
	if in.Conn.Headers, _, err = restoreHeaders(in.Conn.Headers, nil); err != nil {
		return nil, err
	}
	if err := uc.normalize(&in, in.Secret != nil); err != nil {
		return nil, err
	}
	var sealed []byte
	if in.Secret != nil {
		var err error
		if sealed, err = uc.cipher.Seal(*in.Secret); err != nil {
			return nil, err
		}
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	d := &Datasource{ID: id}
	in.apply(d)
	if err := uc.writer.Create(ctx, d, sealed, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Update 编辑数据源（整体替换）。不传凭据 = 沿用已保存的；认证方式改为「无」时清除已保存的凭据；
// 值为 HeaderMask 的请求头沿用已保存的值。沿用任何一项都要求地址的协议、主机、端口没变——否则
// 「只改地址、不重输密码」就能把已保存的密码发到任意主机（ErrDatasourceSecretRequired）；Remote Write
// 地址同理（writeOriginAllowed）。清除仍有启用的记录规则在用的 Remote Write 地址由仓储在写事务内拒绝。
func (uc *DatasourceUsecase) Update(ctx context.Context, userID, id string, in DatasourceInput, version int32) (*Datasource, error) {
	cur, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	headers, reusedHeader, err := restoreHeaders(in.Conn.Headers, cur.Headers)
	if err != nil {
		return nil, err
	}
	in.Conn.Headers = headers
	reusedSecret := in.Secret == nil && in.Conn.AuthType != mc.AuthNone && cur.HasSecret
	if (reusedSecret || reusedHeader) && !sameOrigin(cur.URL, in.Conn.URL) {
		return nil, errOriginChanged
	}
	if (reusedSecret || reusedHeader) && !writeOriginAllowed(strings.TrimSpace(in.RemoteWriteURL), in.Conn.URL, cur.RemoteWriteURL) {
		return nil, errWriteOriginChanged
	}
	var change SecretChange
	switch {
	case in.Conn.AuthType == mc.AuthNone:
		in.Secret = nil
		change = SecretChange{Set: true}
	case in.Secret != nil:
		change.Set = true
	}
	if err := uc.normalize(&in, in.Secret != nil || reusedSecret); err != nil {
		return nil, err
	}
	if in.Secret != nil {
		if change.Sealed, err = uc.cipher.Seal(*in.Secret); err != nil {
			return nil, err
		}
	}
	d := &Datasource{ID: id}
	in.apply(d)
	if err := uc.writer.Update(ctx, d, change, version, userID); err != nil {
		return nil, err
	}
	return uc.Get(ctx, id)
}

// Delete 删除数据源；仍被规则明确指定（范围为「全部数据源」的规则不算引用）或仍被记录规则使用时拒绝。
func (uc *DatasourceUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.writer.Delete(ctx, id, userID)
}

// savedConnection 取已保存数据源的连接配置并解出凭据。
func (uc *DatasourceUsecase) savedConnection(ctx context.Context, d *Datasource) (Connection, error) {
	conn := d.connection()
	if !d.HasSecret || d.AuthType == mc.AuthNone {
		return conn, nil
	}
	sealed, err := uc.lookup.GetSecret(ctx, d.ID)
	if err != nil {
		return Connection{}, err
	}
	if len(sealed) == 0 {
		return conn, nil
	}
	conn.Secret, err = uc.cipher.Open(sealed)
	return conn, err
}

// acquire 占一个并发额度；已满返回 false。
func (uc *DatasourceUsecase) acquire() (release func(), ok bool) {
	select {
	case uc.slots <- struct{}{}:
		return func() { <-uc.slots }, true
	default:
		return nil, false
	}
}

func queryTimeout(conn Connection) time.Duration {
	ms := conn.TimeoutMS
	if ms <= 0 {
		ms = 10000
	}
	return time.Duration(min(ms, 30000)) * time.Millisecond
}

// outbound 所有出站请求（测试连接、即时查询、区间查询）的统一入口：占一个并发额度（已满立即返回
// ErrQueryBusy，不排队，避免请求堆积）、套上数据源的超时、记录耗时。
func (uc *DatasourceUsecase) outbound(ctx context.Context, conn Connection, call func(ctx context.Context) (*QueryResult, error)) (*QueryResult, time.Duration, error) {
	release, ok := uc.acquire()
	if !ok {
		return nil, 0, ErrQueryBusy
	}
	defer release()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout(conn))
	defer cancel()
	start := time.Now()
	res, err := call(ctx)
	return res, time.Since(start), err
}

// probe 实际发一次查询（`1`），把结果折成 TestResult：数据源返回的失败不作为接口错误。
func (uc *DatasourceUsecase) probe(ctx context.Context, conn Connection) (*TestResult, error) {
	_, latency, err := uc.outbound(ctx, conn, func(ctx context.Context) (*QueryResult, error) {
		return uc.client.Instant(ctx, conn, "1", time.Time{}, 1)
	})
	// 并发已满、地址被策略拒绝是调用方要处理的错误，不是「数据源连不通」。
	if kerrors.Is(err, ErrQueryBusy) || kerrors.Is(err, ErrDatasourceAddressBlocked) {
		return nil, err
	}
	res := &TestResult{OK: err == nil, Latency: latency}
	if err != nil {
		res.Error = probeReason(err)
	}
	return res, nil
}

// probeReason 测试连接失败的说明：只给粗粒度的分类，不回显数据源 / 客户端的错误原文（原文可能带着
// 上游返回的内部地址、版本等部署信息）。
func probeReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "连接超时：数据源在超时时间内没有响应"
	case kerrors.Is(err, ErrQueryInvalid):
		return "数据源拒绝了测试查询：请检查数据源类型与地址路径"
	case kerrors.Is(err, ErrDatasourceInvalid):
		return "连接配置不合法：请检查地址、认证方式与请求头"
	case kerrors.Is(err, ErrDatasourceUnreachable):
		return "数据源不可达或返回了无法解析的响应：请检查地址、认证信息与网络连通性"
	}
	return "连接失败"
}

// Test 测试已保存数据源的连接。
func (uc *DatasourceUsecase) Test(ctx context.Context, id string) (*TestResult, error) {
	d, err := uc.reader.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	conn, err := uc.savedConnection(ctx, d)
	if err != nil {
		return nil, err
	}
	return uc.probe(ctx, conn)
}

// TestConfig 测试未保存的连接配置。给出 id 时，未传的凭据（secret 为 nil）与值为 HeaderMask 的请求头
// 沿用该数据源已保存的值。沿用有两个前提，任一不满足返回 ErrDatasourceSecretRequired：
//   - 调用方有编辑数据源的接口权限（本接口同时归属「新建」元素，只有新建权限的人不该能借别的数据源
//     已保存的凭据发请求）；
//   - 地址的协议、主机、端口与已保存的一致（否则已保存的凭据会被发到调用方指定的任意地址）。
func (uc *DatasourceUsecase) TestConfig(ctx context.Context, userID string, conn Connection, secret *string, id string) (*TestResult, error) {
	wantSecret := conn.AuthType != mc.AuthNone && secret == nil
	if secret != nil && conn.AuthType != mc.AuthNone {
		conn.Secret = *secret
	}
	if id == "" {
		var err error
		if conn.Headers, _, err = restoreHeaders(conn.Headers, nil); err != nil {
			return nil, err
		}
	} else if wantSecret || hasMaskedHeader(conn.Headers) {
		if err := uc.reuseSaved(ctx, userID, id, &conn, wantSecret); err != nil {
			return nil, err
		}
	}
	if conn.TimeoutMS == 0 {
		conn.TimeoutMS = 10000
	}
	if err := uc.client.Validate(conn); err != nil {
		return nil, err
	}
	return uc.probe(ctx, conn)
}

func hasMaskedHeader(headers map[string]string) bool {
	for _, v := range headers {
		if v == HeaderMask {
			return true
		}
	}
	return false
}

// reuseSaved 把数据源 id 已保存的凭据（wantSecret 时）与掩码请求头的值填进 conn。
func (uc *DatasourceUsecase) reuseSaved(ctx context.Context, userID, id string, conn *Connection, wantSecret bool) error {
	ok, err := uc.perms.CanEditDatasource(ctx, userID)
	if err != nil {
		return err
	}
	if !ok {
		return detail(ErrDatasourceSecretRequired, "reusing saved credentials requires permission to edit datasources: please enter them")
	}
	saved, err := uc.reader.Get(ctx, id)
	if err != nil {
		return err
	}
	headers, reusedHeader, err := restoreHeaders(conn.Headers, saved.Headers)
	if err != nil {
		return err
	}
	conn.Headers = headers
	reusedSecret := wantSecret && saved.HasSecret && saved.AuthType != mc.AuthNone
	if !reusedSecret && !reusedHeader {
		return nil
	}
	if !sameOrigin(saved.URL, conn.URL) {
		return errOriginChanged
	}
	if reusedSecret {
		savedConn, err := uc.savedConnection(ctx, saved)
		if err != nil {
			return err
		}
		conn.Secret = savedConn.Secret
	}
	return nil
}

// queryable 取启用的数据源及其带凭据的连接配置。
func (uc *DatasourceUsecase) queryable(ctx context.Context, id string) (Connection, error) {
	d, err := uc.reader.Get(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	if !d.Enabled {
		return Connection{}, ErrDatasourceDisabled
	}
	return uc.savedConnection(ctx, d)
}

// query 在启用的数据源上执行一次查询，并把结果压到返回上限以内。
func (uc *DatasourceUsecase) query(ctx context.Context, datasourceID string, call func(ctx context.Context, conn Connection) (*QueryResult, error)) (*QueryResult, error) {
	conn, err := uc.queryable(ctx, datasourceID)
	if err != nil {
		return nil, err
	}
	res, took, err := uc.outbound(ctx, conn, func(ctx context.Context) (*QueryResult, error) { return call(ctx, conn) })
	if err != nil {
		return nil, err
	}
	res.Duration = took
	capPoints(res)
	return res, nil
}

// Query 即时查询。
func (uc *DatasourceUsecase) Query(ctx context.Context, datasourceID, expr string, ts time.Time) (*QueryResult, error) {
	return uc.query(ctx, datasourceID, func(ctx context.Context, conn Connection) (*QueryResult, error) {
		return uc.client.Instant(ctx, conn, expr, ts, maxQuerySeries)
	})
}

// QueryRange 区间查询。
func (uc *DatasourceUsecase) QueryRange(ctx context.Context, datasourceID, expr string, start, end time.Time, step time.Duration) (*QueryResult, error) {
	if !start.Before(end) || step <= 0 {
		return nil, detail(ErrQueryInvalid, "start must be before end and step must be positive")
	}
	if end.Sub(start)/step > maxRangeSteps {
		return nil, detail(ErrQueryInvalid, "too many points: enlarge the step or narrow the time range (at most %d steps)", maxRangeSteps)
	}
	return uc.query(ctx, datasourceID, func(ctx context.Context, conn Connection) (*QueryResult, error) {
		return uc.client.Range(ctx, conn, expr, start, end, step, maxQuerySeries)
	})
}

// metadata 在启用的数据源上读取一次补全元数据（指标名 / 标签名 / 标签值）：与 query 相同的
// 可查性检查、并发闸门与单请求超时，只是结果为字符串列表（无采样点，无需截点）。
func (uc *DatasourceUsecase) metadata(ctx context.Context, datasourceID string, call func(ctx context.Context, conn Connection) ([]string, error)) ([]string, error) {
	conn, err := uc.queryable(ctx, datasourceID)
	if err != nil {
		return nil, err
	}
	release, ok := uc.acquire()
	if !ok {
		return nil, ErrQueryBusy
	}
	defer release()
	mctx, cancel := context.WithTimeout(ctx, queryTimeout(conn))
	defer cancel()
	return call(mctx, conn)
}

// ListMetricNames 指标名补全数据：字典序，至多 limit 条（≤0 或超上限按缺省 / 上限夹紧）。
func (uc *DatasourceUsecase) ListMetricNames(ctx context.Context, datasourceID string, limit int) ([]string, error) {
	limit = clampCompletionLimit(limit)
	return uc.metadata(ctx, datasourceID, func(ctx context.Context, conn Connection) ([]string, error) {
		return uc.client.MetricNames(ctx, conn, limit)
	})
}

// ListLabelNames 标签名补全数据。
func (uc *DatasourceUsecase) ListLabelNames(ctx context.Context, datasourceID string, limit int) ([]string, error) {
	limit = clampCompletionLimit(limit)
	return uc.metadata(ctx, datasourceID, func(ctx context.Context, conn Connection) ([]string, error) {
		return uc.client.LabelNames(ctx, conn, limit)
	})
}

// ListLabelValues 标签值补全数据；label 名非法由 client 返回查询领域错误。
func (uc *DatasourceUsecase) ListLabelValues(ctx context.Context, datasourceID, label string, limit int) ([]string, error) {
	limit = clampCompletionLimit(limit)
	return uc.metadata(ctx, datasourceID, func(ctx context.Context, conn Connection) ([]string, error) {
		return uc.client.LabelValues(ctx, conn, label, limit)
	})
}

// clampCompletionLimit 补全条数夹紧：≤0 取缺省，超过上限取上限。
func clampCompletionLimit(limit int) int {
	switch {
	case limit <= 0:
		return defaultCompletionLimit
	case limit > maxCompletionLimit:
		return maxCompletionLimit
	default:
		return limit
	}
}

// capPoints 把采样点总数压到 maxQueryPoints 以内：超出的序列整条丢弃并标记截断。
func capPoints(res *QueryResult) {
	total := 0
	for i := range res.Series {
		total += len(res.Series[i].Points)
		if total > maxQueryPoints {
			res.Series, res.Truncated = res.Series[:i], true
			return
		}
	}
}
