package biz

import (
	"context"
	"time"
)

// 仓储接口（data 层以 pgx 实现）。写方法以 Tx 后缀的在调用方事务内执行；其余独立语句。
// 本域所有表只有 core 读写，无其他进程。

// IncidentReader 故障读。
type IncidentReader interface {
	// Get 不存在返回 ErrNotFound；派生字段 AlertCount / PendingProposals 一并填充。
	Get(ctx context.Context, id string) (*Incident, error)
	// List 按开启时间倒序分页。
	List(ctx context.Context, filter IncidentFilter, page, pageSize int64) ([]*Incident, int64, error)
	// ListByIDs 批量取（不存在的不在结果中）。
	ListByIDs(ctx context.Context, ids []string) ([]*Incident, error)
	// Inspect 巡检筛选，按开启时间升序至多 limit 条。
	Inspect(ctx context.Context, q InspectQuery, limit int) ([]*Incident, error)
	// Search 全文检索（标题 / 摘要 / 根因），返回 (故障, 相关度) 按相关度倒序。
	Search(ctx context.Context, query, excludeID string, limit int) ([]*Incident, []float32, error)
	// CountRecentByRule 规则在 since 之后开出的故障数（风暴判定）。
	CountRecentByRule(ctx context.Context, ruleID string, since time.Time) (int, error)
	// FindByExternalKey 接入源 + 幂等键对应的故障；没有返回 (nil, nil)。
	FindByExternalKey(ctx context.Context, sourceID, externalKey string) (*Incident, error)
}

// IncidentWriter 故障写。
type IncidentWriter interface {
	// NextNumberTx 当日序号 +1 并返回（行锁；回滚不留空号）。
	NextNumberTx(ctx context.Context, tx Tx, day time.Time) (int32, error)
	// CreateTx 插入新故障（Version 置 1）。编号重复 / 归并键冲突返回 ErrVersionConflict（调用方重试）。
	CreateTx(ctx context.Context, tx Tx, inc *Incident) error
	// LockTx 事务内行锁读取；不存在返回 ErrNotFound。
	LockTx(ctx context.Context, tx Tx, id string) (*Incident, error)
	// SaveTx 以乐观锁写回全部可变列（version +1，入参的 Version 为读取时的值，成功后回填新版本）。
	SaveTx(ctx context.Context, tx Tx, inc *Incident) error
}

// MemberRepo 成员。
type MemberRepo interface {
	List(ctx context.Context, incidentID string) ([]*Member, error)
	// ListMany 批量取多条故障的成员。
	ListMany(ctx context.Context, incidentIDs []string) (map[string][]*Member, error)
	// SetTx 新增 / 覆盖一个成员的角色（已是该角色为空操作）。
	SetTx(ctx context.Context, tx Tx, m *Member, actor string) error
	// RemoveTx 移除成员。
	RemoveTx(ctx context.Context, tx Tx, incidentID, userID string) error
}

// EventRepo 时间线。
type EventRepo interface {
	AppendTx(ctx context.Context, tx Tx, ev *Event) error
	// List 按时间升序分页。
	List(ctx context.Context, incidentID string, filter TimelineFilter, page, pageSize int64) ([]*Event, int64, error)
	// Get 不存在返回 ErrNotFound。
	Get(ctx context.Context, incidentID, eventID string) (*Event, error)
	SetPinnedTx(ctx context.Context, tx Tx, incidentID, eventID string, pinned bool) error
	// Recent 最近 limit 条（时间升序返回），可排除类型。
	Recent(ctx context.Context, incidentID string, excludeTypes []string, limit int) ([]*Event, error)
	// CountSince 某类型事件在 since 之后的条数（Agent 频率上限）。
	CountSince(ctx context.Context, incidentID, typ string, since time.Time) (int, error)
}

// AlertLinkRepo 故障 ↔ 告警。
type AlertLinkRepo interface {
	// AttachTx 关联；告警已属于另一活跃故障返回 ErrAlertLinked；已在本故障返回 (false, nil)。
	AttachTx(ctx context.Context, tx Tx, link *AlertLink) (inserted bool, err error)
	// DetachTx 移出（物理删除）。
	DetachTx(ctx context.Context, tx Tx, incidentID, alertID string) (bool, error)
	// MoveAllTx 把 from 的全部活跃关联迁到 to（method 置 LinkMerge）；返回迁移数。
	MoveAllTx(ctx context.Context, tx Tx, from, to string) (int, error)
	// ReleaseAllTx 故障关闭 / 合并后释放其全部关联（告警可被别的故障关联）。
	ReleaseAllTx(ctx context.Context, tx Tx, incidentID string) error
	// List 分页（按关联时间倒序）。
	List(ctx context.Context, incidentID string, page, pageSize int64) ([]*AlertLink, int64, error)
	// ListAll 全部关联（至多 limit）。
	ListAll(ctx context.Context, incidentID string, limit int) ([]*AlertLink, error)
	// ActiveIncident 告警当前所属的活跃故障 ID（未关联返回空串）。
	ActiveIncident(ctx context.Context, alertID string) (string, error)
	// IncidentIDsByAlert 告警关联过的全部故障 ID。
	IncidentIDsByAlert(ctx context.Context, alertID string) ([]string, error)
	// Count 关联数。
	Count(ctx context.Context, incidentID string) (int, error)
}

// ChangeRefRepo 变更引用。
type ChangeRefRepo interface {
	UpsertTx(ctx context.Context, tx Tx, ref *ChangeRef) error
	List(ctx context.Context, incidentID string) ([]*ChangeRef, error)
}

// CandidateRepo 候选故障。
type CandidateRepo interface {
	// LockOpenTx 行锁读取规则 + 归并键的待定候选；没有返回 (nil, nil)。
	LockOpenTx(ctx context.Context, tx Tx, ruleID, key string) (*Candidate, error)
	// UpsertTx 新建或更新候选（追加告警 / 资源）。
	UpsertTx(ctx context.Context, tx Tx, c *Candidate) error
	// SetStatusTx 置状态（转正时带故障 ID）。
	SetStatusTx(ctx context.Context, tx Tx, id string, status int32, incidentID string) error
	Get(ctx context.Context, id string) (*Candidate, error)
	List(ctx context.Context, status int32, page, pageSize int64) ([]*Candidate, int64, error)
	// Expire 把 updateBefore 之前仍待定的候选置为已过期，返回条数。
	Expire(ctx context.Context, updateBefore time.Time) (int, error)
	// Counts 统计用：总数与已转正数（first_time 在范围内）。
	Counts(ctx context.Context, r StatsRange) (total, promoted int, err error)
}

// RuleRepo 触发规则。
type RuleRepo interface {
	List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*Rule, int64, error)
	// ListEnabled 全部启用的规则，按 sort、create_time 升序。
	ListEnabled(ctx context.Context) ([]*Rule, error)
	// Get 不存在返回 ErrRuleNotFound。
	Get(ctx context.Context, id string) (*Rule, error)
	ListByIDs(ctx context.Context, ids []string) ([]*Rule, error)
	// Create 重名返回 ErrRuleNameExists。
	Create(ctx context.Context, r *Rule, actor string) error
	// Update 乐观锁；重名 ErrRuleNameExists，版本不符 ErrVersionConflict。
	Update(ctx context.Context, r *Rule, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
	// CountPlaybookRefs 引用某预案的规则数。
	CountPlaybookRefs(ctx context.Context, playbookID string) (int, error)
	// CountBusinessRefs 引用某业务的规则数。
	CountBusinessRefs(ctx context.Context, businessID string) (int, error)
}

// BusinessRepo 业务字典。
type BusinessRepo interface {
	List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*Business, int64, error)
	// ListAll 全部未删除的业务，按 sort、name 升序。
	ListAll(ctx context.Context) ([]*Business, error)
	Get(ctx context.Context, id string) (*Business, error)
	// ListByIDs 批量取（含已删除：历史故障仍要显示业务名；不存在的不在结果中）。
	ListByIDs(ctx context.Context, ids []string) ([]*Business, error)
	GetByCode(ctx context.Context, code string) (*Business, error)
	Create(ctx context.Context, b *Business, actor string) error
	Update(ctx context.Context, b *Business, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
	// InUse 被故障 / 接入源引用的数量。
	InUse(ctx context.Context, id string) (int, error)
}

// PlaybookRepo 预案。
type PlaybookRepo interface {
	List(ctx context.Context, keyword string, enabled *bool, page, pageSize int64) ([]*Playbook, int64, error)
	ListEnabled(ctx context.Context) ([]*Playbook, error)
	Get(ctx context.Context, id string) (*Playbook, error)
	ListByIDs(ctx context.Context, ids []string) ([]*Playbook, error)
	Create(ctx context.Context, p *Playbook, actor string) error
	Update(ctx context.Context, p *Playbook, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// ProposalRepo 动作建议。
type ProposalRepo interface {
	// CreateTx 幂等键冲突返回 ErrProposalDuplicate。
	CreateTx(ctx context.Context, tx Tx, p *Proposal) error
	Get(ctx context.Context, id string) (*Proposal, error)
	// LockTx 事务内行锁读取。
	LockTx(ctx context.Context, tx Tx, id string) (*Proposal, error)
	// SaveTx 写回全部可变列；fromStatus 非 0 时要求当前状态等于它（围栏），不符返回 (false, nil)。
	SaveTx(ctx context.Context, tx Tx, p *Proposal, fromStatus int32) (bool, error)
	// SaveVerifiedTx 写回验证结果：围栏「成功且尚未验证」（status = 成功 AND verify_result IS NULL），不符返回 (false, nil)——
	// 两次验证窗口检查并发领到同一条建议时只有一次能写入、记时间线、触发熔断。
	SaveVerifiedTx(ctx context.Context, tx Tx, p *Proposal) (bool, error)
	// List 至多 200 条（接口分页用）。
	List(ctx context.Context, incidentID string, status int32) ([]*Proposal, error)
	// ListByStatusTx 事务内读取故障下处于给定状态的全部建议（不设上限，带行锁）。
	ListByStatusTx(ctx context.Context, tx Tx, incidentID string, statuses []int32) ([]*Proposal, error)
	// ListExpired 待审批且已过期的（至多 limit）。
	ListExpired(ctx context.Context, now time.Time, limit int) ([]*Proposal, error)
	// ListRunning 执行中的（轮询执行结果）。
	ListRunning(ctx context.Context, limit int) ([]*Proposal, error)
	// ListVerifyDue 成功且验证窗口已到、尚未验证的。
	ListVerifyDue(ctx context.Context, now time.Time, limit int) ([]*Proposal, error)
	// LockAutoExecTx 在事务内取「Agent 自动执行配额」的全局咨询锁（事务结束自动释放）：
	// CountAutoRunning 与随后的自动批准写入须在同一把锁下，否则并发的两条建议会同时通过配额检查。
	LockAutoExecTx(ctx context.Context, tx Tx) error
	// CountAutoRunning 自动批准且已批准 / 执行中的 Agent 建议数：全局与指定业务（businessID 为空时 business 为 0）。
	CountAutoRunning(ctx context.Context, businessID string) (global, business int, err error)
	// LastAutoExec 同一预案步骤、靶标与 ciIDs 有交集、自动批准且已批准 / 执行中 / 已结束的建议里最近一次的批准时间；没有返回 nil。
	LastAutoExec(ctx context.Context, ciIDs []string, playbookID string, stepIndex int32) (*time.Time, error)
	// CountPlaybookExecs 预案在 since 之后的执行次数。
	CountPlaybookExecs(ctx context.Context, playbookID string, since time.Time) (int, error)
	// CountPlaybookExecsMany 同 CountPlaybookExecs 的口径，一条语句取多个预案的执行次数；没有执行过的不在结果中。
	CountPlaybookExecsMany(ctx context.Context, playbookIDs []string, since time.Time) (map[string]int, error)
	// CountPending 全局待审批数（统计）。
	CountPending(ctx context.Context) (int, error)
}

// UpdateRepo 公告。
type UpdateRepo interface {
	CreateTx(ctx context.Context, tx Tx, u *Update) error
	// List 按时间倒序至多 limit 条。
	List(ctx context.Context, incidentID string, limit int) ([]*Update, error)
}

// ShareRepo 分享链接。
type ShareRepo interface {
	Create(ctx context.Context, l *ShareLink) error
	List(ctx context.Context, incidentID string) ([]*ShareLink, error)
	// Get 未作废的；不存在返回 ErrShareNotFound。
	Get(ctx context.Context, id string) (*ShareLink, error)
	FindByHash(ctx context.Context, hash []byte) (*ShareLink, error)
	SetToken(ctx context.Context, id string, hash, sealed []byte) error
	Delete(ctx context.Context, id string) error
	// Touch 访问计数 +1 并记时间。
	Touch(ctx context.Context, id string) error
}

// PostmortemRepo 复盘。
type PostmortemRepo interface {
	// Get 没有记录返回 (nil, nil)。
	Get(ctx context.Context, incidentID string) (*Postmortem, error)
	SaveTx(ctx context.Context, tx Tx, pm *Postmortem) error
	AddVersionTx(ctx context.Context, tx Tx, v *PostmortemVersion) error
	ListVersions(ctx context.Context, incidentID string) ([]*PostmortemVersion, error)
}

// ActionItemRepo 改进项。
type ActionItemRepo interface {
	List(ctx context.Context, filter ActionItemFilter, page, pageSize int64) ([]*ActionItem, int64, error)
	ListByIncident(ctx context.Context, incidentID string) ([]*ActionItem, error)
	Get(ctx context.Context, id string) (*ActionItem, error)
	SaveTx(ctx context.Context, tx Tx, it *ActionItem) error
	// SetTicketTx 只写关联工单（其余列不动：入参对象可能是跨域建单之前读出的旧值，整体写回会覆盖期间的编辑）；
	// 改进项不存在返回 ErrActionItemNotFound。
	SetTicketTx(ctx context.Context, tx Tx, id, ticketID, ticketNumber string) error
	// MarkDoneTx 待完成 → 已完成，只写状态与更新时间；已不是待完成（被手动完成 / 已删除）返回 false。
	MarkDoneTx(ctx context.Context, tx Tx, id string) (bool, error)
	DeleteTx(ctx context.Context, tx Tx, incidentID, id string) error
	// ListTicketed 待完成且已建单的（巡检同步工单状态）。
	ListTicketed(ctx context.Context, limit int) ([]*ActionItem, error)
	// Counts 统计：范围内故障的改进项总数与已完成数。
	Counts(ctx context.Context, r StatsRange) (total, done int, err error)
}

// SourceRepo 接入源。
type SourceRepo interface {
	List(ctx context.Context) ([]*Source, error)
	Get(ctx context.Context, id string) (*Source, error)
	// FindByHash 启用的接入源；没有返回 ErrSourceUnauthorized。
	FindByHash(ctx context.Context, hash []byte) (*Source, error)
	Create(ctx context.Context, s *Source, actor string) error
	Update(ctx context.Context, s *Source, expectVersion int32, actor string) error
	SetToken(ctx context.Context, id string, hash []byte, prefix string, actor string) error
	Delete(ctx context.Context, id string, actor string) error
	// Touch 更新最近调用时间（至多每分钟一次）。
	Touch(ctx context.Context, id string) error
	// CountBusinessRefs 引用某业务的接入源数。
	CountBusinessRefs(ctx context.Context, businessID string) (int, error)
}

// WebhookRepo Webhook 订阅。
type WebhookRepo interface {
	List(ctx context.Context) ([]*Webhook, error)
	ListEnabled(ctx context.Context) ([]*Webhook, error)
	Get(ctx context.Context, id string) (*Webhook, error)
	Create(ctx context.Context, w *Webhook, actor string) error
	// Update 不改 Secret（Secret 为 nil 时保留）。
	Update(ctx context.Context, w *Webhook, expectVersion int32, actor string) error
	Delete(ctx context.Context, id string, expectVersion int32, actor string) error
}

// OutboxRepo Webhook 出站箱。
type OutboxRepo interface {
	EnqueueTx(ctx context.Context, tx Tx, items []*OutboxItem) error
	// Claim 领取到期的待投递行（attempts +1、next_time 推到租约之后）。
	Claim(ctx context.Context, limit int, lease time.Duration) ([]*OutboxItem, error)
	// Finish 终结（围栏：仍待投递且 attempts 相同）。
	Finish(ctx context.Context, it *OutboxItem, status int32, note string) error
	// Retry 记失败并按退避重排。
	Retry(ctx context.Context, it *OutboxItem, backoff time.Duration, reason string) error
	// Counts 各 Webhook 的待投递 / 已丢弃数。
	Counts(ctx context.Context) (pending, failed map[string]int32, err error)
	// Purge 清理 before 之前已终结的行。
	Purge(ctx context.Context, before time.Time, limit int) (int, error)
}

// ActionRepo 动作发件箱。
type ActionRepo interface {
	// EnqueueTx 入箱；同一 (incident, kind, ref) 已有待执行行时忽略。
	EnqueueTx(ctx context.Context, tx Tx, a *Action) error
	Claim(ctx context.Context, limit int, lease time.Duration) ([]*Action, error)
	FinishTx(ctx context.Context, tx Tx, a *Action, status int32, note string) (bool, error)
	// Retry 记失败并按退避重排；backoff 为 0 且原因为空即收回租约（下次可领取时间拉回现在，尝试次数不动）。
	Retry(ctx context.Context, a *Action, backoff time.Duration, reason string) error
	Purge(ctx context.Context, before time.Time, limit int) (int, error)
}

// CursorRepo 后台游标；连同关联器已处理序号（同一张游标语义的簿记，由同一仓储实现）。
type CursorRepo interface {
	Get(ctx context.Context, name string) (int64, error)
	SetTx(ctx context.Context, tx Tx, name string, seq int64) error
	FeedSeenRepo
}

// FeedSeenRepo 关联器已处理的告警事件序号（incident_feed_seen）：回看窗口只补没处理过的事件。
type FeedSeenRepo interface {
	// MarkSeenTx 记录已处理的序号（重复忽略）。
	MarkSeenTx(ctx context.Context, tx Tx, seqs []int64) error
	// Seen 返回 (from, to] 内已处理的序号集合。
	Seen(ctx context.Context, from, to int64) (map[int64]struct{}, error)
	// PruneBelow 清理小于 seq 的记录（已滑出回看窗口）。
	PruneBelow(ctx context.Context, seq int64) error
}

// SettingRepo 单例设置。
type SettingRepo interface {
	// Get 返回设置（空对象 = 全缺省）与版本。
	Get(ctx context.Context) (*Settings, error)
	// Save 乐观锁保存。
	Save(ctx context.Context, s *Settings, expectVersion int32, actor string) error
}

// OverviewStats 概览。
type OverviewStats struct {
	Active          int32
	ActiveBySev     map[int32]int32
	Unresponded     int32
	ReviewPending   int32
	ReviewOverdue   int32
	ProposalPending int32
	TodayOpened     int32
	TodayResolved   int32
	// Candidates 待定候选数。带业务过滤时只含「规则固定指向该业务」的候选：候选在转正前没有业务，
	// 按告警标签动态匹配业务的规则的候选不计入任何业务。
	Candidates int32
}

// TrendPoint 趋势的一个桶。
type TrendPoint struct {
	Bucket   time.Time
	Total    int32
	BySev    map[int32]int32
	Duration int64
}

// EfficiencyRow 效率指标一行（Severity 0 = 全部）。
type EfficiencyRow struct {
	Severity int32
	Count    int32
	MTTD     int64
	MTTA     int64
	MTTM     int64
	MTTR     int64
	Reopened int32
}

// NamedCount 分布的一行。
type NamedCount struct {
	Key      string
	Name     string
	Count    int32
	Duration int64
}

// DistributionStats 分布。
type DistributionStats struct {
	RootCauses  []NamedCount
	Sources     []NamedCount
	Businesses  []NamedCount
	Rules       []NamedCount
	AlertLinked int32
}

// StatsRepo 统计查询（演练与已合并的不计）。
type StatsRepo interface {
	Overview(ctx context.Context, businessID string, now time.Time) (*OverviewStats, error)
	Trend(ctx context.Context, r StatsRange, bucket string) ([]*TrendPoint, error)
	Efficiency(ctx context.Context, r StatsRange) ([]*EfficiencyRow, error)
	// ReviewOverdueRate 范围内需要复盘的故障里超期（含已完成但曾超期的按完成时间判定）的占比。
	ReviewOverdue(ctx context.Context, r StatsRange, now time.Time) (required, overdue int, err error)
	Distribution(ctx context.Context, r StatsRange) (*DistributionStats, error)
}
