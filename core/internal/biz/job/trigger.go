package biz

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/kit/idgen"
	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
	"github.com/lanyulei/reborn/backend/kit/paging"
)

const (
	// alertBatch 一轮读取的告警事件数。
	alertBatch = 200
	// alertLookback 告警事件的回看窗口：触发器只处理最近这段时间内首次触发的告警（服务停机更久的不补）。
	alertLookback = 10 * time.Minute
	// signalBatch 一轮发送的流程信号数。
	signalBatch = 20
	// signalMaxAttempts 流程信号的最多尝试次数（流程实例一直没有等这个信号的节点时放弃）。
	signalMaxAttempts = 60
	// signalRetry 信号重试间隔的基数（按次数线性增长，封顶 5 分钟）。
	signalRetry = 5 * time.Second
)

// SecretOpener 解密触发器令牌（校验流程 Webhook 的 HMAC 签名需要明文密钥）。与作业凭据不同：
// 这个令牌是 core 自己签发的调用凭证，不是登录主机的凭据。
type SecretOpener interface {
	// Open 解密；主密钥未配置返回 ErrSecretUnavailable。
	Open(sealed []byte) (string, error)
}

// TriggerUsecase 触发器用例：维护、开放接口触发、告警触发、流程信号回发。
type TriggerUsecase struct {
	repo      TriggerRepo
	secrets   TriggerSecrets
	fires     FireRepo
	signals   SignalQueue
	alerts    AlertFeed
	rules     AlertRules
	templates TemplateReader
	targets   *TargetUsecase
	execs     *ExecutionUsecase
	sealer    SecretSealer
	opener    SecretOpener
	workflow  Signals
	dir       Directory
	now       func() time.Time
	// seen 本进程已处理过的（触发器, 告警事件）：回看窗口内每轮都会重读同一批事件，记住处理过的
	// 就不必每轮都去数据库撞一次去重键。只是省查询的缓存——正确性由触发记录的唯一键保证。
	seenMu sync.Mutex
	seen   map[string]time.Time
}

// TriggerDeps 聚合 TriggerUsecase 的依赖。
type TriggerDeps struct {
	Repo     TriggerRepo
	Secrets  TriggerSecrets
	Fires    FireRepo
	Signals  SignalQueue
	Alerts   AlertFeed
	Rules    AlertRules
	Sealer   SecretSealer
	Opener   SecretOpener
	Workflow Signals
}

// NewTriggerDeps 聚合依赖。
func NewTriggerDeps(repo TriggerRepo, secrets TriggerSecrets, fires FireRepo, signals SignalQueue, alerts AlertFeed,
	rules AlertRules, sealer SecretSealer, opener SecretOpener, workflow Signals,
) TriggerDeps {
	return TriggerDeps{
		Repo: repo, Secrets: secrets, Fires: fires, Signals: signals, Alerts: alerts,
		Rules: rules, Sealer: sealer, Opener: opener, Workflow: workflow,
	}
}

// NewTriggerUsecase new a TriggerUsecase.
func NewTriggerUsecase(d TriggerDeps, templates TemplateReader, targets *TargetUsecase, execs *ExecutionUsecase, dir Directory) *TriggerUsecase {
	return &TriggerUsecase{
		repo: d.Repo, secrets: d.Secrets, fires: d.Fires, signals: d.Signals, alerts: d.Alerts, rules: d.Rules,
		templates: templates, targets: targets, execs: execs, sealer: d.Sealer, opener: d.Opener, workflow: d.Workflow,
		dir: dir, now: time.Now, seen: map[string]time.Time{},
	}
}

// List 分页查询触发器。
func (uc *TriggerUsecase) List(ctx context.Context, filter TriggerFilter, page, pageSize int64) ([]*Trigger, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	items, total, err := uc.repo.List(ctx, filter, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	return items, total, uc.fillNames(ctx, items)
}

func (uc *TriggerUsecase) fillNames(ctx context.Context, items []*Trigger) error {
	var users, ruleIDs []string
	for _, t := range items {
		users, ruleIDs = append(users, t.Owner), append(ruleIDs, t.Alert.RuleIDs...)
	}
	userNames, err := uc.dir.UserNames(ctx, uniq(users))
	if err != nil {
		return err
	}
	ruleNames := map[string]string{}
	if ruleIDs = uniq(ruleIDs); len(ruleIDs) > 0 {
		if ruleNames, err = uc.rules.RuleNames(ctx, ruleIDs); err != nil {
			return err
		}
	}
	for _, t := range items {
		t.OwnerName = userNames[t.Owner]
		t.RuleNames = make([]string, len(t.Alert.RuleIDs))
		for i, id := range t.Alert.RuleIDs {
			t.RuleNames[i] = ruleNames[id]
		}
	}
	return nil
}

func (uc *TriggerUsecase) get(ctx context.Context, id string) (*Trigger, error) {
	t, err := uc.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return t, uc.fillNames(ctx, []*Trigger{t})
}

// normalize 规整并校验触发器。固定参数按模板的参数定义校验类型与取值，但不要求必填项齐全
// （接口触发器允许调用方补齐；告警触发器在触发时才最终校验）。
func (uc *TriggerUsecase) normalize(ctx context.Context, t *Trigger) error {
	t.Name, t.Description, t.Signal = strings.TrimSpace(t.Name), strings.TrimSpace(t.Description), strings.TrimSpace(t.Signal)
	if t.Name == "" {
		return detail(ErrTriggerInvalid, "name must not be blank")
	}
	switch t.Type {
	case jc.TriggerTypeAPI:
		t.Alert = jc.AlertMatch{}
	case jc.TriggerTypeAlert:
		t.PassParams, t.Signal = false, ""
		a := &t.Alert
		a.RuleIDs, a.HostLabel = uniq(a.RuleIDs), strings.TrimSpace(a.HostLabel)
		for k, v := range a.Labels {
			if strings.TrimSpace(v) == "" {
				delete(a.Labels, k)
			}
		}
		// 没有规则也没有标签条件 = 任何告警都触发一次作业：几乎必然是配错了。
		if len(a.RuleIDs) == 0 && len(a.Labels) == 0 {
			return detail(ErrTriggerInvalid, "an alert trigger needs at least one alert rule or one label condition")
		}
	default:
		return detail(ErrTriggerInvalid, "unknown trigger type")
	}
	tpl, err := uc.templates.Get(ctx, t.TemplateID)
	if err != nil {
		if errorsIs(err, ErrTemplateNotFound) {
			// 停用必须总能成功：模板已被删除的触发器也要停得下来（内容原样保存，反正不会再触发）。
			if !t.Enabled {
				return nil
			}
			return detail(ErrTriggerInvalid, "the selected job template does not exist")
		}
		return err
	}
	if t.TemplateVersion, err = confirmTemplate(ErrTriggerInvalid, tpl, t.TemplateVersion, t.Enabled); err != nil {
		return err
	}
	fixed, err := checkPartialParams(tpl.Params, t.Params)
	if err != nil {
		if !t.Enabled {
			return nil // 同上：模板改过后参数对不上，也不妨碍停用
		}
		return detail(ErrTriggerInvalid, "%s", Message(err))
	}
	t.Params = fixed
	return nil
}

// newToken 生成接口触发器的调用令牌：摘要用于 Bearer 认证，密文用于校验 HMAC 签名。
// 主密钥未配置时没有密文——此时只支持 Bearer 认证。
func (uc *TriggerUsecase) newToken() (string, TriggerToken, error) {
	token, hash, err := jc.NewTriggerToken()
	if err != nil {
		return "", TriggerToken{}, err
	}
	sealed, err := uc.sealer.Seal(token)
	if err != nil && !errorsIs(err, ErrSecretUnavailable) {
		return "", TriggerToken{}, err
	}
	return token, TriggerToken{Hash: hash, Sealed: sealed}, nil
}

// Create 新建触发器；接口触发器同时返回调用令牌明文（只此一次）。创建人即负责人。
func (uc *TriggerUsecase) Create(ctx context.Context, userID string, t *Trigger) (*Trigger, string, error) {
	// 保存人即负责人：他自己不能执行作业，就不能让定时 / 触发替他执行。
	if err := uc.execs.RequireRun(ctx, userID); err != nil {
		return nil, "", err
	}
	if err := uc.normalize(ctx, t); err != nil {
		return nil, "", err
	}
	id, err := idgen.NewID()
	if err != nil {
		return nil, "", err
	}
	t.ID = id
	var token string
	var stored *TriggerToken
	if t.Type == jc.TriggerTypeAPI {
		var tk TriggerToken
		if token, tk, err = uc.newToken(); err != nil {
			return nil, "", err
		}
		stored = &tk
	}
	if err := uc.repo.Write(ctx, OpCreate, t, 0, userID, stored); err != nil {
		return nil, "", err
	}
	created, err := uc.get(ctx, id)
	return created, token, err
}

// Update 编辑触发器（整体替换，类型不可改）；负责人变更为当前操作人。
func (uc *TriggerUsecase) Update(ctx context.Context, userID string, t *Trigger, version int32) (*Trigger, error) {
	// 保存人即负责人：他自己不能执行作业，就不能让定时 / 触发替他执行。停用不需要：有编辑权限的人
	// 必须总能把一个失控的触发器停下来（再次启用时仍会检查启用人）。
	if t.Enabled {
		if err := uc.execs.RequireRun(ctx, userID); err != nil {
			return nil, err
		}
	}
	cur, err := uc.repo.Get(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	if cur.Type != t.Type {
		return nil, detail(ErrTriggerInvalid, "the trigger type cannot be changed")
	}
	if err := uc.normalize(ctx, t); err != nil {
		return nil, err
	}
	if err := uc.repo.Write(ctx, OpUpdate, t, version, userID, nil); err != nil {
		return nil, err
	}
	return uc.get(ctx, t.ID)
}

// ResetToken 重置接口触发器的调用令牌：旧令牌立即失效。
func (uc *TriggerUsecase) ResetToken(ctx context.Context, userID, id string) (string, error) {
	// 拿到新令牌的人就能触发执行：他须自己有执行权限，并成为负责人（此后以他的身份与数据范围执行）。
	if err := uc.execs.RequireRun(ctx, userID); err != nil {
		return "", err
	}
	token, stored, err := uc.newToken()
	if err != nil {
		return "", err
	}
	if err := uc.secrets.SetToken(ctx, id, stored, userID); err != nil {
		return "", err
	}
	return token, nil
}

// Delete 删除触发器。
func (uc *TriggerUsecase) Delete(ctx context.Context, userID, id string) error {
	return uc.repo.Write(ctx, OpDelete, &Trigger{ID: id}, 0, userID, nil)
}

// ListFires 分页查询触发记录。
func (uc *TriggerUsecase) ListFires(ctx context.Context, filter FireFilter, page, pageSize int64) ([]*TriggerFire, int64, error) {
	page, pageSize = paging.Normalize(page, pageSize)
	return uc.fires.List(ctx, filter, page, pageSize)
}

// authenticate 校验开放接口的调用凭证：Bearer 令牌（比对摘要），或流程 Webhook 的 HMAC 签名
// （sha256=hex(HMAC-SHA256(令牌, 时间戳 + "." + 请求体))，时间戳偏差不超过 SignatureSkew 秒）。
// 触发器不存在与凭证错误返回同一个错误，不泄露触发器是否存在。
func (uc *TriggerUsecase) authenticate(ctx context.Context, id string, auth FireAuth) error {
	stored, err := uc.secrets.Token(ctx, id)
	if err != nil {
		if errorsIs(err, ErrTriggerNotFound) {
			return ErrTriggerUnauthorized
		}
		return err
	}
	if auth.Bearer != "" {
		if jc.RunnerSecretMatches(auth.Bearer, stored.Hash) {
			return nil
		}
		return ErrTriggerUnauthorized
	}
	if auth.Signature == "" || auth.Timestamp == "" || len(stored.Sealed) == 0 {
		return ErrTriggerUnauthorized
	}
	ts, err := strconv.ParseInt(auth.Timestamp, 10, 64)
	if err != nil || math.Abs(float64(uc.now().Unix()-ts)) > jc.SignatureSkew {
		return ErrTriggerUnauthorized
	}
	token, err := uc.opener.Open(stored.Sealed)
	if err != nil {
		return ErrTriggerUnauthorized
	}
	mac := hmac.New(sha256.New, []byte(token))
	mac.Write([]byte(auth.Timestamp + "."))
	mac.Write(auth.Body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(auth.Signature)) {
		return ErrTriggerUnauthorized
	}
	return nil
}

// Fire 开放接口触发：认证 → 去重 → 以负责人的身份提交执行。返回触发记录（幂等键命中时是先前那条）。
func (uc *TriggerUsecase) Fire(ctx context.Context, id string, auth FireAuth, req FireRequest) (*TriggerFire, error) {
	if !idgen.IsUUID(id) {
		return nil, ErrTriggerUnauthorized
	}
	if err := uc.authenticate(ctx, id, auth); err != nil {
		return nil, err
	}
	t, err := uc.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if t.Type != jc.TriggerTypeAPI {
		return nil, ErrTriggerUnauthorized
	}
	if !t.Enabled {
		return nil, ErrTriggerDisabled
	}
	if req.InstanceID != "" && !idgen.IsUUID(req.InstanceID) {
		return nil, detail(ErrTriggerInvalid, "instanceId must be a uuid")
	}
	params := map[string]any{}
	for k, v := range t.Params {
		params[k] = v
	}
	if t.PassParams {
		tpl, err := uc.templates.Get(ctx, t.TemplateID)
		if err != nil {
			return nil, err
		}
		// 流程变量里只取与模板参数同名的项；显式的 params 优先，未定义的键照常被拒绝。
		for _, p := range tpl.Params {
			if v, ok := req.Variables[p.Code]; ok && v != nil {
				params[p.Code] = v
			}
		}
		for k, v := range req.Params {
			params[k] = v
		}
	}
	if strings.ContainsRune(req.Ref, 0) {
		return nil, detail(ErrTriggerInvalid, "requestId contains invalid characters")
	}
	req.Summary = strings.ReplaceAll(req.Summary, "\x00", "")
	ref := req.Ref
	if ref == "" {
		if ref, err = idgen.NewID(); err != nil {
			return nil, err
		}
	}
	fire := &TriggerFire{
		TriggerID: t.ID, Ref: truncateRunes(ref, 128), Summary: truncateRunes(strings.TrimSpace(req.Summary), 255),
		WorkflowInstanceID: req.InstanceID,
	}
	if t.Signal != "" && req.InstanceID != "" {
		fire.Signal, fire.SignalState = t.Signal, jc.SignalPending
	}
	return uc.fire(ctx, t, fire, params, nil)
}

// fire 登记一次触发并提交执行。only 非 nil 时只在这些主机上执行。
func (uc *TriggerUsecase) fire(ctx context.Context, t *Trigger, fire *TriggerFire, params map[string]any, only []string) (*TriggerFire, error) {
	id, err := idgen.NewID()
	if err != nil {
		return nil, err
	}
	fire.ID = id
	existing, fresh, err := uc.fires.Begin(ctx, fire, t.CooldownS)
	if err != nil {
		return nil, err
	}
	if !fresh {
		// 先前那次登记之后、提交执行之前进程中断了（记录停在「已提交」却没有执行）：接着把它做完。
		// 提交的幂等键由触发记录 ID 派生，所以与另一个正在处理同一条记录的实例并发也只会建出一条执行。
		if existing.Result != jc.FireSubmitted || existing.ExecutionID != "" {
			return existing, nil
		}
		fire = existing
	}
	if fire.Result == jc.FireSkipped {
		return fire, nil // 冷却中：Begin 已直接记为跳过
	}
	trigger := jc.TriggerWorkflow
	if t.Type == jc.TriggerTypeAlert {
		trigger = jc.TriggerAlert
	}
	exec, err := uc.execs.Submit(ctx, t.Owner, SubmitInput{
		TemplateID: t.TemplateID, TemplateVersion: t.TemplateVersion, Params: params, CIIDs: only, Trigger: trigger, TriggerRef: t.ID,
		RequestID: "trg-" + strings.ReplaceAll(fire.ID, "-", ""),
	})
	switch {
	case err == nil:
		fire.Result, fire.ExecutionID = jc.FireSubmitted, exec.ID
	case Rejected(err):
		fire.Result, fire.Reason, fire.SignalState = jc.FireFailed, truncateRunes(Message(err), 512), jc.SignalNone
	default:
		return nil, err
	}
	if err := uc.fires.Complete(ctx, fire); err != nil {
		return nil, err
	}
	return fire, nil
}

// alertMatches 报告告警事件是否满足触发器的匹配条件。
func alertMatches(m jc.AlertMatch, a *AlertEvent) bool {
	if len(m.RuleIDs) > 0 && !slices.Contains(m.RuleIDs, a.RuleID) {
		return false
	}
	if len(m.Severities) > 0 && !slices.Contains(m.Severities, a.Severity) {
		return false
	}
	for k, v := range m.Labels {
		if a.Labels[k] != v {
			return false
		}
	}
	return true
}

// alertHost 从告警标签里取主机地址：去掉 :端口（instance 常见为 10.0.0.1:9100），再按主机地址的口径规范化。
func alertHost(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if h, _, err := net.SplitHostPort(value); err == nil {
		value = h
	}
	return jc.NormalizeAddress(strings.Trim(value, "[]"))
}

// RunAlerts 告警自愈（后台循环；多实例并发安全）：读取最近首次触发的告警事件，逐个与启用的告警
// 触发器匹配，命中即触发一次。同一事件对同一触发器只触发一次（触发记录的唯一键去重），
// 因此每轮都从回看窗口的起点重读是安全的。返回本轮新触发的次数。
func (uc *TriggerUsecase) RunAlerts(ctx context.Context) (int, error) {
	triggers, err := uc.secrets.EnabledAlertTriggers(ctx)
	if err != nil || len(triggers) == 0 {
		return 0, err
	}
	events, err := uc.alerts.FiringSince(ctx, uc.now().Add(-alertLookback), alertBatch)
	if err != nil {
		return 0, err
	}
	fired := 0
	var first error
	for _, a := range events {
		for _, t := range triggers {
			// 触发器启用（或最后一次编辑）之前就已触发的告警不处理：启用一个触发器不应对着存量告警补跑作业。
			if !alertMatches(t.Alert, a) || a.FirstFireTime.Before(t.UpdateTime) || uc.alreadySeen(t.ID, a.ID) {
				continue
			}
			fresh, err := uc.fireAlert(ctx, t, a)
			if err != nil {
				// 一个触发器出故障不挡住其余的：没记为已处理，下一轮重试。
				if first == nil {
					first = err
				}
				continue
			}
			uc.markSeen(t.ID, a.ID)
			if fresh {
				fired++
			}
		}
	}
	_ = fired
	// 本轮即使有新触发也不要求立即再跑：下一轮照常按间隔执行。
	return 0, first
}

func (uc *TriggerUsecase) alreadySeen(triggerID, alertID string) bool {
	uc.seenMu.Lock()
	defer uc.seenMu.Unlock()
	_, ok := uc.seen[triggerID+"/"+alertID]
	return ok
}

func (uc *TriggerUsecase) markSeen(triggerID, alertID string) {
	now := uc.now()
	uc.seenMu.Lock()
	defer uc.seenMu.Unlock()
	uc.seen[triggerID+"/"+alertID] = now
	if len(uc.seen) > 4*alertBatch {
		for k, at := range uc.seen {
			if now.Sub(at) > 2*alertLookback {
				delete(uc.seen, k)
			}
		}
	}
}

// fireAlert 对一条告警事件触发一个触发器；返回是否为新的一次触发。
func (uc *TriggerUsecase) fireAlert(ctx context.Context, t *Trigger, a *AlertEvent) (bool, error) {
	fire := &TriggerFire{TriggerID: t.ID, Ref: a.ID, Summary: truncateRunes(a.RuleName, 255)}
	var only []string
	if t.Alert.HostLabel != "" {
		addr, ok := alertHost(a.Labels[t.Alert.HostLabel])
		fire.Summary = truncateRunes(a.RuleName+" · "+a.Labels[t.Alert.HostLabel], 255)
		ciID := ""
		if ok {
			var err error
			if ciID, err = uc.hostByAddress(ctx, t, addr); err != nil && !Rejected(err) {
				return false, err
			}
		}
		if ciID == "" {
			// 告警里的主机不在模板的目标主机之内：不执行，但留一条记录说明原因（同一事件不再重复判断）。
			id, err := idgen.NewID()
			if err != nil {
				return false, err
			}
			fire.ID, fire.Result = id, jc.FireSkipped
			fire.Reason = "告警标签 " + t.Alert.HostLabel + " 的主机不在作业模板的目标主机之内"
			_, fresh, err := uc.fires.Begin(ctx, fire, 0)
			return fresh, err
		}
		only = []string{ciID}
	}
	out, err := uc.fire(ctx, t, fire, t.Params, only)
	if err != nil {
		return false, err
	}
	// fire 返回的是本次新建的记录（同一指针）才算新触发；去重命中时返回的是先前那条。
	return out == fire, nil
}

// hostByAddress 在模板的目标主机（按触发器负责人的数据范围）里找连接地址为 addr 的那台，返回其资源 ID。
func (uc *TriggerUsecase) hostByAddress(ctx context.Context, t *Trigger, addr string) (string, error) {
	tpl, err := uc.templates.Get(ctx, t.TemplateID)
	if err != nil {
		return "", err
	}
	set, err := uc.targets.ResolveN(ctx, t.Owner, tpl.Target, jc.MaxHosts*jc.MaxSlices)
	if err != nil {
		return "", err
	}
	for _, h := range set.Hosts {
		if h.Problem == "" && h.Address == addr {
			return h.CIID, nil
		}
	}
	return "", nil
}

// RunSignals 回发流程信号（后台循环；多实例并发安全——重复发送同一信号由流程引擎判为「没有节点在等」）。
// 作业（含全部切片）结束后向发起调用的流程实例发送信号；流程还没走到等待节点时稍后重试。
func (uc *TriggerUsecase) RunSignals(ctx context.Context) (int, error) {
	items, err := uc.signals.Due(ctx, signalBatch)
	if err != nil {
		return 0, err
	}
	var first error
	for _, s := range items {
		delivered, gone, err := uc.workflow.Signal(ctx, s.InstanceID, s.Signal, map[string]any{
			"jobExecutionId": s.ExecutionID, "jobStatus": s.Status, "jobSuccess": s.Success,
		})
		state, retry := jc.SignalPending, min(time.Duration(s.Attempts+1)*signalRetry, 5*time.Minute)
		switch {
		case err != nil && !Rejected(err):
			// 故障（非流程拒绝）：推后重试、不计入放弃，也不挡住队列里其余的信号。
			if first == nil {
				first = err
			}
			if err := uc.signals.Mark(ctx, s.FireID, jc.SignalPending, retry); err != nil {
				return 0, err
			}
			continue
		case delivered:
			state = jc.SignalSent
		case gone || err != nil || s.Attempts+1 >= signalMaxAttempts:
			// 实例已结束 / 信号被流程拒绝（变量名与业务字段冲突等）/ 重试用尽：放弃。
			state = jc.SignalGaveUp
		}
		if err := uc.signals.Mark(ctx, s.FireID, state, retry); err != nil {
			return 0, err
		}
	}
	return 0, first
}
