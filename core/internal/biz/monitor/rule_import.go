package biz

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"

	kerrors "github.com/go-kratos/kratos/v3/errors"
	"gopkg.in/yaml.v3"
)

// 导入预览结论（ImportItem.Status）与导入结果（ImportResult.Action）。
const (
	ImportNew      = "new"
	ImportConflict = "conflict"
	ImportInvalid  = "invalid"
	ImportSkipped  = "skipped"

	ImportCreated     = "created"
	ImportUpdated     = "updated"
	ImportSkippedRule = "skipped"
	ImportFailed      = "failed"

	ConflictSkip      = "skip"
	ConflictOverwrite = "overwrite"
	ConflictRename    = "rename"
)

// 导入内容的结构上限：正文大小由接口校验（1 MiB）；这里再限制节点数与嵌套深度，并拒绝锚点 /
// 别名（规则文件用不到，而别名展开是 YAML 炸弹的来源）。
const (
	maxImportBytes = 1 << 20
	// maxImportExprBytes 一次导入 / 预览里全部表达式（各查询与恢复取值）的总字节数上限。表达式逐条
	// 过语法校验，这是整批校验的 CPU 上限（见 BenchmarkImportPreview_WorstCase）；500 条规则平均
	// 每条 1KiB 表达式仍在其内。
	maxImportExprBytes = 512 << 10
	maxYAMLNodes       = 100000
	maxYAMLDepth       = 16
	// defaultEvalIntervalS 组上没有 interval 时采用的评估周期。
	defaultEvalIntervalS = 60
)

// 导出格式。
const (
	ExportYAML = "yaml"
	ExportJSON = "json"
)

// maxSeverityMap 导入级别对照表的条数上限。
const maxSeverityMap = 50

// ImportOptions 导入的公共选项（预览与导入共用）。SeverityMap 为 severity 标签取值 → 级别的自定义
// 对照（只对 YAML 起作用；键不区分大小写）。
type ImportOptions struct {
	Content         string
	GroupID         string
	DatasourceScope int32
	DatasourceIDs   []string
	DefaultSeverity int32
	Notify          mc.NotifyTargets
	SeverityMap     map[string]int32
}

// normalizeSeverityMap 校验并规整对照表：键去首尾空白、转小写。
func normalizeSeverityMap(in map[string]int32) (map[string]int32, error) {
	if len(in) > maxSeverityMap {
		return nil, ruleInvalid("severity_map has more than %d entries", maxSeverityMap)
	}
	out := make(map[string]int32, len(in))
	for k, v := range in {
		key := strings.ToLower(strings.TrimSpace(k))
		if key == "" || len(key) > 64 || !mc.ValidSeverity(v) {
			return nil, ruleInvalid("severity_map: keys must be 1-64 characters and values 1-4")
		}
		if _, dup := out[key]; dup {
			return nil, ruleInvalid("severity_map: key %q is duplicated (keys are case-insensitive)", key)
		}
		out[key] = v
	}
	return out, nil
}

// severityOf 把 severity 标签的取值换成级别：自定义对照 → 内置对照；都不认识返回 false。
func (o *ImportOptions) severityOf(value string) (int32, bool) {
	key := strings.ToLower(strings.TrimSpace(value))
	if s, ok := o.SeverityMap[key]; ok {
		return s, true
	}
	s, ok := severityNames[key]
	return s, ok
}

// ImportItem 导入内容中的一条规则及其预览结论。
type ImportItem struct {
	Index          int32
	SourceGroup    string
	Name           string
	Status         string
	Spec           *RuleSpec
	ExistingRuleID string
	Error          string
	Warnings       []string
	// duplicate 与文件内更靠前的规则重名（内部标记）。
	duplicate bool
}

// ImportResult 一条规则的导入结果。
type ImportResult struct {
	Index  int32
	Name   string
	Action string
	RuleID string
	Error  string
}

// ruleFile Prometheus / vmalert 规则文件。导出复用同一组结构。
type ruleFile struct {
	Groups []ruleFileGroup `yaml:"groups"`
}

type ruleFileGroup struct {
	Name     string      `yaml:"name"`
	Interval string      `yaml:"interval,omitempty"`
	Rules    []yaml.Node `yaml:"rules"`
}

type ruleFileRule struct {
	Alert         string            `yaml:"alert,omitempty"`
	Record        string            `yaml:"record,omitempty"`
	Expr          string            `yaml:"expr"`
	For           string            `yaml:"for,omitempty"`
	KeepFiringFor string            `yaml:"keep_firing_for,omitempty"`
	Labels        map[string]string `yaml:"labels,omitempty"`
	Annotations   map[string]string `yaml:"annotations,omitempty"`
}

// knownRuleFields 规则条目里认识的字段；其余在预览里提示已忽略。
var knownRuleFields = []string{"alert", "record", "expr", "for", "keep_firing_for", "labels", "annotations"}

// severityNames 导入时内置识别的 severity 标签取值（不区分大小写）。调用方给的对照表
// （ImportOptions.SeverityMap）优先于它；两边都不认识的采用默认级别并在预览里提示。
var severityNames = map[string]int32{
	"info": mc.SeverityInfo, "information": mc.SeverityInfo, "informational": mc.SeverityInfo, "notice": mc.SeverityInfo,
	"low": mc.SeverityInfo, "p4": mc.SeverityInfo, "1": mc.SeverityInfo,
	"warning": mc.SeverityWarning, "warn": mc.SeverityWarning, "minor": mc.SeverityWarning, "medium": mc.SeverityWarning,
	"p3": mc.SeverityWarning, "2": mc.SeverityWarning,
	"critical": mc.SeverityCritical, "crit": mc.SeverityCritical, "error": mc.SeverityCritical, "major": mc.SeverityCritical,
	"high": mc.SeverityCritical, "p2": mc.SeverityCritical, "3": mc.SeverityCritical,
	"fatal": mc.SeverityFatal, "emergency": mc.SeverityFatal, "disaster": mc.SeverityFatal, "page": mc.SeverityFatal,
	"p1": mc.SeverityFatal, "p0": mc.SeverityFatal, "4": mc.SeverityFatal,
}

// exportSeverityNames 导出时写入 severity 标签的取值；与 severityNames 互逆，导出再导入级别不变。
var exportSeverityNames = map[int32]string{
	mc.SeverityInfo: "info", mc.SeverityWarning: "warning", mc.SeverityCritical: "critical", mc.SeverityFatal: "fatal",
}

func importInvalid(format string, args ...any) error {
	return detail(ErrRuleImportInvalid, format, args...)
}

// errImportExprBytes 表达式总量超限。
func errImportExprBytes() error {
	return importInvalid("expressions add up to more than %d bytes in one file, please split it", maxImportExprBytes)
}

// checkYAMLTree 遍历语法树：拒绝锚点与别名，限制节点数与深度。
func checkYAMLTree(n *yaml.Node, depth int, count *int) error {
	if *count++; *count > maxYAMLNodes {
		return importInvalid("document is too large (more than %d nodes)", maxYAMLNodes)
	}
	if depth > maxYAMLDepth {
		return importInvalid("document is nested too deeply")
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return importInvalid("anchors and aliases are not supported")
	}
	for _, c := range n.Content {
		if err := checkYAMLTree(c, depth+1, count); err != nil {
			return err
		}
	}
	return nil
}

// parsedRule 规则文件中的一个条目（尚未转换）。
type parsedRule struct {
	group    string
	interval string
	rule     ruleFileRule
	unknown  []string
	err      error
}

// parseRuleFile 解析规则文件。整体结构不合法（不是 YAML、没有 groups、规则数超限）返回
// ErrRuleImportInvalid；单个条目的问题记在该条目上，不影响其余。
func parseRuleFile(content string) ([]parsedRule, error) {
	if len(content) > maxImportBytes {
		return nil, importInvalid("content exceeds %d bytes", maxImportBytes)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
		return nil, importInvalid("not valid YAML: %s", truncate(err.Error(), 200))
	}
	count := 0
	if err := checkYAMLTree(&doc, 0, &count); err != nil {
		return nil, err
	}
	var file ruleFile
	if err := doc.Decode(&file); err != nil {
		return nil, importInvalid("not a rule file: %s", truncate(err.Error(), 200))
	}
	if len(file.Groups) == 0 {
		return nil, importInvalid("no rule groups found (expecting a top-level `groups` list)")
	}
	var out []parsedRule
	exprBytes := 0
	for _, g := range file.Groups {
		for i := range g.Rules {
			if len(out) >= MaxImportRules {
				return nil, importInvalid("more than %d rules in one file, please split it", MaxImportRules)
			}
			p := parsedRule{group: g.Name, interval: g.Interval}
			node := &g.Rules[i]
			if node.Kind != yaml.MappingNode {
				p.err = fmt.Errorf("rule entry must be a mapping")
			} else if err := node.Decode(&p.rule); err != nil {
				p.err = fmt.Errorf("cannot read rule: %s", truncate(err.Error(), 200))
			} else {
				for k := 0; k+1 < len(node.Content); k += 2 {
					if key := node.Content[k].Value; !slices.Contains(knownRuleFields, key) {
						p.unknown = append(p.unknown, key)
					}
				}
			}
			if exprBytes += len(p.rule.Expr); exprBytes > maxImportExprBytes {
				return nil, errImportExprBytes()
			}
			out = append(out, p)
		}
	}
	return out, nil
}

// convertRule 把一个规则条目转换为预览项（Status 为 new / invalid / skipped；重名在之后判定）。
func convertRule(index int, p parsedRule, opts *ImportOptions) *ImportItem {
	item := &ImportItem{Index: int32(index), SourceGroup: p.group, Status: ImportNew} // #nosec G115 -- 至多 MaxImportRules
	fail := func(format string, args ...any) *ImportItem {
		item.Status, item.Spec, item.Error = ImportInvalid, nil, fmt.Sprintf(format, args...)
		return item
	}
	if p.err != nil {
		return fail("%v", p.err)
	}
	r := p.rule
	if r.Record != "" {
		item.Name, item.Status, item.Error = r.Record, ImportSkipped, "记录规则暂不支持，已跳过"
		return item
	}
	item.Name = strings.TrimSpace(r.Alert)
	if item.Name == "" {
		return fail("缺少 alert（规则名称）")
	}
	spec := &RuleSpec{
		GroupID: opts.GroupID, Name: item.Name, LevelInhibit: true, NotifyRecovery: true,
		DatasourceScope: opts.DatasourceScope, DatasourceIDs: slices.Clone(opts.DatasourceIDs),
		EvalIntervalS: defaultEvalIntervalS, Labels: map[string]string{}, Annotations: maps.Clone(r.Annotations),
		Notify: mc.NotifyTargets{
			UserIDs: slices.Clone(opts.Notify.UserIDs), ScheduleIDs: slices.Clone(opts.Notify.ScheduleIDs), DeptIDs: slices.Clone(opts.Notify.DeptIDs),
			CIOwner: opts.Notify.CIOwner,
		},
	}
	if len(p.unknown) > 0 {
		item.Warnings = append(item.Warnings, "已忽略无法识别的字段："+strings.Join(p.unknown, "、"))
	}
	if p.interval != "" {
		d, ok := parsePromDuration(p.interval)
		if !ok {
			return fail("组的 interval %q 不是合法的时长", p.interval)
		}
		secs := int32(min(max(int64(d.Seconds()), 15), 3600)) // #nosec G115 -- 已夹在 15-3600
		if int64(d.Seconds()) != int64(secs) {
			item.Warnings = append(item.Warnings, fmt.Sprintf("评估周期 %s 超出 15s-1h，已调整为 %s", p.interval, formatPromDuration(secs)))
		}
		spec.EvalIntervalS = secs
	}
	var ok bool
	if spec.ForS, ok = importSeconds(r.For); !ok {
		return fail("for %q 不是合法的时长（至多 1 天）", r.For)
	}
	if spec.RecoverS, ok = importSeconds(r.KeepFiringFor); !ok {
		return fail("keep_firing_for %q 不是合法的时长（至多 1 天）", r.KeepFiringFor)
	}
	severity := opts.DefaultSeverity
	for name, value := range r.Labels {
		if name == "severity" {
			if s, known := opts.severityOf(value); known {
				severity = s
			} else {
				item.Warnings = append(item.Warnings, fmt.Sprintf("无法识别的级别 %q，已采用默认级别", value))
			}
			continue
		}
		if value == "" {
			// Prometheus 的空标签值等价于没有这个标签：跳过而不是让整条规则不合法。
			item.Warnings = append(item.Warnings, fmt.Sprintf("附加标签 %s 的值为空，已忽略", name))
			continue
		}
		if strings.Contains(value, "{{") {
			item.Warnings = append(item.Warnings, fmt.Sprintf("附加标签 %s 的值含模板写法，将按原文保存（附加标签不支持模板）", name))
		}
		spec.Labels[name] = value
	}
	if _, has := r.Labels["severity"]; !has {
		item.Warnings = append(item.Warnings, "没有 severity 标签，已采用默认级别")
	}
	spec.Queries = []mc.Query{{Expr: r.Expr, Severity: severity}}
	if err := validateStatic(spec); err != nil {
		return fail("%s", errorMessage(err))
	}
	item.Spec = spec
	return item
}

// importSeconds 把 Prometheus 时长换成 0-86400 的整秒（不足一秒向上取整）；空串为 0。
func importSeconds(s string) (int32, bool) {
	if s == "" {
		return 0, true
	}
	d, ok := parsePromDuration(s)
	if !ok || d > 86400e9 {
		return 0, false
	}
	return int32((d + 999999999) / 1e9), true // #nosec G115 -- 已限制在 86400 秒内
}

// labelsKey 标签集的规范化文本（判断两条规则的附加标签是否相同）。
func labelsKey(labels map[string]string) string {
	keys := slices.Sorted(maps.Keys(labels))
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "\x00" + labels[k] + "\x00")
	}
	return b.String()
}

// prepareImport 解析并转换导入内容，标出与分组内现有规则、以及文件内彼此的重名。
// 要求对目标分组可管理；数据源与通知对象对整批相同，只校验一次。
func (uc *RuleUsecase) prepareImport(ctx context.Context, userID string, opts *ImportOptions) ([]*ImportItem, map[string]string, error) {
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if err := requireTargetGroup(scope, opts.GroupID); err != nil {
		return nil, nil, err
	}
	if !mc.ValidSeverity(opts.DefaultSeverity) {
		return nil, nil, ruleInvalid("default_severity must be 1-4")
	}
	if opts.SeverityMap, err = normalizeSeverityMap(opts.SeverityMap); err != nil {
		return nil, nil, err
	}
	if len(opts.Content) > maxImportBytes {
		return nil, nil, importInvalid("content exceeds %d bytes", maxImportBytes)
	}
	// 用一条占位规则走一遍静态校验，覆盖数据源范围与通知对象的个数 / 去重约束。
	probe := RuleSpec{
		Name: "probe", Queries: []mc.Query{{Expr: "up", Severity: opts.DefaultSeverity}}, EvalIntervalS: defaultEvalIntervalS,
		DatasourceScope: opts.DatasourceScope, DatasourceIDs: opts.DatasourceIDs, Notify: opts.Notify,
	}
	if err := validateStatic(&probe); err != nil {
		return nil, nil, err
	}
	if err := uc.validateDatasources(ctx, opts.DatasourceIDs, nil); err != nil {
		return nil, nil, err
	}
	if err := uc.validateNotify(ctx, opts.Notify, nil); err != nil {
		return nil, nil, err
	}
	var items []*ImportItem
	if isRuleExport(opts.Content) {
		if items, err = uc.convertExport(ctx, scope, opts); err != nil {
			return nil, nil, err
		}
	} else {
		parsed, err := parseRuleFile(opts.Content)
		if err != nil {
			return nil, nil, err
		}
		items = make([]*ImportItem, 0, len(parsed))
		for i, p := range parsed {
			// 语法校验是纯 CPU：逐条之间看一眼取消，调用方断开后不再算下去。
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
			items = append(items, convertRule(i, p, opts))
		}
	}
	var candidates []string
	for _, item := range items {
		if item.Status == ImportNew {
			candidates = append(candidates, item.Name)
		}
	}
	existing := map[string]string{}
	if candidates = uniq(candidates); len(candidates) > 0 {
		if existing, err = uc.lookup.FindByNames(ctx, opts.GroupID, candidates); err != nil {
			return nil, nil, err
		}
	}
	first := make(map[string]*ImportItem, len(items))
	for _, item := range items {
		if item.Status != ImportNew {
			continue
		}
		if id, ok := existing[item.Name]; ok {
			item.Status, item.ExistingRuleID = ImportConflict, id
		}
		prev, dup := first[item.Name]
		if !dup {
			first[item.Name] = item
			continue
		}
		item.Status, item.duplicate = ImportConflict, true
		warn := fmt.Sprintf("与文件内第 %d 条规则重名", prev.Index+1)
		if labelsKey(prev.Spec.Labels) == labelsKey(item.Spec.Labels) && prev.Spec.Queries[0].Severity != item.Spec.Queries[0].Severity {
			warn += "；两者只有阈值与级别不同，可在导入后合并为一条多级阈值规则"
		}
		item.Warnings = append(item.Warnings, warn)
	}
	return items, existing, nil
}

// PreviewImport 导入预览：逐条给出转换结果、重名情况与无法转换的原因，不落库。
func (uc *RuleUsecase) PreviewImport(ctx context.Context, userID string, opts ImportOptions) ([]*ImportItem, error) {
	items, _, err := uc.prepareImport(ctx, userID, &opts)
	return items, err
}

// Import 导入规则到指定分组：逐条独立事务，结果逐条返回。文件内彼此重名的规则，除「跳过」外
// 一律改名导入（覆盖会让后一条冲掉前一条）；不会自动合并多级阈值。
func (uc *RuleUsecase) Import(ctx context.Context, userID string, opts ImportOptions, onConflict string, enabled bool) ([]ImportResult, error) {
	if onConflict != ConflictSkip && onConflict != ConflictOverwrite && onConflict != ConflictRename {
		return nil, ruleInvalid("on_conflict must be skip, overwrite or rename")
	}
	items, _, err := uc.prepareImport(ctx, userID, &opts)
	if err != nil {
		return nil, err
	}
	results := make([]ImportResult, 0, len(items))
	for _, item := range items {
		res := ImportResult{Index: item.Index, Name: item.Name}
		switch item.Status {
		case ImportSkipped:
			res.Action, res.Error = ImportSkippedRule, item.Error
		case ImportInvalid:
			res.Action, res.Error = ImportFailed, item.Error
		default:
			item.Spec.Enabled = enabled
			rule, action, err := uc.importOne(ctx, userID, item, onConflict)
			switch {
			case err != nil && ctx.Err() != nil:
				return nil, ctx.Err()
			case err != nil:
				res.Action, res.Error = ImportFailed, publicMessage(err)
			case rule == nil:
				res.Action, res.Error = ImportSkippedRule, "分组内已有同名规则"
			default:
				res.Action, res.RuleID, res.Name = action, rule.ID, rule.Spec.Name
			}
		}
		results = append(results, res)
	}
	return results, nil
}

// publicMessage 可以展示给调用方的错误说明：内部错误不透出细节。
func publicMessage(err error) string {
	if e := kerrors.FromError(err); e.Code < 500 {
		return e.Message
	}
	return "internal error"
}

// importOne 导入一条规则（一个事务）。返回 (nil, "", nil) 表示按策略跳过。
//
// 覆盖只针对「此刻仍在目标分组里、仍叫这个名字」的那条规则：预览与导入之间它可能被移到别的分组
// （操作者对那个分组未必可管理）或改了名，这时它已不是重名规则，不能再覆盖——否则等于把别的分组
// 的规则改写后拉回来。读取之后的并发改动由版本号乐观锁挡住。
func (uc *RuleUsecase) importOne(ctx context.Context, userID string, item *ImportItem, onConflict string) (*Rule, string, error) {
	change := RuleChange{Action: mc.RuleActionImport, Summary: "导入自规则文件", Operator: userID}
	if item.Status == ImportNew {
		rule, err := uc.insert(ctx, *item.Spec, change)
		if !kerrors.Is(err, ErrRuleNameExists) {
			return rule, ImportCreated, err
		}
		// 预览之后有人建了同名规则：按重名策略继续处理。
		existing, err := uc.lookup.FindByNames(ctx, item.Spec.GroupID, []string{item.Name})
		if err != nil {
			return nil, "", err
		}
		item.ExistingRuleID = existing[item.Name]
	}
	switch {
	case onConflict == ConflictSkip:
		return nil, "", nil
	case onConflict == ConflictOverwrite && !item.duplicate && item.ExistingRuleID != "":
		rule, err := uc.reader.Get(ctx, item.ExistingRuleID)
		switch {
		case kerrors.Is(err, ErrRuleNotFound):
		case err != nil:
			return nil, "", err
		case rule.Spec.GroupID == item.Spec.GroupID && rule.Spec.Name == item.Name:
			old := rule.Spec
			rule.Spec = *item.Spec
			if err := uc.save(ctx, rule, old, rule.Version, change); err != nil {
				return nil, "", err
			}
			return rule, ImportUpdated, nil
		}
		// 预览之后那条规则被删除、移走或改名：名称多半已空出来，按新建处理（仍重名则自动加序号）。
	}
	rule, err := uc.insertNumbered(ctx, *item.Spec, item.Name, change)
	return rule, ImportCreated, err
}

// Export 导出规则：ids 与 groupID 至少给一个，都给时取交集；只导出数据范围内的规则。
//   - ExportYAML（缺省）：Prometheus / vmalert 规则 YAML。每个分组（按评估周期再分）一个 group，
//     级别写入 severity 标签，多档阈值的规则按级别拆成多条；本系统特有的配置（生效时间、通知对象
//     等）不在其中。
//   - ExportJSON：本系统的完整格式（见 ruleExport），可原样再导入。
//
// 给了 ids 就按 ids 查（至多 MaxBatchRules 条），分组只用来筛掉不属于它（含下级）的规则；不能反过来
// 先按分组取前 MaxExportRules 条再挑 ids——分组里的规则多于上限时，排在后面的所选规则会被静默丢掉。
func (uc *RuleUsecase) Export(ctx context.Context, userID string, ids []string, groupID, format string) (content string, count int, err error) {
	if format != "" && format != ExportYAML && format != ExportJSON {
		return "", 0, ruleInvalid("format must be yaml or json")
	}
	if len(ids) > MaxBatchRules || groupID == "" && len(ids) == 0 {
		return "", 0, ruleInvalid("select 1-%d rules or a group to export", MaxBatchRules)
	}
	scope, err := uc.scopes.Resolve(ctx, userID)
	if err != nil {
		return "", 0, err
	}
	if groupID != "" {
		if err := scope.requireRead(groupID); err != nil {
			return "", 0, err
		}
		// 分组不存在（含已删除）时明确报错，而不是回一份空文件（与新建 / 移动 / 导入同一口径）。放在可读
		// 判定之后：对无权查看的分组仍按无权限处理，不泄露它是否存在。
		if !scope.GroupExists(groupID) {
			return "", 0, ErrGroupNotFound
		}
	}
	var rules []*Rule
	if len(ids) > 0 {
		if rules, err = uc.reader.ListByIDs(ctx, ids); err != nil {
			return "", 0, err
		}
		inGroup := func(string) bool { return true }
		if groupID != "" {
			within := scope.Filter(groupID).IDs
			inGroup = func(id string) bool { return slices.Contains(within, id) }
		}
		rules = slices.DeleteFunc(rules, func(r *Rule) bool { return !scope.CanRead(r.Spec.GroupID) || !inGroup(r.Spec.GroupID) })
	} else if rules, err = uc.lookup.ListByGroups(ctx, scope.Filter(groupID), MaxExportRules+1); err != nil {
		return "", 0, err
	}
	if len(rules) > MaxExportRules {
		return "", 0, ruleInvalid("more than %d rules to export, please narrow the selection", MaxExportRules)
	}
	var out []byte
	if format == ExportJSON {
		out, err = uc.exportJSON(ctx, scope, rules)
	} else {
		out, err = yaml.Marshal(exportFile(scope, rules))
	}
	if err != nil {
		return "", 0, fmt.Errorf("encode rule file: %w", err)
	}
	return string(out), len(rules), nil
}

// exportGroup 导出文件里的一个 group（Rules 用具体类型，便于直接编码）。
type exportGroup struct {
	Name     string         `yaml:"name"`
	Interval string         `yaml:"interval"`
	Rules    []ruleFileRule `yaml:"rules"`
}

// exportFile 按（分组, 评估周期）把规则归入 group；同一分组有多种评估周期时组名带上周期。
func exportFile(scope Scope, rules []*Rule) map[string][]*exportGroup {
	slices.SortStableFunc(rules, func(a, b *Rule) int {
		if c := strings.Compare(scope.GroupName(a.Spec.GroupID), scope.GroupName(b.Spec.GroupID)); c != 0 {
			return c
		}
		return strings.Compare(a.Spec.Name, b.Spec.Name)
	})
	intervals := map[string][]int32{}
	for _, r := range rules {
		if !slices.Contains(intervals[r.Spec.GroupID], r.Spec.EvalIntervalS) {
			intervals[r.Spec.GroupID] = append(intervals[r.Spec.GroupID], r.Spec.EvalIntervalS)
		}
	}
	groups := make([]*exportGroup, 0, len(intervals))
	index := map[string]*exportGroup{}
	for _, r := range rules {
		interval := formatPromDuration(r.Spec.EvalIntervalS)
		key := r.Spec.GroupID + "/" + interval
		g, ok := index[key]
		if !ok {
			name := scope.GroupName(r.Spec.GroupID)
			if name == "" {
				name = r.Spec.GroupID
			}
			if len(intervals[r.Spec.GroupID]) > 1 {
				name += " (" + interval + ")"
			}
			g = &exportGroup{Name: name, Interval: interval}
			index[key] = g
			groups = append(groups, g)
		}
		for _, q := range r.Spec.Queries {
			labels := maps.Clone(r.Spec.Labels)
			if labels == nil {
				labels = map[string]string{}
			}
			labels["severity"] = exportSeverityNames[q.Severity]
			rule := ruleFileRule{Alert: r.Spec.Name, Expr: q.Expr, Labels: labels, Annotations: r.Spec.Annotations}
			if r.Spec.ForS > 0 {
				rule.For = formatPromDuration(r.Spec.ForS)
			}
			if r.Spec.RecoverS > 0 {
				rule.KeepFiringFor = formatPromDuration(r.Spec.RecoverS)
			}
			g.Rules = append(g.Rules, rule)
		}
	}
	return map[string][]*exportGroup{"groups": groups}
}
