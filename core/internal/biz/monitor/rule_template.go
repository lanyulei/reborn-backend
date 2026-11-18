package biz

import (
	"cmp"
	"embed"
	"slices"
	"strings"

	mc "github.com/lanyulei/reborn/backend/kit/monitorcontract"
)

// 内置规则模板包：静态内容，随程序嵌入。每个包是一份 Prometheus / vmalert 规则 YAML，前端取到原文后
// 走已有的导入预览 / 导入接口——模板不另开一条写入路径，校验、重名处理、留痕都与手工导入相同。

//go:embed templates/*.yaml
var templateFS embed.FS

// 模板包类别。
const (
	TemplateCategoryHost       = "host"
	TemplateCategoryMiddleware = "middleware"
	TemplateCategoryDatabase   = "database"
	TemplateCategoryPlatform   = "platform"
)

// TemplatePack 一个内置规则模板包。
type TemplatePack struct {
	Code        string
	Name        string
	Description string
	Category    string
	RuleCount   int32
}

// TemplateItem 模板包里的一条规则（摘要）。
type TemplateItem struct {
	Name     string
	Expr     string
	Severity int32
	ForS     int32
	Summary  string
}

// TemplatePackDetail 模板包及其内容。
type TemplatePackDetail struct {
	TemplatePack
	Rules   []TemplateItem
	Content string
}

// templateCatalog 模板包目录（code 即 templates/<code>.yaml 的文件名）。
var templateCatalog = []TemplatePack{
	{
		Code: "node-exporter", Name: "主机（node_exporter）", Category: TemplateCategoryHost,
		Description: "Linux 主机的 CPU、内存、磁盘、网络、文件描述符、时钟与 systemd 服务。依赖 node_exporter（node_* 指标）；「主机失联」按 job 名含 node 的 up 指标判断。",
	},
	{
		Code: "mysql", Name: "MySQL（mysqld_exporter）", Category: TemplateCategoryDatabase,
		Description: "MySQL 的可用性、连接数、慢查询、主从复制与 InnoDB。依赖 mysqld_exporter（mysql_* 指标）。",
	},
	{
		Code: "redis", Name: "Redis（redis_exporter）", Category: TemplateCategoryMiddleware,
		Description: "Redis 的可用性、内存、连接数、淘汰、持久化与主从复制。依赖 redis_exporter（redis_* 指标）。",
	},
	{
		Code: "nginx-blackbox", Name: "Nginx 与拨测（blackbox_exporter）", Category: TemplateCategoryMiddleware,
		Description: "Nginx 连接状态（nginx-prometheus-exporter，nginx_* 指标），以及站点拨测、HTTP 状态码、TLS 证书到期与 DNS 解析（blackbox_exporter，probe_* 指标）。",
	},
	{
		Code: "kafka", Name: "Kafka（kafka_exporter）", Category: TemplateCategoryMiddleware,
		Description: "Kafka 的 Broker 数量、分区副本同步、Leader 与消费组积压。依赖 kafka_exporter（kafka_* 指标）。",
	},
	{
		Code: "reborn-self", Name: "本平台自监控", Category: TemplateCategoryPlatform,
		Description: "本平台各服务（core、通知管道、CMDB 采集、评估引擎）/metrics 暴露的指标：后台循环失败、流程任务积压与死信、通知积压 / 死信 / 熔断、规则评估失败与延迟。需要把这些服务的 /metrics 接入采集。",
	},
}

// templatePacks 全部模板包（已解析），进程启动时装载一次、之后只读。内容随程序嵌入，解析失败是构建
// 缺陷：初始化期即暴露（有测试逐包校验）。
var templatePacks = func() []TemplatePackDetail {
	out := make([]TemplatePackDetail, 0, len(templateCatalog))
	for _, pack := range templateCatalog {
		raw, err := templateFS.ReadFile("templates/" + pack.Code + ".yaml")
		if err != nil {
			panic("monitor: embedded rule template " + pack.Code + ": " + err.Error())
		}
		d := TemplatePackDetail{TemplatePack: pack, Content: string(raw)}
		parsed, err := parseRuleFile(d.Content)
		if err != nil {
			panic("monitor: embedded rule template " + pack.Code + ": " + err.Error())
		}
		opts := &ImportOptions{}
		for _, p := range parsed {
			if p.err != nil || p.rule.Alert == "" {
				continue
			}
			item := TemplateItem{Name: p.rule.Alert, Expr: p.rule.Expr, Severity: mc.SeverityWarning}
			if s, ok := opts.severityOf(p.rule.Labels["severity"]); ok {
				item.Severity = s
			}
			item.ForS, _ = importSeconds(p.rule.For)
			if item.Summary = p.rule.Annotations["summary"]; item.Summary == "" {
				item.Summary = p.rule.Annotations["description"]
			}
			d.Rules = append(d.Rules, item)
		}
		d.RuleCount = int32(len(d.Rules)) // #nosec G115 -- 内置内容，每包几十条
		out = append(out, d)
	}
	slices.SortFunc(out, func(a, b TemplatePackDetail) int {
		return cmp.Or(strings.Compare(a.Category, b.Category), strings.Compare(a.Name, b.Name))
	})
	return out
}()

// TemplatePacks 列出内置规则模板包（按类别、名称排序）。返回副本。
func (uc *RuleUsecase) TemplatePacks() []TemplatePack {
	out := make([]TemplatePack, 0, len(templatePacks))
	for i := range templatePacks {
		out = append(out, templatePacks[i].TemplatePack)
	}
	return out
}

// TemplatePack 取一个模板包的内容；编码不存在返回 ErrRuleTemplateNotFound。返回副本。
func (uc *RuleUsecase) TemplatePack(code string) (*TemplatePackDetail, error) {
	for i := range templatePacks {
		if templatePacks[i].Code == code {
			d := templatePacks[i]
			d.Rules = slices.Clone(d.Rules)
			return &d, nil
		}
	}
	return nil, ErrRuleTemplateNotFound
}
