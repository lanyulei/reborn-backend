// Package cmdbcontract 是 core 控制面与 cmdb 数据面（backend/cmdb 采集/对账进程）之间的
// 数据契约：同库不同进程，不跨模块 import 业务代码，只共享"落库长什么样"——状态字面量、
// 来源格式、jsonb 列结构、采集源配置的解码与校验、唯一键拼接规则。任何一方改契约必须改
// 这里，由编译器把漂移点全部暴露。
package cmdbcontract

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// 资源状态（cmdb_ci.status）。
const (
	CIStatusPlanning    = "planning"
	CIStatusActive      = "active"
	CIStatusMaintenance = "maintenance"
	CIStatusRetired     = "retired"
)

// 字段归属（cmdb_model_field.owner）：manual 人工 / sync 同步只读 / both 同步写入可人工覆盖。
const (
	FieldOwnerManual = "manual"
	FieldOwnerSync   = "sync"
	FieldOwnerBoth   = "both"
)

// 历史动作（cmdb_ci_history.action）。
const (
	ActionCreate     = "create"
	ActionUpdate     = "update"
	ActionTransition = "transition"
	ActionDelete     = "delete"
	ActionLink       = "link"
	ActionUnlink     = "unlink"
)

// 关系端通配（cmdb_relation_type.src_model / dst_model）。
const ModelAny = "*"

// 关系删除策略（cmdb_relation_type.on_delete）：src 退役时对 dst 的处理。
const (
	OnDeleteRestrict      = "restrict"
	OnDeleteCascadeRetire = "cascade_retire"
)

// SourceManual 人工来源（cmdb_ci.source / cmdb_relation.source / cmdb_ci_history.source）。
const SourceManual = "manual"

// 关系方向（相对当前资源）。
const (
	DirectionOutgoing = "outgoing"
	DirectionIncoming = "incoming"
)

// 采集源提供方（cmdb_sync_source.provider）。
const (
	ProviderAliyun  = "aliyun"
	ProviderVMware  = "vmware"
	ProviderTencent = "tencent"
	ProviderAWS     = "aws"
)

// 同步任务触发方式（cmdb_sync_task.trigger）。
const (
	TriggerManual   = "manual"
	TriggerSchedule = "schedule"
	TriggerWebhook  = "webhook"
)

// 回调触发（数据面入站）：路径 + 令牌头；令牌明文 64 位 hex，库中只存 SHA-256。
const (
	WebhookPathPrefix  = "/webhook/v1/cmdb/sync/"
	WebhookTokenHeader = "X-Cmdb-Webhook-Token"
	// WebhookMinInterval 同一采集源两次回调触发的最小间隔。
	WebhookMinInterval = 60 * time.Second
)

// WebhookPath 采集源的回调路径。
func WebhookPath(sourceID string) string { return WebhookPathPrefix + sourceID }

// MaskedValue 敏感属性读取时的占位值（写入时遇到该值视为"未修改"）。
const MaskedValue = "******"

// FieldMapKeyPattern 字段映射键/值（采集器输出键 → 模型字段编码）。
var FieldMapKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// 同步任务状态（cmdb_sync_task.status）：pending → running → succeeded | failed。
const (
	TaskPending   = "pending"
	TaskRunning   = "running"
	TaskSucceeded = "succeeded"
	TaskFailed    = "failed"
)

// 同步运行状态（cmdb_sync_run.status / cmdb_sync_source.last_status）。
const (
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	RunFailed    = "failed"
	// RunNever 采集源尚未运行过（cmdb_sync_source.last_status 缺省）。
	RunNever = "never"
)

// 任务最大尝试次数（租约过期被重领计一次）。
const TaskMaxAttempts = 3

// 分布式锁键（数据面选主）。
const (
	LockScheduler   = "cmdb:scheduler"
	LockMaintenance = "cmdb:maintenance"
)

// 采集源周期与宽限期边界（秒）。
const (
	MinIntervalSeconds = 300
	MaxIntervalSeconds = 7 * 24 * 3600
	MaxGraceSeconds    = 30 * 24 * 3600
)

// 内置类型编码（迁移 0032 预置）；采集器输出固定映射到这些类型。
const (
	ModelPhysicalServer = "physical_server"
	ModelVirtualMachine = "virtual_machine"
	ModelCloudHost      = "cloud_host"
)

// 内置关系类型编码。
const RelationHostRunsVM = "host_runs_vm"

// Providers 全部提供方（顺序即展示顺序）。
func Providers() []string {
	return []string{ProviderAliyun, ProviderTencent, ProviderAWS, ProviderVMware}
}

// ProviderModels 提供方写入的资源类型（core 展示用；数据面按此过滤陈旧判定范围）。
func ProviderModels(provider string) []string {
	switch provider {
	case ProviderAliyun, ProviderTencent, ProviderAWS:
		return []string{ModelCloudHost}
	case ProviderVMware:
		return []string{ModelPhysicalServer, ModelVirtualMachine}
	default:
		return nil
	}
}

// SourceOf 采集源的来源字面量（cmdb_ci.source = sync:<源 ID>）。
func SourceOf(sourceID string) string { return "sync:" + sourceID }

// IsSyncSource 报告来源是否同步写入。
func IsSyncSource(source string) bool { return strings.HasPrefix(source, "sync:") }

// UniqueKey 按类型唯一字段拼接资源唯一键：唯一字段 code 升序，`code=value|...`，
// 缺失/nil 值跳过，空串不参与唯一约束。core 写路径与数据面对账写入必须一致。
func UniqueKey(uniqueCodes []string, attrs map[string]any) string {
	codes := make([]string, len(uniqueCodes))
	copy(codes, uniqueCodes)
	sort.Strings(codes)
	parts := make([]string, 0, len(codes))
	for _, code := range codes {
		v, ok := attrs[code]
		if !ok || v == nil {
			continue
		}
		parts = append(parts, code+"="+uniqueKeyText(v))
	}
	return strings.Join(parts, "|")
}

// uniqueKeyText 把属性值转成与 SQL `attrs ->> code` 一致的文本：core 的 SQL
// 重算（stmtRecomputeUniqueKey）与数据面对账（stmtUniqueKey）都以 jsonb 文本
// 为准。fmt.Sprint 会把 float64(1000000) 写成 1e+06，与 jsonb 的 1000000
// 不一致——同一资源在 Go 与 SQL 两条路径上算出不同的唯一键。
func uniqueKeyText(v any) string {
	switch n := v.(type) {
	case float64:
		return strconv.FormatFloat(n, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(n), 'f', -1, 32)
	case json.Number:
		return n.String()
	default:
		return fmt.Sprint(v)
	}
}

// jsonb 列结构（json 标签即落库 schema；数据面以 DisallowUnknownFields 解码——
// 字段增删必须两端同时发布）。
type (
	// AliyunConfig cmdb_sync_source.config（provider=aliyun）：ECS DescribeInstances。
	// 凭据 AccessKeySecret 单独加密存 credential 列。
	AliyunConfig struct {
		RegionID    string `json:"region_id"`
		AccessKeyID string `json:"access_key_id"`
		// Endpoint 可选，缺省 https://ecs.aliyuncs.com（测试/私有化接入用）。
		Endpoint string `json:"endpoint,omitempty"`
		// FieldMap 采集器输出键 → 模型字段编码的重命名映射，可选。
		FieldMap map[string]string `json:"field_map,omitempty"`
	}
	// TencentConfig cmdb_sync_source.config（provider=tencent）：CVM DescribeInstances。
	// 凭据 SecretKey 单独加密存 credential 列。
	TencentConfig struct {
		Region   string `json:"region"`
		SecretID string `json:"secret_id"`
		// Endpoint 可选，缺省 https://cvm.tencentcloudapi.com。
		Endpoint string            `json:"endpoint,omitempty"`
		FieldMap map[string]string `json:"field_map,omitempty"`
	}
	// AWSConfig cmdb_sync_source.config（provider=aws）：EC2 DescribeInstances。
	// 凭据 SecretAccessKey 单独加密存 credential 列。
	AWSConfig struct {
		Region      string `json:"region"`
		AccessKeyID string `json:"access_key_id"`
		// Endpoint 可选，缺省 https://ec2.<region>.amazonaws.com。
		Endpoint string            `json:"endpoint,omitempty"`
		FieldMap map[string]string `json:"field_map,omitempty"`
	}
	// VMwareConfig cmdb_sync_source.config（provider=vmware）：vCenter SOAP（govmomi）。
	// 凭据 password 单独加密存 credential 列。
	VMwareConfig struct {
		URL      string `json:"url"`
		Username string `json:"username"`
		// Insecure 跳过 TLS 证书校验（自签证书环境）。
		Insecure bool              `json:"insecure,omitempty"`
		FieldMap map[string]string `json:"field_map,omitempty"`
	}
	// StagingRelation cmdb_sync_staging.relations 元素：当前资源与对端（同源同步写入）的关系。
	StagingRelation struct {
		Type           string `json:"type"`
		Direction      string `json:"direction"`
		PeerModel      string `json:"peer_model"`
		PeerExternalID string `json:"peer_external_id"`
	}
	// RunError cmdb_sync_run.errors 元素（样本，最多 50 条）。
	RunError struct {
		ModelCode  string `json:"model_code"`
		ExternalID string `json:"external_id"`
		Name       string `json:"name"`
		Reason     string `json:"reason"`
	}
)

// RunErrorSampleLimit 单次运行记录的错误样本上限。
const RunErrorSampleLimit = 50

// ErrConfigInvalid 采集源配置不合法。
var ErrConfigInvalid = errors.New("cmdbcontract: invalid source config")

// DecodeConfig 解码并校验采集源配置（provider 决定结构）；core 写入前与数据面读取时同用。
func DecodeConfig(provider string, raw []byte) (any, error) {
	switch provider {
	case ProviderAliyun:
		var c AliyunConfig
		if err := strictDecode(raw, &c); err != nil {
			return nil, err
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return &c, nil
	case ProviderVMware:
		var c VMwareConfig
		if err := strictDecode(raw, &c); err != nil {
			return nil, err
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return &c, nil
	case ProviderTencent:
		var c TencentConfig
		if err := strictDecode(raw, &c); err != nil {
			return nil, err
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return &c, nil
	case ProviderAWS:
		var c AWSConfig
		if err := strictDecode(raw, &c); err != nil {
			return nil, err
		}
		if err := c.Validate(); err != nil {
			return nil, err
		}
		return &c, nil
	default:
		return nil, fmt.Errorf("%w: unknown provider %q", ErrConfigInvalid, provider)
	}
}

func strictDecode(raw []byte, out any) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: config is empty", ErrConfigInvalid)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("%w: %v", ErrConfigInvalid, err)
	}
	return nil
}

// Validate 校验阿里云配置。
func (c *AliyunConfig) Validate() error {
	switch {
	case c.RegionID == "" || len(c.RegionID) > 64:
		return fmt.Errorf("%w: region_id is required (≤64)", ErrConfigInvalid)
	case c.AccessKeyID == "" || len(c.AccessKeyID) > 128:
		return fmt.Errorf("%w: access_key_id is required (≤128)", ErrConfigInvalid)
	case c.Endpoint != "":
		if err := validateHTTPURL(c.Endpoint); err != nil {
			return err
		}
	}
	return validateFieldMap(c.FieldMap)
}

// Validate 校验腾讯云配置。
func (c *TencentConfig) Validate() error {
	switch {
	case c.Region == "" || len(c.Region) > 64:
		return fmt.Errorf("%w: region is required (≤64)", ErrConfigInvalid)
	case c.SecretID == "" || len(c.SecretID) > 128:
		return fmt.Errorf("%w: secret_id is required (≤128)", ErrConfigInvalid)
	case c.Endpoint != "":
		if err := validateHTTPURL(c.Endpoint); err != nil {
			return err
		}
	}
	return validateFieldMap(c.FieldMap)
}

// Validate 校验 AWS 配置。
func (c *AWSConfig) Validate() error {
	switch {
	case c.Region == "" || len(c.Region) > 64:
		return fmt.Errorf("%w: region is required (≤64)", ErrConfigInvalid)
	case c.AccessKeyID == "" || len(c.AccessKeyID) > 128:
		return fmt.Errorf("%w: access_key_id is required (≤128)", ErrConfigInvalid)
	case c.Endpoint != "":
		if err := validateHTTPURL(c.Endpoint); err != nil {
			return err
		}
	}
	return validateFieldMap(c.FieldMap)
}

// Validate 校验 VMware 配置。
func (c *VMwareConfig) Validate() error {
	if err := validateHTTPURL(c.URL); err != nil {
		return err
	}
	if c.Username == "" || len(c.Username) > 128 {
		return fmt.Errorf("%w: username is required (≤128)", ErrConfigInvalid)
	}
	return validateFieldMap(c.FieldMap)
}

// FieldMapOf 返回配置中的字段映射（任何提供方）；配置非法返回 nil。
func FieldMapOf(provider string, raw []byte) map[string]string {
	cfg, err := DecodeConfig(provider, raw)
	if err != nil {
		return nil
	}
	switch c := cfg.(type) {
	case *AliyunConfig:
		return c.FieldMap
	case *TencentConfig:
		return c.FieldMap
	case *AWSConfig:
		return c.FieldMap
	case *VMwareConfig:
		return c.FieldMap
	default:
		return nil
	}
}

func validateFieldMap(m map[string]string) error {
	if len(m) > 64 {
		return fmt.Errorf("%w: field_map has too many entries (≤64)", ErrConfigInvalid)
	}
	for k, v := range m {
		if !FieldMapKeyPattern.MatchString(k) || !FieldMapKeyPattern.MatchString(v) {
			return fmt.Errorf("%w: field_map entry %q → %q must match %s", ErrConfigInvalid, k, v, FieldMapKeyPattern)
		}
	}
	return nil
}

func validateHTTPURL(raw string) error {
	if raw == "" || len(raw) > 512 {
		return fmt.Errorf("%w: url is required (≤512)", ErrConfigInvalid)
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return fmt.Errorf("%w: url must be http(s) without embedded credentials", ErrConfigInvalid)
	}
	return nil
}
