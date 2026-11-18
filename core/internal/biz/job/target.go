package biz

import (
	"context"
	"sort"
	"strings"

	jc "github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// normalizeTarget 规整并校验目标主机配置的形状（与 CMDB 无关的部分）。
func normalizeTarget(t *jc.Target) error {
	t.ModelCode = strings.TrimSpace(t.ModelCode)
	t.AddressField = strings.TrimSpace(t.AddressField)
	if t.ModelCode == "" {
		return detail(ErrTargetInvalid, "resource model is required")
	}
	switch t.Mode {
	case jc.TargetModeCIs:
		t.CIIDs = uniq(t.CIIDs)
		if len(t.CIIDs) == 0 {
			return detail(ErrTargetInvalid, "select at least one resource")
		}
		if len(t.CIIDs) > jc.MaxHosts {
			return detail(ErrTargetInvalid, "at most %d resources can be selected", jc.MaxHosts)
		}
		t.Env, t.Tag, t.Keyword = "", "", ""
	case jc.TargetModeFilter:
		t.CIIDs = nil
		t.Env, t.Tag, t.Keyword = strings.TrimSpace(t.Env), strings.TrimSpace(t.Tag), strings.TrimSpace(t.Keyword)
	default:
		return detail(ErrTargetInvalid, "unknown target mode")
	}
	return nil
}

// TargetUsecase 目标主机解析：把 CMDB 资源变成可执行的主机（地址校验、去重）。
// 全部 CMDB 读取经 HostSource，按调用人的数据范围判定。
type TargetUsecase struct {
	source HostSource
}

// NewTargetUsecase new a TargetUsecase.
func NewTargetUsecase(source HostSource) *TargetUsecase { return &TargetUsecase{source: source} }

// check 校验目标主机配置（形状 + CMDB 侧：类型在数据范围内、地址字段可用）。
func (uc *TargetUsecase) check(ctx context.Context, userID string, target *jc.Target) error {
	if err := normalizeTarget(target); err != nil {
		return err
	}
	return uc.source.Check(ctx, userID, *target)
}

// Resolve 解析目标主机，至多 jobcontract.MaxHosts 台；每台主机的地址校验结果在 Host.Problem。
func (uc *TargetUsecase) Resolve(ctx context.Context, userID string, target jc.Target) (HostSet, error) {
	return uc.ResolveN(ctx, userID, target, jc.MaxHosts)
}

// ResolveN 同 Resolve，上限由调用方给出（全局策略收紧的上限，或切片时放宽到每片上限 × 片数）。
func (uc *TargetUsecase) ResolveN(ctx context.Context, userID string, target jc.Target, limit int) (HostSet, error) {
	if err := normalizeTarget(&target); err != nil {
		return HostSet{}, err
	}
	set, err := uc.source.Resolve(ctx, userID, target, limit)
	if err != nil {
		return HostSet{}, err
	}
	// 按条件筛选的主机按名称排序：滚动执行的批次、切片的划分、日志里的顺序都因此稳定可预期，
	// 不随资源在 CMDB 里的更新时间漂移。指定资源的方式保持用户选择的顺序。
	if target.Mode == jc.TargetModeFilter {
		sort.SliceStable(set.Items, func(i, j int) bool {
			if set.Items[i].Name != set.Items[j].Name {
				return set.Items[i].Name < set.Items[j].Name
			}
			return set.Items[i].ID < set.Items[j].ID
		})
	}
	return HostSet{Hosts: buildHosts(set.Items), Truncated: set.Truncated, OutOfScope: set.Skipped}, nil
}

// buildHosts 校验并规范化各资源的地址；同一地址只有第一台可执行（地址是 Ansible 清单里的主机名，必须唯一）。
func buildHosts(items []Resource) []Host {
	hosts := make([]Host, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, r := range items {
		h := Host{CIID: r.ID, Name: r.Name, Env: r.Env, Address: strings.TrimSpace(r.Address)}
		addr, ok := jc.NormalizeAddress(r.Address)
		switch {
		case h.Address == "":
			h.Problem = ProblemAddressEmpty
		case !ok:
			h.Problem = ProblemAddressInvalid
			// 原始取值可能很长或带控制字符：只留一段可打印的前缀用于展示。
			h.Address = printable(h.Address, 64)
		default:
			h.Address = addr
			if _, dup := seen[addr]; dup {
				h.Problem = ProblemAddressDuplicate
			}
			seen[addr] = struct{}{}
		}
		hosts = append(hosts, h)
	}
	return hosts
}

// printable 截取至多 n 个字符，并把不可打印字符替换为 ?。
func printable(s string, n int) string {
	var b strings.Builder
	for i, r := range []rune(s) {
		if i >= n {
			b.WriteString("…")
			break
		}
		if r < 0x20 || r == 0x7f {
			r = '?'
		}
		b.WriteRune(r)
	}
	return b.String()
}
