package biz

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// 指挥与处理人的解析（功能设计 §4.1 / §4.2）：
//   指挥：规则指定用户 → 规则值班表主班 → 业务字典值班表主班 → 设置里的兜底值班表主班 → 空；
//   处理人：规则值班表备班、值班分组管理员、受影响资源负责人、指定用户。
// 升级链：指挥所在值班表的备班 → 值班分组管理员（规则配置）→ 指挥的部门负责人上 1 级。

// responseSpec 解析输入（触发规则的响应配置或手动开故障时的显式选择，以及业务字典 / 兜底）。
type responseSpec struct {
	commanderUserID    string
	commanderSchedule  string
	responderBackup    bool
	responderGroupID   string
	responderCIOwner   bool
	responderUserIDs   []string
	businessSchedule   string
	fallbackSchedule   string
	affectedCIOwnerIDs []string
}

// resolved 解析结果。
type resolved struct {
	commanderID       string
	commanderSchedule string
	responders        []string
	notes             []string
}

// resolveRoles 解析指挥与处理人为可用用户。
func (uc *IncidentUsecase) resolveRoles(ctx context.Context, spec responseSpec, now time.Time) (*resolved, error) {
	out := &resolved{}
	pick := func(scheduleID string) (string, error) {
		if scheduleID == "" {
			return "", nil
		}
		users, err := uc.d.Duty.OnDuty(ctx, scheduleID, now)
		if err != nil {
			return "", fmt.Errorf("resolve on-duty of schedule %s: %w", scheduleID, err)
		}
		if len(users) == 0 {
			out.notes = append(out.notes, "值班表 "+scheduleID+" 此刻无人值班")
			return "", nil
		}
		out.commanderSchedule = scheduleID
		return users[0], nil
	}
	var err error
	if spec.commanderUserID != "" {
		active, aerr := uc.d.Dir.ActiveUserIDs(ctx, []string{spec.commanderUserID})
		if aerr != nil {
			return nil, aerr
		}
		if len(active) == 1 {
			out.commanderID = active[0]
		} else {
			out.notes = append(out.notes, "指定的指挥已停用")
		}
	}
	for _, sched := range []string{spec.commanderSchedule, spec.businessSchedule, spec.fallbackSchedule} {
		if out.commanderID != "" {
			break
		}
		if out.commanderID, err = pick(sched); err != nil {
			return nil, err
		}
	}
	if out.commanderID == "" {
		out.notes = append(out.notes, "未解析到指挥")
	}
	var responders []string
	if spec.responderBackup && out.commanderSchedule != "" {
		_, backup, err := uc.d.Duty.OnDutyByRole(ctx, out.commanderSchedule)
		if err != nil {
			return nil, err
		}
		responders = append(responders, backup...)
	}
	if spec.responderGroupID != "" {
		admins, err := uc.d.Groups.AdminIDs(ctx, spec.responderGroupID)
		if err != nil {
			return nil, err
		}
		responders = append(responders, admins...)
	}
	if spec.responderCIOwner {
		responders = append(responders, spec.affectedCIOwnerIDs...)
	}
	responders = append(responders, spec.responderUserIDs...)
	responders = slices.DeleteFunc(uniq(responders), func(id string) bool { return id == out.commanderID })
	if len(responders) > 0 {
		if responders, err = uc.d.Dir.ActiveUserIDs(ctx, responders); err != nil {
			return nil, err
		}
	}
	out.responders = responders
	return out, nil
}

// escalationTargets 未响应升级第 level 级的通知对象。
func (uc *IncidentUsecase) escalationTargets(ctx context.Context, inc *Incident, level int32, scheduleID, groupID string) ([]string, error) {
	var users []string
	switch level {
	case 1:
		if scheduleID != "" {
			_, backup, err := uc.d.Duty.OnDutyByRole(ctx, scheduleID)
			if err != nil {
				return nil, err
			}
			users = backup
		}
	case 2:
		if groupID != "" {
			admins, err := uc.d.Groups.AdminIDs(ctx, groupID)
			if err != nil {
				return nil, err
			}
			users = admins
		}
	default:
		if inc.CommanderID != "" {
			dept, err := uc.d.Dir.PrimaryDept(ctx, inc.CommanderID)
			if err != nil {
				return nil, err
			}
			if dept != "" {
				leaders, err := uc.d.Leaders.DeptLeaderIDs(ctx, dept, 2, inc.CommanderID)
				if err != nil {
					return nil, err
				}
				users = leaders
			}
		}
	}
	users = slices.DeleteFunc(uniq(users), func(id string) bool { return id == inc.CommanderID })
	if len(users) == 0 {
		return nil, nil
	}
	return uc.d.Dir.ActiveUserIDs(ctx, users)
}
