package biz

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/lanyulei/reborn/backend/job/internal/pkg/textx"
	"github.com/lanyulei/reborn/backend/kit/idgen"
	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// commitIDPattern Git 提交号（SHA-1 / SHA-256 的十六进制）。不合规的提交号被忽略而不是拒绝整个请求：
// 它只是展示信息，不值得为它让节点续不上租约。
var commitIDPattern = regexp.MustCompile(`^[0-9a-f]{7,64}$`)

// Report 处理执行中的周期上报：围栏校验 → 追加日志 → 续租 → 记提交号，应答是否已请求取消与已确认的日志序号。
//
// 日志块按序号接收：不大于已确认序号的块忽略（重传），恰为已确认序号 + 1 的块接收，遇到第一个
// 空洞即停（其后的块等节点重传）。日志总量超过 MaxLogBytes 后内容丢弃并标记截断，序号照常确认。
// 执行此前因租约到期被判为结果未知的，节点带着同一领取令牌回来即恢复为执行中。
func (uc *RunnerUsecase) Report(ctx context.Context, r *Runner, req *jobcontract.ReportRequest) (resp *jobcontract.ReportResponse, err error) {
	defer func() { uc.obs.ObserveCall(ctx, OpReport, resultOf(err)) }()
	if err := validateLease(req.ExecutionID, req.LeaseToken); err != nil {
		return nil, err
	}
	if len(req.Chunks) > jobcontract.MaxLogChunks {
		return nil, fmt.Errorf("%w: at most %d log chunks per report", ErrInvalidRequest, jobcontract.MaxLogChunks)
	}
	for _, c := range req.Chunks {
		if len(c.Content) > jobcontract.MaxLogChunkBytes {
			return nil, fmt.Errorf("%w: log chunk exceeds %d bytes", ErrInvalidRequest, jobcontract.MaxLogChunkBytes)
		}
	}
	var (
		out       jobcontract.ReportResponse
		truncated bool
	)
	revived, err := uc.progress.Report(ctx, req.ExecutionID, func(cur ExecState) (*ReportMutation, error) {
		if !holdsLease(cur, r.ID, req.LeaseToken) || (cur.Status != jobcontract.StatusRunning && cur.Status != jobcontract.StatusUnknown) {
			return nil, ErrLeaseLost
		}
		m := &ReportMutation{
			Revive:       cur.Status == jobcontract.StatusUnknown,
			LogSeq:       cur.LogSeq,
			LogBytes:     cur.LogBytes,
			LogTruncated: cur.LogTruncated,
			CommitID:     cur.CommitID,
			Lease:        uc.lease,
		}
		for _, c := range req.Chunks {
			if c.Seq <= m.LogSeq {
				continue // 已确认过的块（重传）
			}
			if c.Seq != m.LogSeq+1 {
				break // 空洞：其后的块等重传
			}
			m.LogSeq = c.Seq
			content := textx.Clean(c.Content)
			if m.LogTruncated || m.LogBytes+int64(len(content)) > jobcontract.MaxLogBytes {
				// 一旦截断，其后的内容全部丢弃：保留的日志始终是完整的前缀。
				m.LogTruncated = true
				continue
			}
			m.LogBytes += int64(len(content))
			if content != "" {
				m.Chunks = append(m.Chunks, jobcontract.LogChunk{Seq: c.Seq, Content: content})
			}
		}
		if m.CommitID == "" && commitIDPattern.MatchString(req.CommitID) {
			m.CommitID = req.CommitID
		}
		out = jobcontract.ReportResponse{Cancel: cur.CancelRequested, AckSeq: m.LogSeq}
		truncated = m.LogTruncated && !cur.LogTruncated
		return m, nil
	})
	if err != nil {
		return nil, progressError("report", err)
	}
	if revived {
		uc.obs.AddExecutions(ctx, EventRevived, 1)
		uc.log.WarnContext(ctx, "execution revived: runner resumed reporting after lease expiry", "execution_id", req.ExecutionID, "runner_id", r.ID)
	}
	if truncated {
		uc.obs.AddExecutions(ctx, EventLogTruncated, 1)
		uc.log.WarnContext(ctx, "execution log truncated: size limit reached", "execution_id", req.ExecutionID)
	}
	return &out, nil
}

// Finish 处理执行结束：围栏校验 → 写各主机结果 → 推出终态并落账。重复的结束请求（执行已是终态且
// 领取令牌一致）是空操作。结果未知的执行（租约到期后节点才报结束）直接落为真实终态。
func (uc *RunnerUsecase) Finish(ctx context.Context, r *Runner, req *jobcontract.FinishRequest) (err error) {
	defer func() { uc.obs.ObserveCall(ctx, OpFinish, resultOf(err)) }()
	if err := validateLease(req.ExecutionID, req.LeaseToken); err != nil {
		return err
	}
	switch req.Outcome {
	case jobcontract.OutcomeExited, jobcontract.OutcomeCanceled, jobcontract.OutcomeTimeout, jobcontract.OutcomeError:
	default:
		return fmt.Errorf("%w: unknown outcome", ErrInvalidRequest)
	}
	// 只认本次执行的主机：终态与主机统计都按主机快照里的地址计算，节点报来的其它地址一律忽略。
	// 主机快照在建单后不再变化，可以在事务之外读取。
	known, err := uc.materials.Hosts(ctx, req.ExecutionID)
	if err != nil {
		return fmt.Errorf("load execution hosts: %w", err)
	}
	results := knownResults(req.Hosts, known)
	status := jobcontract.FinalStatus(req.Outcome, req.ExitCode, results)
	applied := false
	err = uc.progress.Finish(ctx, req.ExecutionID, func(cur ExecState) (*FinishMutation, error) {
		if !holdsLease(cur, r.ID, req.LeaseToken) {
			return nil, ErrLeaseLost
		}
		switch cur.Status {
		case jobcontract.StatusRunning, jobcontract.StatusUnknown:
		case jobcontract.StatusSuccess, jobcontract.StatusFailed, jobcontract.StatusPartial, jobcontract.StatusCanceled, jobcontract.StatusTimeout:
			return nil, nil // 重复的结束请求
		default:
			return nil, ErrLeaseLost
		}
		m := &FinishMutation{
			Status: status,
			Error:  textx.TruncateRunes(textx.Clean(req.Error), jobcontract.MaxErrorLen),
			Hosts:  hostUpdates(results),
		}
		if commitIDPattern.MatchString(req.CommitID) {
			m.CommitID = req.CommitID
		}
		applied = true
		return m, nil
	})
	if err != nil {
		return progressError("finish", err)
	}
	if applied {
		uc.obs.AddExecutions(ctx, finishEvents[status], 1)
		uc.log.InfoContext(ctx, "execution finished", "execution_id", req.ExecutionID, "runner_id", r.ID, "status", status, "outcome", req.Outcome)
	}
	return nil
}

// validateLease 校验上报 / 结束请求里的执行 ID 与领取令牌的形态。
func validateLease(executionID, leaseToken string) error {
	if !idgen.IsUUID(executionID) {
		return fmt.Errorf("%w: execution_id", ErrInvalidRequest)
	}
	if leaseToken == "" || len(leaseToken) > 64 {
		return fmt.Errorf("%w: lease_token", ErrInvalidRequest)
	}
	return nil
}

// holdsLease 报告请求方是否持有该执行的租约：领取令牌一致且执行确由该节点领取。
func holdsLease(cur ExecState, runnerID, leaseToken string) bool {
	return cur.LeaseToken != "" && cur.LeaseToken == leaseToken && cur.RunnerID == runnerID
}

// progressError 归一化仓储错误：执行不存在按租约失效处理（节点须丢弃该作业）。
func progressError(op string, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return ErrLeaseLost
	case errors.Is(err, ErrLeaseLost), errors.Is(err, ErrInvalidRequest):
		return err
	}
	return fmt.Errorf("%s execution: %w", op, err)
}

// knownResults 过滤出属于本次执行的主机统计：同一地址后者覆盖前者，保持首次出现的顺序。
func knownResults(results []jobcontract.HostResult, known []Host) []jobcontract.HostResult {
	addrs := make(map[string]struct{}, len(known))
	for _, h := range known {
		addrs[h.Address] = struct{}{}
	}
	index := make(map[string]int, len(results))
	out := make([]jobcontract.HostResult, 0, min(len(results), len(known)))
	for _, r := range results {
		if _, ok := addrs[r.Address]; !ok {
			continue
		}
		if i, dup := index[r.Address]; dup {
			out[i] = r
			continue
		}
		index[r.Address] = len(out)
		out = append(out, r)
	}
	return out
}

// hostUpdates 把主机统计转成主机行更新（计数不为负）。
func hostUpdates(results []jobcontract.HostResult) []HostUpdate {
	out := make([]HostUpdate, 0, len(results))
	for _, h := range results {
		out = append(out, HostUpdate{
			Address: h.Address, Status: h.Status(),
			OK: max(h.OK, 0), Changed: max(h.Changed, 0), Failed: max(h.Failed, 0),
			Unreachable: max(h.Unreachable, 0), Skipped: max(h.Skipped, 0),
		})
	}
	return out
}
