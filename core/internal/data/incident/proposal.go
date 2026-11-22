package data

import (
	"context"
	"errors"
	"fmt"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/incident"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/txrun"

	"github.com/jackc/pgx/v5"
)

var _ biz.ProposalRepo = (*proposalRepo)(nil)

type proposalRepo struct {
	data *basedata.Data
}

// NewProposalRepo new a proposalRepo.
func NewProposalRepo(data *basedata.Data) *proposalRepo { return &proposalRepo{data: data} }

const proposalColumns = `p.id::text, p.incident_id::text, p.title, p.reason, p.evidence, p.action, COALESCE(p.playbook_id::text, ''), p.step_index, p.risk, p.idem_key,
	p.status, p.auto, p.proposer_type, COALESCE(p.proposer_id::text, ''), COALESCE(p.decided_by::text, ''), p.decided_time, p.decision_note, p.expire_time,
	p.exec_kind, p.exec_ref, COALESCE(p.exec_user::text, ''), p.result, p.verify_until, p.verify_result, p.create_time, p.update_time, p.auto_eligible`

func scanProposal(scan func(dest ...any) error) (*biz.Proposal, error) {
	var p biz.Proposal
	var evidence, action, verify []byte
	if err := scan(&p.ID, &p.IncidentID, &p.Title, &p.Reason, &evidence, &action, &p.PlaybookID, &p.StepIndex, &p.Risk, &p.IdemKey,
		&p.Status, &p.Auto, &p.ProposerType, &p.ProposerID, &p.DecidedBy, &p.DecidedTime, &p.DecisionNote, &p.ExpireTime,
		&p.ExecKind, &p.ExecRef, &p.ExecUser, &p.Result, &p.VerifyUntil, &verify, &p.CreateTime, &p.UpdateTime, &p.AutoEligible); err != nil {
		return nil, err
	}
	if err := decodeJSON(evidence, &p.Evidence, "proposal evidence"); err != nil {
		return nil, err
	}
	if err := decodeJSON(action, &p.Action, "proposal action"); err != nil {
		return nil, err
	}
	if len(verify) > 0 {
		p.VerifyResult = &biz.VerifyResult{}
		if err := decodeJSON(verify, p.VerifyResult, "proposal verify result"); err != nil {
			return nil, err
		}
	}
	return &p, nil
}

func (r *proposalRepo) query(ctx context.Context, what, tail string, args ...any) ([]*biz.Proposal, error) {
	rows, err := r.data.Pool.Query(ctx, "SELECT "+proposalColumns+" FROM incident_proposal p WHERE "+tail, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer rows.Close()
	out := make([]*biz.Proposal, 0, 16)
	for rows.Next() {
		p, err := scanProposal(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("%s: scan: %w", what, err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func proposalArgs(p *biz.Proposal) ([]any, error) {
	evidence, err := encodeJSON(nonNilStrings(p.Evidence), "proposal evidence")
	if err != nil {
		return nil, err
	}
	action, err := encodeJSON(p.Action, "proposal action")
	if err != nil {
		return nil, err
	}
	var verify any
	if p.VerifyResult != nil {
		raw, err := encodeJSON(p.VerifyResult, "proposal verify result")
		if err != nil {
			return nil, err
		}
		verify = raw
	}
	return []any{
		p.Title, p.Reason, evidence, action, nilIfEmpty(p.PlaybookID), p.StepIndex, p.Risk, p.IdemKey, p.Status, p.Auto, p.ProposerType, nilIfEmpty(p.ProposerID),
		nilIfEmpty(p.DecidedBy), nilTime(p.DecidedTime), p.DecisionNote, p.ExpireTime, p.ExecKind, p.ExecRef, nilIfEmpty(p.ExecUser), p.Result, nilTime(p.VerifyUntil), verify,
		p.AutoEligible,
	}, nil
}

// autoExecLockKey Agent 自动执行配额的咨询锁键（本库内唯一即可）。
const autoExecLockKey int64 = 0x494E4341_50524F50 // "INCAPROP"

// LockAutoExecTx implements biz.ProposalRepo。
func (r *proposalRepo) LockAutoExecTx(ctx context.Context, tx txrun.Tx) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", autoExecLockKey); err != nil {
		return fmt.Errorf("lock auto exec quota: %w", err)
	}
	return nil
}

// CountAutoRunning implements biz.ProposalRepo。
func (r *proposalRepo) CountAutoRunning(ctx context.Context, businessID string) (int, int, error) {
	var global, business int
	err := r.data.Pool.QueryRow(ctx,
		`SELECT count(*)::int, count(*) FILTER (WHERE $1 <> '' AND i.business_id::text = $1)::int
		 FROM incident_proposal p JOIN incident_record i ON i.id = p.incident_id
		 WHERE p.auto AND p.proposer_type = 3 AND p.status IN (2, 4)`, businessID).Scan(&global, &business)
	if err != nil {
		return 0, 0, fmt.Errorf("count auto running proposals: %w", err)
	}
	return global, business, nil
}

// LastAutoExec implements biz.ProposalRepo。靶标交集经 jsonb 数组的 ?| 判定。
func (r *proposalRepo) LastAutoExec(ctx context.Context, ciIDs []string, playbookID string, stepIndex int32) (*time.Time, error) {
	var last *time.Time
	err := r.data.Pool.QueryRow(ctx,
		`SELECT max(decided_time) FROM incident_proposal
		 WHERE auto AND playbook_id = $1 AND step_index = $2 AND status IN (2, 4, 5, 6)
		   AND (cardinality($3::text[]) = 0 OR (action -> 'ci_ids') ?| $3::text[])`, playbookID, stepIndex, textArray(ciIDs)).Scan(&last)
	if err != nil {
		return nil, fmt.Errorf("last auto exec: %w", err)
	}
	return last, nil
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// CreateTx implements biz.ProposalRepo.
func (r *proposalRepo) CreateTx(ctx context.Context, tx txrun.Tx, p *biz.Proposal) error {
	args, err := proposalArgs(p)
	if err != nil {
		return err
	}
	if err := tx.QueryRow(ctx,
		`INSERT INTO incident_proposal (id, incident_id, title, reason, evidence, action, playbook_id, step_index, risk, idem_key, status, auto, proposer_type, proposer_id,
		   decided_by, decided_time, decision_note, expire_time, exec_kind, exec_ref, exec_user, result, verify_until, verify_result, auto_eligible)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25) RETURNING create_time, update_time`,
		append([]any{p.ID, p.IncidentID}, args...)...).Scan(&p.CreateTime, &p.UpdateTime); err != nil {
		if isUniqueViolation(err, "uk_incident_proposal_idem") {
			return biz.ErrProposalDuplicate
		}
		return fmt.Errorf("insert incident proposal: %w", err)
	}
	return nil
}

// Get implements biz.ProposalRepo.
func (r *proposalRepo) Get(ctx context.Context, id string) (*biz.Proposal, error) {
	p, err := scanProposal(r.data.Pool.QueryRow(ctx, "SELECT "+proposalColumns+" FROM incident_proposal p WHERE p.id = $1", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrProposalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get incident proposal: %w", err)
	}
	return p, nil
}

// LockTx implements biz.ProposalRepo.
func (r *proposalRepo) LockTx(ctx context.Context, tx txrun.Tx, id string) (*biz.Proposal, error) {
	p, err := scanProposal(tx.QueryRow(ctx, "SELECT "+proposalColumns+" FROM incident_proposal p WHERE p.id = $1 FOR UPDATE", id).Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, biz.ErrProposalNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock incident proposal: %w", err)
	}
	return p, nil
}

// SaveTx implements biz.ProposalRepo.
func (r *proposalRepo) SaveTx(ctx context.Context, tx txrun.Tx, p *biz.Proposal, fromStatus int32) (bool, error) {
	args, err := proposalArgs(p)
	if err != nil {
		return false, err
	}
	n, err := tx.Exec(ctx,
		`UPDATE incident_proposal SET title = $3, reason = $4, evidence = $5, action = $6, playbook_id = $7, step_index = $8, risk = $9, idem_key = $10, status = $11, auto = $12,
		   proposer_type = $13, proposer_id = $14, decided_by = $15, decided_time = $16, decision_note = $17, expire_time = $18, exec_kind = $19, exec_ref = $20, exec_user = $21,
		   result = $22, verify_until = $23, verify_result = $24, auto_eligible = $25, update_time = now()
		 WHERE id = $1 AND ($2 = 0 OR status = $2)`, append([]any{p.ID, fromStatus}, args...)...)
	if err != nil {
		if isUniqueViolation(err, "uk_incident_proposal_idem") {
			return false, biz.ErrProposalDuplicate
		}
		return false, fmt.Errorf("save incident proposal: %w", err)
	}
	return n == 1, nil
}

// SaveVerifiedTx implements biz.ProposalRepo.
func (r *proposalRepo) SaveVerifiedTx(ctx context.Context, tx txrun.Tx, p *biz.Proposal) (bool, error) {
	var verify any
	if p.VerifyResult != nil {
		raw, err := encodeJSON(p.VerifyResult, "proposal verify result")
		if err != nil {
			return false, err
		}
		verify = raw
	}
	n, err := tx.Exec(ctx, "UPDATE incident_proposal SET verify_result = $2, update_time = now() WHERE id = $1 AND status = 5 AND verify_result IS NULL", p.ID, verify)
	if err != nil {
		return false, fmt.Errorf("save incident proposal verify result: %w", err)
	}
	return n == 1, nil
}

// ListByStatusTx implements biz.ProposalRepo.
func (r *proposalRepo) ListByStatusTx(ctx context.Context, tx txrun.Tx, incidentID string, statuses []int32) ([]*biz.Proposal, error) {
	pgtx, ok := basedata.PgxTx(tx)
	if !ok {
		return nil, errors.New("list incident proposals in tx: not a pgx transaction")
	}
	rows, err := pgtx.Query(ctx, "SELECT "+proposalColumns+" FROM incident_proposal p WHERE p.incident_id = $1 AND p.status = ANY($2::smallint[]) ORDER BY p.create_time FOR UPDATE", incidentID, int32Array(statuses))
	if err != nil {
		return nil, fmt.Errorf("list incident proposals in tx: %w", err)
	}
	defer rows.Close()
	out := make([]*biz.Proposal, 0, 16)
	for rows.Next() {
		p, err := scanProposal(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("list incident proposals in tx: scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// List implements biz.ProposalRepo.
func (r *proposalRepo) List(ctx context.Context, incidentID string, status int32) ([]*biz.Proposal, error) {
	if status > 0 {
		return r.query(ctx, "list incident proposals", "p.incident_id = $1 AND p.status = $2 ORDER BY p.create_time DESC LIMIT 200", incidentID, status)
	}
	return r.query(ctx, "list incident proposals", "p.incident_id = $1 ORDER BY p.create_time DESC LIMIT 200", incidentID)
}

// ListExpired implements biz.ProposalRepo.
func (r *proposalRepo) ListExpired(ctx context.Context, now time.Time, limit int) ([]*biz.Proposal, error) {
	return r.query(ctx, "list expired incident proposals", "p.status = 1 AND p.expire_time < $1 ORDER BY p.expire_time LIMIT $2", now, limit)
}

// ListRunning implements biz.ProposalRepo.
func (r *proposalRepo) ListRunning(ctx context.Context, limit int) ([]*biz.Proposal, error) {
	return r.query(ctx, "list running incident proposals", "p.status = 4 ORDER BY p.update_time LIMIT $1", limit)
}

// ListVerifyDue implements biz.ProposalRepo.
func (r *proposalRepo) ListVerifyDue(ctx context.Context, now time.Time, limit int) ([]*biz.Proposal, error) {
	return r.query(ctx, "list verify-due incident proposals", "p.status = 5 AND p.verify_result IS NULL AND p.verify_until IS NOT NULL AND p.verify_until <= $1 ORDER BY p.verify_until LIMIT $2", now, limit)
}

// CountPlaybookExecs implements biz.ProposalRepo.
func (r *proposalRepo) CountPlaybookExecs(ctx context.Context, playbookID string, since time.Time) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_proposal WHERE playbook_id = $1 AND status IN (4, 5, 6) AND create_time >= $2", playbookID, since).Scan(&n); err != nil {
		return 0, fmt.Errorf("count playbook execs: %w", err)
	}
	return n, nil
}

// CountPending implements biz.ProposalRepo.
func (r *proposalRepo) CountPending(ctx context.Context) (int, error) {
	var n int
	if err := r.data.Pool.QueryRow(ctx, "SELECT count(*) FROM incident_proposal WHERE status = 1").Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending proposals: %w", err)
	}
	return n, nil
}
