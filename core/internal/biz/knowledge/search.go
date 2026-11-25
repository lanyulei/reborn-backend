package biz

import (
	"context"
	"html"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/zhtext"
	"github.com/lanyulei/reborn/backend/kit/idgen"
)

// 检索参数。
const (
	// rrfK 倒数排名融合常数。
	rrfK = 60.0
	// titleBoostWeight 标题召回对该文档最佳分块的加成权重。
	titleBoostWeight = 0.5
	// queryEmbedTimeout 查询向量化超时（检索是在线路径，不能等太久）。
	queryEmbedTimeout = 5 * time.Second
	// overduePenalty 超期未复审文档的得分折扣。
	overduePenalty = 0.8
)

// 熔断参数：连续失败达 breakerThreshold 次打开 breakerOpenFor。
const (
	breakerThreshold = 5
	breakerOpenFor   = time.Minute
)

// EmbedGuard embedding 服务的熔断器：索引与检索共用，打开期间不再发起向量化请求。
type EmbedGuard struct {
	mu        sync.Mutex
	failures  int
	openUntil time.Time
	now       func() time.Time
}

// NewEmbedGuard new an EmbedGuard.
func NewEmbedGuard() *EmbedGuard { return &EmbedGuard{now: time.Now} }

// Allow 当前是否允许调用。
func (g *EmbedGuard) Allow() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.now().After(g.openUntil)
}

// Open 熔断是否打开。
func (g *EmbedGuard) Open() bool { return !g.Allow() }

// Success 记一次成功：清零失败计数。
func (g *EmbedGuard) Success() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures = 0
}

// Failure 记一次失败：达到阈值即打开。
func (g *EmbedGuard) Failure() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.failures++
	if g.failures >= breakerThreshold {
		g.openUntil = g.now().Add(breakerOpenFor)
		g.failures = 0
	}
}

// SearchInput 检索入参。
type SearchInput struct {
	Query    string
	SpaceIDs []string
	Tags     []string
	TopK     int
}

// SearchHit 一条检索结果。
type SearchHit struct {
	ChunkID       string
	DocumentID    string
	Title         string
	SpaceID       string
	SpaceName     string
	Seq           int32
	HeadingPath   string
	Snippet       string
	Section       string
	Score         float64
	ReviewOverdue bool
}

// Citation 引用标记：[kb:<document_id>#<chunk_seq>]。
func (h *SearchHit) Citation() string { return "[kb:" + h.DocumentID + "#" + itoa(h.Seq) + "]" }

// SearchResult 检索结果。
type SearchResult struct {
	Hits     []*SearchHit
	Degraded bool
	LogID    string
	Duration time.Duration
}

// Retriever 进程内检索接口：同进程的其他域（故障处置、未来的 Agent 运行时）直接注入使用，
// 与 HTTP 接口同一实现与同一权限过滤。
type Retriever interface {
	Retrieve(ctx context.Context, userID string, in SearchInput) (*SearchResult, error)
}

// SearchUsecase 混合检索用例：全文（bigram tsvector）+ 向量（pgvector）+ 标题（trgm）→ RRF 融合 → 整节返回。
type SearchUsecase struct {
	spaces   *SpaceUsecase
	chunks   ChunkRepo
	logs     SearchLogRepo
	embedder Embedder
	guard    *EmbedGuard
	logger   *slog.Logger
	now      func() time.Time
}

// NewSearchUsecase new a SearchUsecase.
func NewSearchUsecase(spaces *SpaceUsecase, chunks ChunkRepo, logs SearchLogRepo, embedder Embedder, guard *EmbedGuard, logger *slog.Logger) *SearchUsecase {
	return &SearchUsecase{spaces: spaces, chunks: chunks, logs: logs, embedder: embedder, guard: guard, logger: logger, now: time.Now}
}

// 编译期断言：SearchUsecase 即 Retriever。
var _ Retriever = (*SearchUsecase)(nil)

// Retrieve implements Retriever（来源记为 related）。
func (uc *SearchUsecase) Retrieve(ctx context.Context, userID string, in SearchInput) (*SearchResult, error) {
	return uc.Search(ctx, userID, SearchSourceRelated, in)
}

// VectorEnabled 向量检索当前是否可用（embedding 已配置、向量列存在、熔断未打开）。
func (uc *SearchUsecase) VectorEnabled() bool {
	return uc.embedder.Enabled() && uc.chunks.VectorAvailable() && uc.guard.Allow()
}

// Search 检索：只在用户可见空间内召回，结果按文档去重，附命中所在整节与片段，并记一条检索日志。
func (uc *SearchUsecase) Search(ctx context.Context, userID, source string, in SearchInput) (*SearchResult, error) {
	started := uc.now()
	if strings.TrimSpace(in.Query) == "" {
		return nil, ErrInvalidArgument
	}
	acc, err := uc.spaces.Resolve(ctx, userID)
	if err != nil {
		return nil, err
	}
	return uc.searchAs(ctx, acc, userID, source, in, started)
}

// searchAs 以已解析好的权限视图 acc（须是 userID 的）检索：调用方在同一请求里已经解析过权限时
// 直接传入，不必再解析一遍（每次解析是两条查询，其中一条对全部空间做相关子查询）。
func (uc *SearchUsecase) searchAs(ctx context.Context, acc *Access, userID, source string, in SearchInput, started time.Time) (*SearchResult, error) {
	query := strings.TrimSpace(in.Query)
	if query == "" {
		return nil, ErrInvalidArgument
	}
	topK := in.TopK
	if topK <= 0 {
		topK = DefaultTopK
	}
	if topK > MaxTopK {
		topK = MaxTopK
	}
	unrestricted, spaceIDs, ok := scopeSpaces(acc, in.SpaceIDs)
	res := &SearchResult{Hits: []*SearchHit{}, Degraded: !uc.VectorEnabled()}
	if ok {
		if err := uc.retrieve(ctx, query, unrestricted, spaceIDs, in.Tags, topK, res); err != nil {
			return nil, err
		}
	}
	res.Duration = uc.now().Sub(started)
	res.LogID = uc.log(ctx, userID, source, query, topK, res)
	return res, nil
}

// scopeSpaces 把请求限定的空间与可见空间求交；没有任何可检索空间时 ok 为 false。
func scopeSpaces(acc *Access, requested []string) (unrestricted bool, ids []string, ok bool) {
	if acc.Unrestricted() {
		if len(requested) == 0 {
			return true, nil, true
		}
		return false, requested, true
	}
	visible := acc.IDs(LevelRead)
	if len(requested) == 0 {
		return false, visible, len(visible) > 0
	}
	set := make(map[string]struct{}, len(visible))
	for _, id := range visible {
		set[id] = struct{}{}
	}
	for _, id := range requested {
		if _, ok := set[id]; ok {
			ids = append(ids, id)
		}
	}
	return false, ids, len(ids) > 0
}

// retrieve 三路召回 + 融合 + 整节 + 片段。
func (uc *SearchUsecase) retrieve(ctx context.Context, query string, unrestricted bool, spaceIDs, tags []string, topK int, res *SearchResult) error {
	type candidate struct {
		hit   *ChunkHit
		score float64
	}
	cands := map[string]*candidate{}
	addRanked := func(hits []*ChunkHit, weight float64) {
		for i, h := range hits {
			c, ok := cands[h.ChunkID]
			if !ok {
				c = &candidate{hit: h}
				cands[h.ChunkID] = c
			}
			c.score += weight / (rrfK + float64(i+1))
		}
	}
	if tsq := zhtext.Query(query); tsq != "" {
		hits, err := uc.chunks.FullText(ctx, tsq, unrestricted, spaceIDs, tags, candidateLimit)
		if err != nil {
			return err
		}
		addRanked(hits, 1)
	}
	if !res.Degraded {
		vec, err := uc.embedQuery(ctx, query)
		if err != nil {
			uc.logger.WarnContext(ctx, "knowledge: embed query failed, degrading to full-text", "error", err)
			res.Degraded = true
		} else {
			hits, err := uc.chunks.Vector(ctx, vec, unrestricted, spaceIDs, tags, candidateLimit)
			if err != nil {
				return err
			}
			addRanked(hits, 1)
		}
	}
	if len(cands) == 0 {
		return nil
	}
	// 标题召回：给该文档得分最高的分块加成。
	titles, err := uc.chunks.Titles(ctx, query, unrestricted, spaceIDs, tags, candidateLimit)
	if err != nil {
		return err
	}
	bestByDoc := map[string]*candidate{}
	for _, c := range cands {
		if b, ok := bestByDoc[c.hit.DocumentID]; !ok || c.score > b.score {
			bestByDoc[c.hit.DocumentID] = c
		}
	}
	for i, t := range titles {
		if b, ok := bestByDoc[t.DocumentID]; ok {
			b.score += titleBoostWeight / (rrfK + float64(i+1))
		}
	}
	now := uc.now()
	ranked := make([]*candidate, 0, len(bestByDoc))
	for _, c := range bestByDoc {
		if c.hit.ReviewAt != nil && c.hit.ReviewAt.Before(now) {
			c.score *= overduePenalty
		}
		ranked = append(ranked, c)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].hit.ChunkID < ranked[j].hit.ChunkID
	})
	if len(ranked) > topK {
		ranked = ranked[:topK]
	}
	keys := make([]SectionKey, 0, len(ranked))
	for _, c := range ranked {
		keys = append(keys, SectionKey{DocumentID: c.hit.DocumentID, HeadingPath: c.hit.HeadingPath})
	}
	sections, err := uc.chunks.Sections(ctx, keys, maxSectionChars)
	if err != nil {
		return err
	}
	terms := zhtext.Tokenize(query)
	for _, c := range ranked {
		h := c.hit
		section := sections[SectionKey{DocumentID: h.DocumentID, HeadingPath: h.HeadingPath}]
		if section == "" {
			section = truncateRunes(h.Content, maxSectionChars)
		}
		res.Hits = append(res.Hits, &SearchHit{
			ChunkID: h.ChunkID, DocumentID: h.DocumentID, Title: h.Title, SpaceID: h.SpaceID, SpaceName: h.SpaceName,
			Seq: h.Seq, HeadingPath: h.HeadingPath, Snippet: makeSnippet(h.Content, terms), Section: section,
			Score: c.score, ReviewOverdue: h.ReviewAt != nil && h.ReviewAt.Before(now),
		})
	}
	return nil
}

// embedQuery 带超时与熔断记账的查询向量化。
func (uc *SearchUsecase) embedQuery(ctx context.Context, query string) ([]float32, error) {
	ectx, cancel := context.WithTimeout(ctx, queryEmbedTimeout)
	defer cancel()
	vecs, err := uc.embedder.Embed(ectx, []string{query})
	if err != nil {
		// 调用方取消（请求断开）不是 embedding 服务的故障，不计入熔断；只有服务自身出错或超时才计。
		if ctx.Err() == nil {
			uc.guard.Failure()
		}
		return nil, err
	}
	if len(vecs) != 1 || len(vecs[0]) != EmbeddingDimensions {
		uc.guard.Failure()
		return nil, ErrEmbeddingDimension
	}
	uc.guard.Success()
	return vecs[0], nil
}

// log 记检索日志；失败只记日志（不影响检索结果）。
func (uc *SearchUsecase) log(ctx context.Context, userID, source, query string, topK int, res *SearchResult) string {
	id, err := idgen.NewID()
	if err != nil {
		return ""
	}
	ids := make([]string, 0, len(res.Hits))
	for _, h := range res.Hits {
		ids = append(ids, h.ChunkID)
	}
	l := &SearchLog{
		ID: id, UserID: userID, Source: source, Query: truncateRunes(query, 500), TopK: clampInt32(topK), HitCount: clampInt32(len(res.Hits)),
		Degraded: res.Degraded, DurationMS: clampInt32(res.Duration.Milliseconds()), ChunkIDs: ids, CreateTime: uc.now(),
	}
	if err := uc.logs.Create(context.WithoutCancel(ctx), l); err != nil {
		uc.logger.WarnContext(ctx, "knowledge: write search log", "error", err)
		return ""
	}
	return id
}

// Feedback 对本人的检索记录反馈。
func (uc *SearchUsecase) Feedback(ctx context.Context, userID, logID string, helpful bool) error {
	fb := FeedbackUnhelpful
	if helpful {
		fb = FeedbackHelpful
	}
	return uc.logs.SetFeedback(ctx, logID, userID, fb)
}

// makeSnippet 在正文里找第一个命中词元，取其前后窗口作为片段；命中词元以 <mark> 标注，其余 HTML 转义。
func makeSnippet(content string, terms []string) string {
	text := strings.Join(strings.Fields(content), " ")
	runes := []rune(text)
	lower := strings.ToLower(text)
	first := -1
	for _, t := range terms {
		if i := strings.Index(lower, t); i >= 0 && (first < 0 || i < first) {
			first = i
		}
	}
	start, end := 0, len(runes)
	if first >= 0 {
		pos := len([]rune(lower[:first]))
		start = max(0, pos-maxSnippetChars/3)
		end = min(len(runes), start+maxSnippetChars)
	} else if end > maxSnippetChars {
		end = maxSnippetChars
	}
	window := string(runes[start:end])
	marked := markTerms(window, terms)
	if start > 0 {
		marked = "…" + marked
	}
	if end < len(runes) {
		marked += "…"
	}
	return marked
}

// markTerms 对窗口文本做 HTML 转义并把命中词元包进 <mark>（按出现位置，不重叠）。
func markTerms(window string, terms []string) string {
	lower := strings.ToLower(window)
	type span struct{ s, e int }
	var spans []span
	for _, t := range terms {
		if t == "" {
			continue
		}
		from := 0
		for {
			i := strings.Index(lower[from:], t)
			if i < 0 {
				break
			}
			spans = append(spans, span{from + i, from + i + len(t)})
			from += i + len(t)
		}
	}
	if len(spans) == 0 {
		return html.EscapeString(window)
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].s < spans[j].s })
	// 合并重叠 / 相邻的命中（二元组相邻命中拼成完整词）。
	merged := spans[:1]
	for _, sp := range spans[1:] {
		last := &merged[len(merged)-1]
		if sp.s <= last.e {
			if sp.e > last.e {
				last.e = sp.e
			}
			continue
		}
		merged = append(merged, sp)
	}
	var b strings.Builder
	pos := 0
	for _, sp := range merged {
		b.WriteString(html.EscapeString(window[pos:sp.s]))
		b.WriteString("<mark>" + html.EscapeString(window[sp.s:sp.e]) + "</mark>")
		pos = sp.e
	}
	b.WriteString(html.EscapeString(window[pos:]))
	return b.String()
}

// clampInt32 把计数 / 毫秒数收敛到 int32 范围（超出按上限）。
func clampInt32[T ~int | ~int64](n T) int32 {
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	if n < math.MinInt32 {
		return math.MinInt32
	}
	return int32(n)
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func itoa(n int32) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
