package biz

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
)

// producerRepoStub 生产者仓储内存桩：承载用例断言所需的最小语义（同 biz
// 未删除行重名冲突、Spec 整组替换、令牌轮换、软删不可见、NotFound）。
type producerRepoStub struct {
	producers map[string]*Producer
}

func newProducerRepoStub() *producerRepoStub {
	return &producerRepoStub{producers: make(map[string]*Producer)}
}

func (s *producerRepoStub) nameTaken(p *Producer) bool {
	for _, other := range s.producers {
		if other == p || other.BizID != p.BizID || other.Name != p.Name {
			continue
		}
		return true
	}
	return false
}

func (s *producerRepoStub) CreateProducer(_ context.Context, producer *Producer) error {
	if s.nameTaken(producer) {
		return ErrProducerNameExists
	}
	s.producers[producer.ID] = producer
	return nil
}

func (s *producerRepoStub) UpdateProducer(_ context.Context, id string, upd ProducerUpdate) error {
	producer, ok := s.producers[id]
	if !ok {
		return ErrProducerNotFound
	}
	if upd.Spec != nil {
		if newName := upd.Spec.Name; newName != producer.Name {
			probe := *producer
			probe.Name = newName
			if s.nameTaken(&probe) {
				return ErrProducerNameExists
			}
		}
		producer.Name = upd.Spec.Name
		producer.EventWhitelist = upd.Spec.EventWhitelist
		producer.ExpireTime = upd.Spec.ExpireTime
		producer.QPSLimit, producer.DailyLimit = upd.Spec.QPSLimit, upd.Spec.DailyLimit
	}
	if upd.Enabled != nil {
		producer.Enabled = *upd.Enabled
	}
	producer.UpdateTime = time.Now()
	return nil
}

func (s *producerRepoStub) UpdateProducerToken(_ context.Context, id string, tokenHash []byte) (*Producer, error) {
	producer, ok := s.producers[id]
	if !ok {
		return nil, ErrProducerNotFound
	}
	producer.TokenHash = tokenHash
	producer.UpdateTime = time.Now()
	return producer, nil
}

func (s *producerRepoStub) DeleteProducer(_ context.Context, id string) error {
	if _, ok := s.producers[id]; !ok {
		return ErrProducerNotFound
	}
	delete(s.producers, id)
	return nil
}

func (s *producerRepoStub) GetProducer(_ context.Context, id string) (*Producer, error) {
	producer, ok := s.producers[id]
	if !ok {
		return nil, ErrProducerNotFound
	}
	return producer, nil
}

func (s *producerRepoStub) ListProducers(_ context.Context, filter ProducerFilter, page, pageSize int64) ([]*Producer, int64, error) {
	var matched []*Producer
	for _, producer := range s.producers {
		if filter.Keyword != "" && !strings.Contains(producer.Name, filter.Keyword) {
			continue
		}
		if filter.Enabled != nil && producer.Enabled != *filter.Enabled {
			continue
		}
		matched = append(matched, producer)
	}
	total := int64(len(matched))
	start := (page - 1) * pageSize
	if start >= total {
		return nil, total, nil
	}
	end := min(start+pageSize, total)
	return matched[start:end], total, nil
}

// newProducerSpec 返回合法生产者配置入参（白名单两条 + 一小时后过期）。
func newProducerSpec() *ProducerSpec {
	expire := time.Now().UTC().Add(time.Hour)
	return &ProducerSpec{
		Name:           "monitor-engine",
		EventWhitelist: []string{"monitor.alert.*", "monitor.host.down"},
		ExpireTime:     &expire,
	}
}

// newProducerUC 组装生产者用例。
func newProducerUC() (*ProducerUsecase, *producerRepoStub) {
	repo := newProducerRepoStub()
	return NewProducerUsecase(repo), repo
}

func TestProducerUsecase_Create(t *testing.T) {
	t.Parallel()
	uc, _ := newProducerUC()
	ctx := context.Background()

	producer := &Producer{Enabled: true}
	producer.ProducerSpec = *newProducerSpec()
	out, token, err := uc.Create(ctx, producer)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !uuidV7Pattern.MatchString(out.ID) {
		t.Fatalf("Create() id = %q, want UUIDv7", out.ID)
	}
	if out.BizID != DefaultBizID {
		t.Fatalf("Create() bizID = %q, want default", out.BizID)
	}
	if out.CreateTime.IsZero() || out.UpdateTime.IsZero() {
		t.Fatal("Create() timestamps not set")
	}

	// token 明文：npt_ 前缀 + 32 字节 base64url（43 字符）；落库哈希 = SHA-256(明文)。
	if !strings.HasPrefix(token, producerTokenPrefix) || len(token) != len(producerTokenPrefix)+43 {
		t.Fatalf("Create() token = %q (%d chars), want %s + 43 base64url chars",
			token, len(token), producerTokenPrefix)
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, producerTokenPrefix))
	if err != nil || len(raw) != 32 {
		t.Fatalf("Create() token entropy = %v (%d bytes), want 32 random bytes", err, len(raw))
	}
	sum := sha256.Sum256([]byte(token))
	if string(out.TokenHash) != string(sum[:]) {
		t.Fatal("Create() TokenHash != SHA-256(token)")
	}

	// 两次签发互不相同。
	out2, token2, err := uc.Create(ctx, &Producer{
		BizID: "monitor", ProducerSpec: ProducerSpec{Name: "engine-2"}, Enabled: true,
	})
	if err != nil {
		t.Fatalf("Create(second) error = %v", err)
	}
	if token2 == token || string(out2.TokenHash) == string(out.TokenHash) {
		t.Fatal("Create() token repeated across issuances")
	}

	// 同 biz 重名：仓储哨兵透传。
	_, _, err = uc.Create(ctx, &Producer{
		BizID: "monitor", ProducerSpec: ProducerSpec{Name: "engine-2"}, Enabled: true,
	})
	if !errors.Is(err, ErrProducerNameExists) {
		t.Fatalf("Create(dup name) error = %v, want ErrProducerNameExists", err)
	}
}

func TestProducerUsecase_CreateInvalid(t *testing.T) {
	t.Parallel()
	uc, _ := newProducerUC()
	ctx := context.Background()

	past := time.Now().UTC().Add(-time.Minute)
	cases := []struct {
		name string
		spec ProducerSpec
	}{
		{"empty name", ProducerSpec{Name: ""}},
		{"name too long", ProducerSpec{Name: strings.Repeat("a", 65)}},
		{"bad whitelist entry", ProducerSpec{
			Name: "engine", EventWhitelist: []string{"monitor.*.firing"},
		}}, // 中间通配
		{"uppercase entry", ProducerSpec{
			Name: "engine", EventWhitelist: []string{"Monitor.Alert"},
		}},
		{"whitelist overflow", ProducerSpec{
			Name: "engine", EventWhitelist: func() []string {
				list := make([]string, 33)
				for i := range list {
					list[i] = "monitor.alert"
				}
				return list
			}(),
		}},
		{"expire in past", ProducerSpec{Name: "engine", ExpireTime: &past}},
	}
	for _, tc := range cases {
		producer := &Producer{Enabled: true}
		producer.ProducerSpec = tc.spec
		if _, _, err := uc.Create(ctx, producer); !errors.Is(err, ErrProducerInvalid) {
			t.Errorf("Create(%s) error = %v, want ErrProducerInvalid", tc.name, err)
		}
	}

	// 合法边界：空白名单（不限）、'*' 条目、远期过期。
	far := time.Now().UTC().Add(24 * time.Hour)
	for _, spec := range []ProducerSpec{
		{Name: "engine-a"},
		{Name: "engine-b", EventWhitelist: []string{"*"}},
		{Name: "engine-c", EventWhitelist: []string{"monitor.alert.firing", "monitor.alert.*"}, ExpireTime: &far},
	} {
		producer := &Producer{Enabled: true}
		producer.ProducerSpec = spec
		if _, _, err := uc.Create(ctx, producer); err != nil {
			t.Errorf("Create(%q) error = %v, want nil", spec.Name, err)
		}
	}
}

func TestProducerUsecase_Update(t *testing.T) {
	t.Parallel()
	uc, _ := newProducerUC()
	ctx := context.Background()

	producer := &Producer{Enabled: true}
	producer.ProducerSpec = *newProducerSpec()
	created, _, err := uc.Create(ctx, producer)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	oldHash := string(created.TokenHash)

	// Spec 整组替换：不传 expire_time 即清除（永不过期）。
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Spec: &ProducerSpec{
		Name:           "monitor-engine-v2",
		EventWhitelist: []string{"monitor.host.down"},
	}}); err != nil {
		t.Fatalf("Update(spec) error = %v", err)
	}
	got, err := uc.Get(ctx, created.ID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Name != "monitor-engine-v2" || len(got.EventWhitelist) != 1 ||
		got.EventWhitelist[0] != "monitor.host.down" || got.ExpireTime != nil {
		t.Fatalf("Update(spec) result = %+v", got)
	}
	if string(got.TokenHash) != oldHash {
		t.Fatal("Update(spec) must not touch token hash")
	}

	// 独立启停。
	disabled := false
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("Update(enabled) error = %v", err)
	}
	if got, _ = uc.Get(ctx, created.ID); got.Enabled {
		t.Fatal("Update(enabled) not applied")
	}

	// 非法新配置拒绝；不存在 NotFound。
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Spec: &ProducerSpec{
		Name: "bad", EventWhitelist: []string{"monitor alert"},
	}}); !errors.Is(err, ErrProducerInvalid) {
		t.Fatalf("Update(bad spec) error = %v, want ErrProducerInvalid", err)
	}
	if err := uc.Update(ctx, "missing-id", ProducerUpdate{Spec: &ProducerSpec{Name: "x"}}); !errors.Is(err, ErrProducerNotFound) {
		t.Fatalf("Update(missing) error = %v, want ErrProducerNotFound", err)
	}

	// 删除后不可见。
	if err := uc.Delete(ctx, created.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := uc.Get(ctx, created.ID); !errors.Is(err, ErrProducerNotFound) {
		t.Fatalf("Get(deleted) error = %v, want ErrProducerNotFound", err)
	}
}

func TestProducerUsecase_RotateToken(t *testing.T) {
	t.Parallel()
	uc, _ := newProducerUC()
	ctx := context.Background()

	producer := &Producer{Enabled: true}
	producer.ProducerSpec = *newProducerSpec()
	created, initToken, err := uc.Create(ctx, producer)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	rotated, newToken, err := uc.RotateToken(ctx, created.ID)
	if err != nil {
		t.Fatalf("RotateToken() error = %v", err)
	}
	if newToken == initToken || !strings.HasPrefix(newToken, producerTokenPrefix) {
		t.Fatalf("RotateToken() token = %q, want fresh %s-prefixed token", newToken, producerTokenPrefix)
	}
	sum := sha256.Sum256([]byte(newToken))
	if string(rotated.TokenHash) != string(sum[:]) {
		t.Fatal("RotateToken() TokenHash != SHA-256(new token)")
	}
	if rotated.UpdateTime.Before(created.CreateTime) {
		t.Fatalf("RotateToken() updateTime = %v, want >= %v", rotated.UpdateTime, created.CreateTime)
	}
	// 旧哈希已被替换。
	stored, err := uc.Get(ctx, created.ID)
	if err != nil || string(stored.TokenHash) != string(sum[:]) {
		t.Fatalf("Get() after rotate = (%+v, %v), hash must be the rotated one", stored, err)
	}
	// 不存在。
	if _, _, err := uc.RotateToken(ctx, "missing-id"); !errors.Is(err, ErrProducerNotFound) {
		t.Fatalf("RotateToken(missing) error = %v, want ErrProducerNotFound", err)
	}
}

func TestProducerUsecase_ListPagingNormalize(t *testing.T) {
	t.Parallel()
	uc, _ := newProducerUC()
	ctx := context.Background()

	for _, name := range []string{"monitor-engine", "permission-engine", "ci-engine"} {
		producer := &Producer{Enabled: true}
		producer.ProducerSpec = ProducerSpec{Name: name}
		if _, _, err := uc.Create(ctx, producer); err != nil {
			t.Fatalf("Create(%q) error = %v", name, err)
		}
	}

	// 非法分页参数规整为默认值。
	items, total, err := uc.List(ctx, ProducerFilter{}, 0, 0)
	if err != nil {
		t.Fatalf("List(zero paging) error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("List(zero paging) = %d items, total %d, want 3/3", len(items), total)
	}

	// keyword / enabled 过滤透传仓储：按名定位停用 monitor-engine 后组合过滤
	//（桩迭代 map 顺序随机，不可按下标取受害者）。
	var monitorID string
	for _, item := range items {
		if item.Name == "monitor-engine" {
			monitorID = item.ID
		}
	}
	disabled := false
	if err := uc.Update(ctx, monitorID, ProducerUpdate{Enabled: &disabled}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	items, total, err = uc.List(ctx, ProducerFilter{Keyword: "engine", Enabled: &disabled}, 1, 20)
	if err != nil {
		t.Fatalf("List(keyword+enabled) error = %v", err)
	}
	if total != 1 || len(items) != 1 || items[0].Name != "monitor-engine" {
		t.Fatalf("List(keyword+enabled) = %+v, total %d", items, total)
	}
}

// TestProducerUsecase_NameRunesAndExpiredEdit 名称按字符计长（与 proto
// max_len / varchar(64) 同口径，64 个 CJK 字符合法）；已过期生产者仍可
// 改名/改白名单（过期时间未变不重校验），改动过期时间时才要求在未来。
func TestProducerUsecase_NameRunesAndExpiredEdit(t *testing.T) {
	t.Parallel()
	uc, stub := newProducerUC()
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{strings.Repeat("名", 64), true},
		{strings.Repeat("名", 65), false},
	} {
		producer := &Producer{Enabled: true}
		producer.ProducerSpec = ProducerSpec{Name: tc.name}
		_, _, err := uc.Create(ctx, producer)
		if (err == nil) != tc.valid {
			t.Errorf("Create(name %d runes) error = %v, want valid=%v", len([]rune(tc.name)), err, tc.valid)
		}
	}

	far := time.Now().UTC().Add(time.Hour)
	producer := &Producer{Enabled: true}
	producer.ProducerSpec = ProducerSpec{Name: "engine-x", ExpireTime: &far}
	created, _, err := uc.Create(ctx, producer)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	// 模拟时间流逝：库内已过期。
	past := time.Now().UTC().Add(-time.Hour)
	stub.producers[created.ID].ExpireTime = &past
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Spec: &ProducerSpec{Name: "engine-y", ExpireTime: &past}}); err != nil {
		t.Fatalf("Update(rename expired producer, expire unchanged) error = %v, want nil", err)
	}
	other := past.Add(-time.Minute)
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Spec: &ProducerSpec{Name: "engine-y", ExpireTime: &other}}); !errors.Is(err, ErrProducerInvalid) {
		t.Fatalf("Update(move expire into past) error = %v, want ErrProducerInvalid", err)
	}
}

// TestProducerUsecase_Quota 配额字段随 spec 整组替换，负值拒绝，0 = 不限。
func TestProducerUsecase_Quota(t *testing.T) {
	t.Parallel()
	uc, _ := newProducerUC()
	ctx := context.Background()
	producer := &Producer{Enabled: true}
	producer.ProducerSpec = ProducerSpec{Name: "quota", QPSLimit: 50, DailyLimit: 100000}
	created, _, err := uc.Create(ctx, producer)
	if err != nil || created.QPSLimit != 50 || created.DailyLimit != 100000 {
		t.Fatalf("Create = %+v, %v", created, err)
	}
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Spec: &ProducerSpec{Name: "quota", QPSLimit: -1}}); !errors.Is(err, ErrProducerInvalid) {
		t.Fatalf("negative quota: %v", err)
	}
	if err := uc.Update(ctx, created.ID, ProducerUpdate{Spec: &ProducerSpec{Name: "quota"}}); err != nil {
		t.Fatal(err)
	}
	got, _ := uc.Get(ctx, created.ID)
	if got.QPSLimit != 0 || got.DailyLimit != 0 {
		t.Fatalf("spec replace must reset quota to unlimited: %+v", got)
	}
}
