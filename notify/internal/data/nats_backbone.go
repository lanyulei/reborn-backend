package data

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
	"github.com/lanyulei/reborn/backend/notify/internal/conf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// 骨干取值。
const (
	BackbonePG   = "pg"
	BackboneNATS = "nats"
)

// NATS 流与消费者命名（<prefix>_EVENTS / <prefix>_DISPATCHES；durable 固定）。
// 去重窗口 = min(2m, AckWait)：Msg-Id 带发布代际（事件 publish_attempts / 任务 attempts），
// relay 超期重发（≥ 2×AckWait）不会被窗口吞掉。
const (
	natsSubjectEvents     = "notify.event.>"
	natsSubjectDispatches = "notify.dispatch.>"
	natsConsumerRouter    = "router"
	natsConsumerSender    = "sender"
	natsDedupWindow       = 2 * time.Minute
	natsStreamMaxAge      = 7 * 24 * time.Hour
)

// NatsOptions 骨干参数（缺省值见 natsOptionsFrom）。
type NatsOptions struct {
	URL          string
	Token        string
	StreamPrefix string
	AckWait      time.Duration
	MaxDeliver   int
	RelayPoll    time.Duration
	RelayBatch   int
	Replicas     int
}

func natsOptionsFrom(c *conf.Pipeline) NatsOptions {
	n := c.GetNats()
	o := NatsOptions{
		URL: n.GetUrl(), Token: n.GetToken(), StreamPrefix: n.GetStreamPrefix(),
		AckWait: n.GetAckWait().AsDuration(), MaxDeliver: int(n.GetMaxDeliver()),
		RelayPoll: n.GetRelayPoll().AsDuration(), RelayBatch: int(n.GetRelayBatch()), Replicas: int(n.GetReplicas()),
	}
	return o.withDefaults()
}

func (o NatsOptions) withDefaults() NatsOptions {
	if o.URL == "" {
		o.URL = nats.DefaultURL
	}
	if o.StreamPrefix == "" {
		o.StreamPrefix = "NOTIFY"
	}
	if o.AckWait <= 0 {
		o.AckWait = 2 * time.Minute
	}
	if o.MaxDeliver <= 0 {
		o.MaxDeliver = 20
	}
	if o.RelayPoll <= 0 {
		o.RelayPoll = 200 * time.Millisecond
	}
	if o.RelayBatch <= 0 {
		o.RelayBatch = 500
	}
	if o.Replicas <= 0 {
		o.Replicas = 1
	}
	return o
}

// NatsBackbone JetStream 连接与两条 WorkQueue 流（EVENTS / DISPATCHES）及其 pull 型
// durable 消费者。PG 仍是事实源：流里只放 {id, create_time} 游标，消费侧以 PG 条件
// UPDATE 做门闩/围栏；消息丢失或 MaxDeliver 耗尽由 relay 的超期重发兜底。
type NatsBackbone struct {
	nc         *nats.Conn
	js         jetstream.JetStream
	events     jetstream.Stream
	dispatches jetstream.Stream
	router     jetstream.Consumer
	sender     jetstream.Consumer
	opts       NatsOptions
}

// NewNatsBackbone 按配置构造；backbone≠nats 返回 nil（PG 骨干），非法取值装配失败。
func NewNatsBackbone(c *conf.Pipeline) (*NatsBackbone, func(), error) {
	switch strings.ToLower(strings.TrimSpace(c.GetBackbone())) {
	case "", BackbonePG:
		return nil, func() {}, nil
	case BackboneNATS:
		return ConnectNats(context.Background(), natsOptionsFrom(c))
	default:
		return nil, nil, fmt.Errorf("unknown pipeline backbone %q (want pg or nats)", c.GetBackbone())
	}
}

// ConnectNats 连接并幂等创建流/消费者（CreateOrUpdate：参数变更随进程重启生效）。
func ConnectNats(ctx context.Context, opts NatsOptions) (*NatsBackbone, func(), error) {
	opts = opts.withDefaults()
	nopts := []nats.Option{nats.Name("reborn-notify"), nats.MaxReconnects(-1), nats.ReconnectWait(2 * time.Second)}
	if opts.Token != "" {
		nopts = append(nopts, nats.Token(opts.Token))
	}
	nc, err := nats.Connect(opts.URL, nopts...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect nats %s: %w", opts.URL, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("init jetstream: %w", err)
	}
	b := &NatsBackbone{nc: nc, js: js, opts: opts}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if b.events, err = js.CreateOrUpdateStream(ctx, b.streamConfig("EVENTS", natsSubjectEvents)); err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("ensure events stream: %w", err)
	}
	if b.dispatches, err = js.CreateOrUpdateStream(ctx, b.streamConfig("DISPATCHES", natsSubjectDispatches)); err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("ensure dispatches stream: %w", err)
	}
	if b.router, err = b.events.CreateOrUpdateConsumer(ctx, b.consumerConfig(natsConsumerRouter)); err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("ensure router consumer: %w", err)
	}
	if b.sender, err = b.dispatches.CreateOrUpdateConsumer(ctx, b.consumerConfig(natsConsumerSender)); err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("ensure sender consumer: %w", err)
	}
	return b, func() { _ = nc.Drain(); nc.Close() }, nil
}

func (b *NatsBackbone) streamConfig(suffix, subject string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name: b.opts.StreamPrefix + "_" + suffix, Subjects: []string{subject},
		Retention: jetstream.WorkQueuePolicy, Storage: jetstream.FileStorage, Replicas: b.opts.Replicas,
		Duplicates: min(natsDedupWindow, b.opts.AckWait), MaxAge: natsStreamMaxAge, Discard: jetstream.DiscardOld,
	}
}

func (b *NatsBackbone) consumerConfig(durable string) jetstream.ConsumerConfig {
	return jetstream.ConsumerConfig{
		Durable: durable, AckPolicy: jetstream.AckExplicitPolicy, AckWait: b.opts.AckWait,
		MaxDeliver: b.opts.MaxDeliver, MaxAckPending: 10_000, DeliverPolicy: jetstream.DeliverAllPolicy,
	}
}

// Options 生效参数。
func (b *NatsBackbone) Options() NatsOptions { return b.opts }

// Ping 连接健康（/healthz）。
func (b *NatsBackbone) Ping(ctx context.Context) error {
	if b == nil {
		return nil
	}
	if !b.nc.IsConnected() {
		return errors.New("nats disconnected")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := b.events.Info(ctx)
	return err
}

// fetch 拉取至多 n 条（无消息等待 maxWait）；返回已到达的消息集合。
func fetch(ctx context.Context, c jetstream.Consumer, n int, maxWait time.Duration) ([]jetstream.Msg, error) {
	batch, err := c.Fetch(n, jetstream.FetchMaxWait(maxWait))
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	var out []jetstream.Msg
	for {
		select {
		case m, ok := <-batch.Messages():
			if !ok {
				if err := batch.Error(); err != nil && !errors.Is(err, nats.ErrTimeout) {
					return out, fmt.Errorf("fetch batch: %w", err)
				}
				return out, nil
			}
			out = append(out, m)
		case <-ctx.Done():
			return out, ctx.Err()
		}
	}
}

// cursor 流消息体：行主键 + 分区键（create_time 用于分区剪枝）。
type cursor struct {
	ID         string    `json:"id"`
	CreateTime time.Time `json:"ct"`
	ChannelID  string    `json:"ch,omitempty"`
}

// nakAfter 延后重投（不计 MaxDeliver 以外的任何状态）；失败只记录——AckWait 到期会
// 自然重投，语义等价只是更慢。
func nakAfter(m jetstream.Msg, d time.Duration) {
	if d < 0 {
		d = 0
	}
	_ = m.NakWithDelay(d)
}

// BackboneStatus implements biz.Backbone：骨干名称与连通性。
type BackboneStatus struct {
	name string
	nb   *NatsBackbone
}

// NewBackboneStatus 构造骨干状态（pg 下 Ping 恒 nil）。
func NewBackboneStatus(c *conf.Pipeline, nb *NatsBackbone) biz.Backbone {
	name := BackbonePG
	if nb != nil {
		name = BackboneNATS
	}
	_ = c
	return &BackboneStatus{name: name, nb: nb}
}

// Name implements biz.Backbone.
func (s *BackboneStatus) Name() string { return s.name }

// Ping implements biz.Backbone.
func (s *BackboneStatus) Ping(ctx context.Context) error { return s.nb.Ping(ctx) }
