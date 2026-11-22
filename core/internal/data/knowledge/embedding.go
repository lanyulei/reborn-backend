package data

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	biz "github.com/lanyulei/reborn/backend/core/internal/biz/knowledge"
	"github.com/lanyulei/reborn/backend/core/internal/conf"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
)

// embedding 客户端参数。
const (
	defaultEmbedTimeout   = 30 * time.Second
	embedConnectTimeout   = 3 * time.Second
	embedMaxInputChars    = 8000
	embedResponseHeaderTO = 60 * time.Second
)

// 编译期断言锁住 biz 契约。
var _ biz.Embedder = (*EmbeddingClient)(nil)

// EmbeddingClient OpenAI 兼容 /v1/embeddings 客户端。
type EmbeddingClient struct {
	client  openai.Client
	model   string
	enabled bool
	timeout time.Duration
	// noDims 端点不接受 dimensions 参数时置位，此后不再携带（结果维度仍须为 1024，由 biz 校验）。
	noDims atomic.Bool
}

// NewEmbedder new an EmbeddingClient；api_key 为空时构造停用客户端。
func NewEmbedder(c *conf.Knowledge) *EmbeddingClient {
	e := c.GetEmbedding()
	timeout := defaultEmbedTimeout
	if d := e.GetTimeout(); d != nil && d.AsDuration() > 0 {
		timeout = d.AsDuration()
	}
	httpClient := &http.Client{Transport: &http.Transport{
		Proxy:        http.ProxyFromEnvironment,
		MaxIdleConns: 8, MaxIdleConnsPerHost: 4, IdleConnTimeout: 90 * time.Second,
		DialContext:           (&net.Dialer{Timeout: embedConnectTimeout}).DialContext,
		TLSHandshakeTimeout:   embedConnectTimeout,
		ResponseHeaderTimeout: embedResponseHeaderTO,
	}}
	opts := []option.RequestOption{option.WithAPIKey(e.GetApiKey()), option.WithHTTPClient(httpClient), option.WithMaxRetries(1)}
	if base := strings.TrimSpace(e.GetBaseUrl()); base != "" {
		opts = append(opts, option.WithBaseURL(base))
	}
	return &EmbeddingClient{client: openai.NewClient(opts...), model: e.GetModel(), enabled: e.GetApiKey() != "" && e.GetModel() != "", timeout: timeout}
}

// Enabled implements biz.Embedder.
func (c *EmbeddingClient) Enabled() bool { return c.enabled }

// Model implements biz.Embedder.
func (c *EmbeddingClient) Model() string { return c.model }

// Embed implements biz.Embedder：先带 dimensions=1024 请求；端点拒绝该参数时去掉重试并记住。
func (c *EmbeddingClient) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	if !c.enabled {
		return nil, errors.New("knowledge: embedding not configured")
	}
	if len(texts) == 0 {
		return nil, nil
	}
	inputs := make([]string, len(texts))
	for i, t := range texts {
		if rs := []rune(t); len(rs) > embedMaxInputChars {
			t = string(rs[:embedMaxInputChars])
		}
		if strings.TrimSpace(t) == "" {
			t = " "
		}
		inputs[i] = t
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	withDims := !c.noDims.Load()
	out, err := c.call(ctx, inputs, withDims)
	var apiErr *openai.Error
	if err != nil && withDims && errors.As(err, &apiErr) && apiErr.StatusCode >= 400 && apiErr.StatusCode < 500 {
		if out, err = c.call(ctx, inputs, false); err == nil {
			c.noDims.Store(true)
		}
	}
	return out, err
}

func (c *EmbeddingClient) call(ctx context.Context, inputs []string, withDims bool) ([][]float32, error) {
	params := openai.EmbeddingNewParams{
		Input:          openai.EmbeddingNewParamsInputUnion{OfArrayOfStrings: inputs},
		Model:          openai.EmbeddingModel(c.model),
		EncodingFormat: openai.EmbeddingNewParamsEncodingFormatFloat,
	}
	if withDims {
		params.Dimensions = param.NewOpt(int64(biz.EmbeddingDimensions))
	}
	resp, err := c.client.Embeddings.New(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(resp.Data) != len(inputs) {
		return nil, fmt.Errorf("knowledge: embedding returned %d vectors for %d inputs", len(resp.Data), len(inputs))
	}
	out := make([][]float32, len(inputs))
	for _, d := range resp.Data {
		if d.Index < 0 || int(d.Index) >= len(inputs) {
			return nil, fmt.Errorf("knowledge: embedding index %d out of range", d.Index)
		}
		vec := make([]float32, len(d.Embedding))
		for i, f := range d.Embedding {
			vec[i] = float32(f)
		}
		out[d.Index] = vec
	}
	return out, nil
}
