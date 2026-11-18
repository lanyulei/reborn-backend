package data

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"
	"github.com/lanyulei/reborn/backend/kit/sigv4"
)

// DriverS3 S3 兼容对象存储驱动名（file_object.driver）：AWS S3 / MinIO / 阿里云 OSS 的
// S3 兼容端点等。
const DriverS3 = "s3"

const (
	// s3RequestTimeout 单次请求（含读完响应体）的上限；附件上限 5MiB，足够宽裕。
	s3RequestTimeout = 30 * time.Second
	// s3ErrorBodyLimit 出错时为取错误码最多读取的响应体字节数。
	s3ErrorBodyLimit = 2 << 10
	// s3DrainLimit 关闭响应体前最多排空的字节数（让连接可复用，又不被超大响应拖住）。
	s3DrainLimit = 64 << 10
	s3Service    = "s3"
)

var (
	_ storage.ObjectStore = (*S3Store)(nil)

	// s3BucketPattern 桶名：3-63 位小写字母 / 数字 / 点 / 中划线，首尾为字母或数字。
	s3BucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	// s3PrefixPattern 键前缀：以 / 分隔的若干段，每段为字母 / 数字 / 点 / 下划线 / 中划线。
	s3PrefixPattern = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*(/[A-Za-z0-9_-][A-Za-z0-9._-]*)*$`)
	// s3RegionPattern 区域名。
	s3RegionPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	// s3ErrorCodePattern 从错误响应（XML）里取错误码，只用于报错信息。
	s3ErrorCodePattern = regexp.MustCompile(`<Code>([A-Za-z0-9.]{1,64})</Code>`)
)

// S3Config S3 驱动配置。
type S3Config struct {
	// Endpoint 服务端点（含协议，不含桶与路径），如 https://s3.ap-east-1.amazonaws.com。
	Endpoint string
	// Region 签名用的区域；MinIO 等不分区域的服务填其配置的区域（默认 us-east-1）。
	Region string
	// Bucket 桶名（须已存在；驱动不建桶）。
	Bucket string
	// AccessKey / SecretKey 访问凭据。
	AccessKey string
	SecretKey string
	// PathStyle 路径式寻址（<endpoint>/<bucket>/<key>）；false 为虚拟主机式（<bucket>.<endpoint>/<key>）。
	PathStyle bool
	// Prefix 对象键前缀，可空（多个环境共用一个桶时区分）。
	Prefix string
}

// S3Store S3 兼容对象存储：手写的最小 SigV4 客户端（PUT / GET / DELETE Object），
// 不引入第三方 SDK。请求体哈希参与签名（不使用 UNSIGNED-PAYLOAD），明文 HTTP 下
// 内容也不可被篡改。无进程内可变状态，多实例直接共用同一个桶。
type S3Store struct {
	cfg     S3Config
	scheme  string
	host    string // 实际请求的主机（虚拟主机式时带桶名）
	base    string // 对象路径前缀：路径式为 /<bucket>[/<prefix>]，虚拟主机式为 [/<prefix>]
	maxSize int64
	client  *http.Client
	now     func() time.Time
}

// NewS3Store 校验配置并构造 S3Store；maxSize 为读取对象时接受的最大字节数。
func NewS3Store(cfg S3Config, maxSize int64) (*S3Store, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
		return nil, errors.New("s3 store: endpoint must be http(s)://host[:port] without path, query or credentials")
	}
	if !s3RegionPattern.MatchString(cfg.Region) {
		return nil, errors.New("s3 store: invalid region")
	}
	if !s3BucketPattern.MatchString(cfg.Bucket) {
		return nil, errors.New("s3 store: invalid bucket name")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("s3 store: access key and secret key are required")
	}
	cfg.Prefix = strings.Trim(cfg.Prefix, "/")
	if cfg.Prefix != "" && !s3PrefixPattern.MatchString(cfg.Prefix) {
		return nil, errors.New("s3 store: invalid key prefix")
	}
	if maxSize <= 0 {
		return nil, errors.New("s3 store: max object size must be positive")
	}
	s := &S3Store{cfg: cfg, scheme: u.Scheme, host: u.Host, maxSize: maxSize, now: time.Now}
	if cfg.PathStyle {
		s.base = "/" + cfg.Bucket
	} else {
		s.host = cfg.Bucket + "." + u.Host
	}
	if cfg.Prefix != "" {
		s.base += "/" + cfg.Prefix
	}
	s.client = &http.Client{
		Timeout: s3RequestTimeout,
		// 已签名的请求不跟随重定向：换了主机签名即失效，也不把凭据派生的签名带去别处。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return s, nil
}

// objectPath 校验键并解析为请求路径。键的字符集（数字 / 小写十六进制 / 斜杠 / 中划线）
// 与前缀、桶名的字符集都不需要转义，路径即规范 URI。
func (s *S3Store) objectPath(key string) (string, error) {
	if !localKeyPattern.MatchString(key) {
		return "", fmt.Errorf("s3 store: invalid object key %q", key)
	}
	return s.base + "/" + key, nil
}

// sign 按 AWS Signature Version 4 给请求加签（kit/sigv4）：签 host、x-amz-content-sha256、
// x-amz-date 三个头与请求体哈希；无查询串。
func (s *S3Store) sign(req *http.Request, payloadHash string) {
	now := s.now().UTC()
	amzDate := now.Format(sigv4.DateFormat)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Set("Authorization", sigv4.Authorization(req.Method, req.URL.EscapedPath(), "",
		map[string]string{"host": req.Host, "x-amz-content-sha256": payloadHash, "x-amz-date": amzDate},
		payloadHash, now, s.cfg.Region, s3Service, s.cfg.AccessKey, s.cfg.SecretKey))
}

// do 发出一次已签名的请求。调用方负责 closeBody。
func (s *S3Store) do(ctx context.Context, method, key string, body []byte) (*http.Response, error) {
	path, err := s.objectPath(key)
	if err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.scheme+"://"+s.host+path, reader)
	if err != nil {
		return nil, fmt.Errorf("s3 store: build request: %w", err)
	}
	sum := sha256.Sum256(body)
	s.sign(req, hex.EncodeToString(sum[:]))
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 store: %s object: %w", strings.ToLower(method), err)
	}
	return resp, nil
}

// closeBody 排空（有上限）并关闭响应体，使连接可被复用。
func closeBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, s3DrainLimit))
	_ = resp.Body.Close()
}

// statusError 把非预期的响应转成错误：只带状态码与服务端错误码，不带响应原文。
func statusError(op string, resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, s3ErrorBodyLimit))
	code := ""
	if m := s3ErrorCodePattern.FindSubmatch(raw); m != nil {
		code = " " + string(m[1])
	}
	return fmt.Errorf("s3 store: %s object: unexpected status %d%s", op, resp.StatusCode, code)
}

// Put implements storage.ObjectStore：PutObject 对读者整体原子可见，同键覆盖。
func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	if data == nil {
		data = []byte{}
	}
	resp, err := s.do(ctx, http.MethodPut, key, data)
	if err != nil {
		return err
	}
	defer closeBody(resp)
	if resp.StatusCode != http.StatusOK {
		return statusError("put", resp)
	}
	return nil
}

// Get implements storage.ObjectStore：不存在返回 ErrObjectNotFound；超过大小上限的对象拒绝读取。
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, err
	}
	defer closeBody(resp)
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return nil, storage.ErrObjectNotFound
	default:
		return nil, statusError("get", resp)
	}
	if resp.ContentLength > s.maxSize {
		return nil, errors.New("s3 store: get object: object exceeds size limit (" + strconv.FormatInt(s.maxSize, 10) + " bytes)")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, s.maxSize+1))
	if err != nil {
		return nil, fmt.Errorf("s3 store: read object: %w", err)
	}
	if int64(len(data)) > s.maxSize {
		return nil, errors.New("s3 store: get object: object exceeds size limit (" + strconv.FormatInt(s.maxSize, 10) + " bytes)")
	}
	return data, nil
}

// Delete implements storage.ObjectStore：不存在视为成功（DeleteObject 本身幂等，
// 个别兼容实现对不存在的键返回 404，同样按成功处理）。
func (s *S3Store) Delete(ctx context.Context, key string) error {
	resp, err := s.do(ctx, http.MethodDelete, key, nil)
	if err != nil {
		return err
	}
	defer closeBody(resp)
	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusOK, http.StatusNotFound:
		return nil
	default:
		return statusError("delete", resp)
	}
}
