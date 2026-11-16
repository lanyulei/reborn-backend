package data

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lanyulei/reborn/backend/core/internal/conf"
	basedata "github.com/lanyulei/reborn/backend/core/internal/data"
	"github.com/lanyulei/reborn/backend/core/internal/pkg/storage"
)

const (
	testAccessKey = "AKIDEXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY" //nolint:gosec // AWS 文档里的公开示例密钥，非真实凭据
	testRegion    = "ap-east-1"
	testKey       = "2026/11/16/018f3e1a-00f0-7000-8000-0000000000aa"
)

// fakeS3 最小的 S3 假服务：独立按 SigV4 规范重算签名并校验，再按「主机 + 路径」存取对象。
type fakeS3 struct {
	t       *testing.T
	mu      sync.Mutex
	objects map[string][]byte
	// fail 非零时所有请求返回该状态码与一段 S3 风格的错误体。
	fail int
	// deleteMissing 删除不存在的键时返回的状态码（标准为 204，个别兼容实现为 404）。
	deleteMissing int
}

func mac(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// expectedAuth 按规范文本从请求原样重算 Authorization（与被测实现各写各的）。
func expectedAuth(r *http.Request, payloadHash string) string {
	amzDate := r.Header.Get("X-Amz-Date")
	day := amzDate[:8]
	canonical := r.Method + "\n" + r.URL.EscapedPath() + "\n\n" +
		"host:" + r.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n\n" +
		"host;x-amz-content-sha256;x-amz-date\n" + payloadHash
	sum := sha256.Sum256([]byte(canonical))
	scope := day + "/" + testRegion + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	k := mac(mac(mac(mac([]byte("AWS4"+testSecretKey), day), testRegion), "s3"), "aws4_request")
	return "AWS4-HMAC-SHA256 Credential=" + testAccessKey + "/" + scope +
		", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + hex.EncodeToString(mac(k, toSign))
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	if got := r.Header.Get("X-Amz-Content-Sha256"); got != payloadHash {
		f.t.Errorf("x-amz-content-sha256 = %q, want the body hash %q", got, payloadHash)
	}
	if got, want := r.Header.Get("Authorization"), expectedAuth(r, payloadHash); got != want {
		f.t.Errorf("Authorization mismatch:\n got %s\nwant %s", got, want)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if r.URL.RawQuery != "" {
		f.t.Errorf("unexpected query %q", r.URL.RawQuery)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != 0 {
		w.WriteHeader(f.fail)
		_, _ = io.WriteString(w, `<?xml version="1.0"?><Error><Code>SlowDown</Code><Message>secret detail</Message></Error>`)
		return
	}
	id := r.Host + r.URL.Path
	switch r.Method {
	case http.MethodPut:
		f.objects[id] = body
	case http.MethodGet:
		data, ok := f.objects[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
			return
		}
		_, _ = w.Write(data)
	case http.MethodDelete:
		if _, ok := f.objects[id]; !ok && f.deleteMissing != 0 {
			w.WriteHeader(f.deleteMissing)
			return
		}
		delete(f.objects, id)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// newS3 起一个假服务并构造指向它的 S3Store。虚拟主机式下把「桶名.主机」的连接改拨到假服务。
func newS3(t *testing.T, pathStyle bool, prefix string, maxSize int64) (*S3Store, *fakeS3, string) {
	t.Helper()
	fake := &fakeS3{t: t, objects: map[string][]byte{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	store, err := NewS3Store(S3Config{
		Endpoint: srv.URL, Region: testRegion, Bucket: "attachments",
		AccessKey: testAccessKey, SecretKey: testSecretKey, PathStyle: pathStyle, Prefix: prefix,
	}, maxSize)
	if err != nil {
		t.Fatalf("NewS3Store() error = %v", err)
	}
	addr := strings.TrimPrefix(srv.URL, "http://")
	if !pathStyle {
		// 只把实际连接改拨到假服务，Host 头保持「桶名.主机」。
		transport := &http.Transport{}
		t.Cleanup(transport.CloseIdleConnections)
		store.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			clone := r.Clone(r.Context())
			clone.Host = r.URL.Host
			clone.URL.Host = addr
			return transport.RoundTrip(clone)
		})
	}
	return store, fake, addr
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestS3Store_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		pathStyle bool
		prefix    string
		wantID    func(addr string) string
	}{
		"path style":               {true, "", func(addr string) string { return addr + "/attachments/" + testKey }},
		"path style with prefix":   {true, "/prod/files/", func(addr string) string { return addr + "/attachments/prod/files/" + testKey }},
		"virtual host":             {false, "", func(addr string) string { return "attachments." + addr + "/" + testKey }},
		"virtual host with prefix": {false, "prod", func(addr string) string { return "attachments." + addr + "/prod/" + testKey }},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, fake, addr := newS3(t, c.pathStyle, c.prefix, 1<<20)
			ctx := context.Background()
			if _, err := store.Get(ctx, testKey); !errors.Is(err, storage.ErrObjectNotFound) {
				t.Fatalf("Get(missing) error = %v, want ErrObjectNotFound", err)
			}
			if err := store.Delete(ctx, testKey); err != nil {
				t.Fatalf("Delete(missing) error = %v, want nil", err)
			}
			for _, data := range [][]byte{[]byte("hello"), {}, bytes.Repeat([]byte{0xff}, 4096)} {
				if err := store.Put(ctx, testKey, data); err != nil {
					t.Fatalf("Put(%d bytes) error = %v", len(data), err)
				}
				got, err := store.Get(ctx, testKey)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("Get() = %d bytes, err %v; want %d bytes", len(got), err, len(data))
				}
			}
			fake.mu.Lock()
			_, stored := fake.objects[c.wantID(addr)]
			fake.mu.Unlock()
			if !stored {
				t.Fatalf("object not stored at %q; have %v", c.wantID(addr), keysOf(fake.objects))
			}
			if err := store.Delete(ctx, testKey); err != nil {
				t.Fatalf("Delete() error = %v", err)
			}
			if _, err := store.Get(ctx, testKey); !errors.Is(err, storage.ErrObjectNotFound) {
				t.Fatalf("Get(after delete) error = %v, want ErrObjectNotFound", err)
			}
		})
	}
}

func keysOf(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestS3Store_Failures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("server error carries status and code only", func(t *testing.T) {
		t.Parallel()
		store, fake, _ := newS3(t, true, "", 1<<20)
		fake.fail = http.StatusServiceUnavailable
		for name, err := range map[string]error{
			"put":    store.Put(ctx, testKey, []byte("x")),
			"delete": store.Delete(ctx, testKey),
			"get":    func() error { _, err := store.Get(ctx, testKey); return err }(),
		} {
			if err == nil || errors.Is(err, storage.ErrObjectNotFound) {
				t.Fatalf("%s error = %v, want a plain failure", name, err)
			}
			msg := err.Error()
			if !strings.Contains(msg, "503") || !strings.Contains(msg, "SlowDown") ||
				strings.Contains(msg, "secret detail") || strings.Contains(msg, testSecretKey) {
				t.Fatalf("%s error message = %q", name, msg)
			}
		}
	})

	t.Run("delete tolerates 404 for a missing key", func(t *testing.T) {
		t.Parallel()
		store, fake, _ := newS3(t, true, "", 1<<20)
		fake.deleteMissing = http.StatusNotFound
		if err := store.Delete(ctx, testKey); err != nil {
			t.Fatalf("Delete(404) error = %v, want nil", err)
		}
	})

	t.Run("oversized object is refused", func(t *testing.T) {
		t.Parallel()
		store, _, _ := newS3(t, true, "", 16)
		if err := store.Put(ctx, testKey, bytes.Repeat([]byte("a"), 17)); err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if _, err := store.Get(ctx, testKey); err == nil || !strings.Contains(err.Error(), "size limit") {
			t.Fatalf("Get(oversized) error = %v, want size limit error", err)
		}
		if err := store.Put(ctx, testKey, bytes.Repeat([]byte("a"), 16)); err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if got, err := store.Get(ctx, testKey); err != nil || len(got) != 16 {
			t.Fatalf("Get(at limit) = %d bytes, err %v", len(got), err)
		}
	})

	t.Run("invalid keys never reach the network", func(t *testing.T) {
		t.Parallel()
		store, fake, _ := newS3(t, true, "", 1<<20)
		for _, key := range []string{"", "../etc/passwd", "2026/11/16/../../x", testKey + "/..", "a b", testKey + "?acl"} {
			if err := store.Put(ctx, key, []byte("x")); err == nil {
				t.Fatalf("Put(%q) must fail", key)
			}
			if _, err := store.Get(ctx, key); err == nil || errors.Is(err, storage.ErrObjectNotFound) {
				t.Fatalf("Get(%q) error = %v, want invalid key", key, err)
			}
			if err := store.Delete(ctx, key); err == nil {
				t.Fatalf("Delete(%q) must fail", key)
			}
		}
		if len(fake.objects) != 0 {
			t.Fatalf("unexpected objects: %v", keysOf(fake.objects))
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		t.Parallel()
		store, _, _ := newS3(t, true, "", 1<<20)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if err := store.Put(cctx, testKey, []byte("x")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put(cancelled) error = %v, want context.Canceled", err)
		}
	})

	t.Run("redirects are not followed", func(t *testing.T) {
		t.Parallel()
		var hits int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits++
			http.Redirect(w, r, "/elsewhere", http.StatusTemporaryRedirect)
		}))
		t.Cleanup(srv.Close)
		store, err := NewS3Store(S3Config{Endpoint: srv.URL, Region: testRegion, Bucket: "attachments", AccessKey: "a", SecretKey: "s", PathStyle: true}, 1<<20)
		if err != nil {
			t.Fatalf("NewS3Store() error = %v", err)
		}
		if _, err := store.Get(ctx, testKey); err == nil || !strings.Contains(err.Error(), "307") || hits != 1 {
			t.Fatalf("Get(redirect) error = %v after %d hits; want a 307 failure after 1", err, hits)
		}
	})
}

// 固定时钟下的签名：时间戳来自注入的时钟，结果与按规范文本独立重算的一致；HMAC 派生链
// 再用 AWS SigV4 文档公开的示例值（密钥 wJalr…+bPx…、20120215 / us-east-1 / iam）锁住，
// 防止被测实现与测试里的重算错在同一处。
func TestS3Store_Sign(t *testing.T) {
	t.Parallel()
	const ak, sk = "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" //nolint:gosec // AWS 文档里的公开示例密钥，非真实凭据
	store := &S3Store{
		cfg: S3Config{Region: "us-east-1", AccessKey: ak, SecretKey: sk},
		now: func() time.Time { return time.Date(2013, 5, 24, 8, 0, 0, 0, time.FixedZone("CST", 8*3600)) },
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	empty := sha256.Sum256(nil)
	store.sign(req, hex.EncodeToString(empty[:]))
	if got := req.Header.Get("X-Amz-Date"); got != "20130524T000000Z" {
		t.Fatalf("X-Amz-Date = %q, want UTC 20130524T000000Z", got)
	}
	if got, want := req.Header.Get("Authorization"), expectedAuthFor(req, ak, sk, "us-east-1", hex.EncodeToString(empty[:])); got != want {
		t.Fatalf("Authorization = %s, want %s", got, want)
	}
	kSigning := mac(mac(mac(mac([]byte("AWS4wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"), "20120215"), "us-east-1"), "iam"), "aws4_request")
	if got := hex.EncodeToString(kSigning); got != "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d" {
		t.Fatalf("signing key derivation = %s", got)
	}
}

func expectedAuthFor(r *http.Request, ak, sk, region, payloadHash string) string {
	amzDate := r.Header.Get("X-Amz-Date")
	day := amzDate[:8]
	canonical := r.Method + "\n" + r.URL.EscapedPath() + "\n\nhost:" + r.Host + "\nx-amz-content-sha256:" + payloadHash +
		"\nx-amz-date:" + amzDate + "\n\nhost;x-amz-content-sha256;x-amz-date\n" + payloadHash
	sum := sha256.Sum256([]byte(canonical))
	scope := day + "/" + region + "/s3/aws4_request"
	k := mac(mac(mac(mac([]byte("AWS4"+sk), day), region), "s3"), "aws4_request")
	return "AWS4-HMAC-SHA256 Credential=" + ak + "/" + scope + ", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" +
		hex.EncodeToString(mac(k, "AWS4-HMAC-SHA256\n"+amzDate+"\n"+scope+"\n"+hex.EncodeToString(sum[:])))
}

func TestNewS3Store_Validation(t *testing.T) {
	t.Parallel()
	valid := S3Config{Endpoint: "https://s3.example.com", Region: "us-east-1", Bucket: "attachments", AccessKey: "a", SecretKey: "s"}
	if _, err := NewS3Store(valid, 1); err != nil {
		t.Fatalf("NewS3Store(valid) error = %v", err)
	}
	for name, mut := range map[string]func(*S3Config){
		"no scheme":         func(c *S3Config) { c.Endpoint = "s3.example.com" },
		"ftp scheme":        func(c *S3Config) { c.Endpoint = "ftp://s3.example.com" },
		"endpoint path":     func(c *S3Config) { c.Endpoint = "https://s3.example.com/bucket" },
		"endpoint query":    func(c *S3Config) { c.Endpoint = "https://s3.example.com?x=1" },
		"endpoint userinfo": func(c *S3Config) { c.Endpoint = "https://u:p@s3.example.com" },
		"empty region":      func(c *S3Config) { c.Region = "" },
		"bad bucket":        func(c *S3Config) { c.Bucket = "My_Bucket" },
		"empty bucket":      func(c *S3Config) { c.Bucket = "" },
		"no access key":     func(c *S3Config) { c.AccessKey = "" },
		"no secret key":     func(c *S3Config) { c.SecretKey = "" },
		"prefix traversal":  func(c *S3Config) { c.Prefix = "a/../b" },
		"prefix with space": func(c *S3Config) { c.Prefix = "a b" },
	} {
		c := valid
		mut(&c)
		if _, err := NewS3Store(c, 1); err == nil {
			t.Errorf("NewS3Store(%s) must fail", name)
		} else if strings.Contains(err.Error(), "u:p") {
			t.Errorf("NewS3Store(%s) error leaks endpoint credentials: %v", name, err)
		}
	}
	if _, err := NewS3Store(valid, 0); err == nil {
		t.Error("NewS3Store(max size 0) must fail")
	}
}

func TestNewStores(t *testing.T) {
	t.Parallel()
	d := &basedata.Data{LogoDir: t.TempDir()}
	s3 := &conf.Data_Upload_S3{Endpoint: "http://127.0.0.1:9000", Bucket: "attachments", AccessKey: "a", SecretKey: "s", PathStyle: true}
	cases := map[string]struct {
		upload  *conf.Data_Upload
		def     string
		drivers int
		fail    bool
	}{
		"default local":            {nil, DriverLocal, 1, false},
		"local with s3 registered": {&conf.Data_Upload{Driver: "local", S3: s3}, DriverLocal, 2, false},
		"s3 default":               {&conf.Data_Upload{Driver: "s3", S3: s3}, DriverS3, 2, false},
		"s3 without config":        {&conf.Data_Upload{Driver: "s3"}, "", 0, true},
		"s3 missing secret":        {&conf.Data_Upload{Driver: "local", S3: &conf.Data_Upload_S3{Endpoint: "http://127.0.0.1:9000", Bucket: "attachments", AccessKey: "a"}}, "", 0, true},
		"unknown driver":           {&conf.Data_Upload{Driver: "oss"}, "", 0, true},
	}
	for name, c := range cases {
		stores, err := NewStores(&conf.Data{Upload: c.upload}, d)
		if (err != nil) != c.fail {
			t.Errorf("NewStores(%s) error = %v, want failure = %v", name, err, c.fail)
			continue
		}
		if !c.fail && (stores.Default != c.def || len(stores.Drivers) != c.drivers) {
			t.Errorf("NewStores(%s) = default %q with %d drivers, want %q with %d", name, stores.Default, len(stores.Drivers), c.def, c.drivers)
		}
	}
}
