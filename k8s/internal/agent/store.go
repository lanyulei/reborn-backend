package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// TokenStore 长期令牌的持久化。Load 在没有令牌时返回空串。
type TokenStore interface {
	Load(ctx context.Context) (string, error)
	Save(ctx context.Context, token string) error
	Delete(ctx context.Context) error
}

// fileStore 转发节点：令牌存为状态目录下的 0600 文件。
type fileStore struct{ path string }

// NewFileStore 以状态目录构造文件存储。
func NewFileStore(dir string) TokenStore { return fileStore{path: filepath.Join(dir, "token")} }

func (f fileStore) Load(context.Context) (string, error) {
	raw, err := os.ReadFile(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	return strings.TrimSpace(string(raw)), err
}

func (f fileStore) Save(_ context.Context, token string) error {
	if err := os.MkdirAll(filepath.Dir(f.path), 0o700); err != nil {
		return err
	}
	tmp := f.path + ".tmp"
	if err := os.WriteFile(tmp, []byte(token+"\n"), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.path)
}

func (f fileStore) Delete(context.Context) error {
	err := os.Remove(f.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// 集群内：令牌存为 Agent 所在命名空间的 Secret。
const (
	tokenSecretName = "reborn-agent-token"
	tokenSecretKey  = "token"
)

type secretStore struct {
	client    kubernetes.Interface
	namespace string
}

// NewSecretStore 以集群客户端与命名空间构造 Secret 存储。
func NewSecretStore(client kubernetes.Interface, namespace string) TokenStore {
	return secretStore{client: client, namespace: namespace}
}

func (s secretStore) Load(ctx context.Context) (string, error) {
	sec, err := s.client.CoreV1().Secrets(s.namespace).Get(ctx, tokenSecretName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read token secret: %w", err)
	}
	return strings.TrimSpace(string(sec.Data[tokenSecretKey])), nil
}

func (s secretStore) Save(ctx context.Context, token string) error {
	api := s.client.CoreV1().Secrets(s.namespace)
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: tokenSecretName, Namespace: s.namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "reborn"}},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{tokenSecretKey: []byte(token)},
	}
	_, err := api.Create(ctx, sec, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		_, err = api.Update(ctx, sec, metav1.UpdateOptions{})
	}
	return err
}

func (s secretStore) Delete(ctx context.Context) error {
	err := s.client.CoreV1().Secrets(s.namespace).Delete(ctx, tokenSecretName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
