package biz

import (
	"context"
	"errors"
	"testing"
)

// 登录的「校验口令」与「落刷新令牌」之间，管理员可能已重置口令（先改哈希、再吊销全部会话）。仓储对登录签发
// 做条件插入，落不下时返回 ErrTokenInvalid：此时登录必须按凭据失败处理，不能把新会话交出去——否则它落在
// 吊销之后，重置口令也踢不掉它。
func TestAuthUsecase_LoginTokenIssuanceRejectedWhenCredentialsChanged(t *testing.T) {
	t.Parallel()
	uc, stub, refresh, logs := newAuthUsecase(t)
	seedLoginUser(t, stub, "racer", StatusEnabled)
	refresh.rejectIssuance = true

	user, pair, err := uc.Login(context.Background(), "racer", "plain-secret", "10.0.0.9", "ua")
	if !errors.Is(err, ErrInvalidCredentials) || user != nil || pair != nil {
		t.Fatalf("Login() = (%v, %v, %v); want ErrInvalidCredentials and no session", user, pair, err)
	}
	if len(refresh.tokens) != 0 {
		t.Fatalf("refresh tokens = %d, want none", len(refresh.tokens))
	}
	if len(logs.entries) != 1 || logs.entries[0].Result != LoginResultTokenIssueFailed {
		t.Fatalf("login audit = %+v, want one token-issue-failed record", logs.entries)
	}
	// 故障排除后（口令已换、仓储恢复）用新口令可以正常登录。
	refresh.rejectIssuance = false
	if _, _, err := uc.Login(context.Background(), "racer", "plain-secret", "10.0.0.9", "ua"); err != nil {
		t.Fatalf("Login() after the condition clears: error = %v", err)
	}
}

// 登录签发的刷新令牌带着签发时的口令哈希；刷新轮换不带（口令已另行校验）。
func TestAuthUsecase_RefreshTokenCarriesIssuedForHashOnlyOnLogin(t *testing.T) {
	t.Parallel()
	uc, stub, refresh, _ := newAuthUsecase(t)
	u := seedLoginUser(t, stub, "hasher", StatusEnabled)
	if _, pair, err := uc.Login(context.Background(), "hasher", "plain-secret", "10.0.0.9", "ua"); err != nil {
		t.Fatalf("Login() error = %v", err)
	} else if issued := refresh.tokens[hashRefreshToken(pair.RefreshToken)]; issued == nil || issued.IssuedForHash != u.Password {
		t.Fatalf("login-issued refresh token = %+v; want IssuedForHash = the verified password hash", issued)
	}
}
