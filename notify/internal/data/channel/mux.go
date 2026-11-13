// Package channel 实现 biz.ChannelSender 渠道适配器（出站 I/O 唯一落位）：
// email（SMTP）、webhook（HTTPS + HMAC 签名）、群机器人（dingtalk / wecom / feishu）。
package channel

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// NewMux 装配全部适配器为单一 ChannelSender（wire 入口）。
func NewMux() biz.ChannelSender {
	return biz.NewSenderMux(NewEmailSender(), NewWebhookSender(), NewRobotSender())
}

// scrubURLError 去掉 *url.Error 里的完整 URL，只留操作与根因。net/http 的出站错误文本
// 自带请求 URL——机器人的 access_token / key / hook 路径、webhook 的查询串都在其中，
// 而错误会落入 notify_dispatch.last_error、delivery_log、死信与日志。错误链保留
// （errors.Is / As 仍可穿透到根因）。
func scrubURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
