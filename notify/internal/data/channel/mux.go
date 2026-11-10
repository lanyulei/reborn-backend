// Package channel 实现 biz.ChannelSender 渠道适配器（出站 I/O 唯一落位）：
// email（SMTP）、webhook（HTTPS + HMAC 签名）、群机器人（dingtalk / wecom / feishu）。
package channel

import (
	"github.com/lanyulei/reborn/backend/notify/internal/biz"
)

// NewMux 装配全部适配器为单一 ChannelSender（wire 入口）。
func NewMux() biz.ChannelSender {
	return biz.NewSenderMux(NewEmailSender(), NewWebhookSender(), NewRobotSender())
}
