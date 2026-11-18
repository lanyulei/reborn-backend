// Package monitorpipeline 是告警事件流水线里与 I/O 无关的纯函数，评估引擎（backend/monitor）与
// core 的模拟触发共用同一份实现（monitor P2/P3 技术设计 §3.5、§3.7）：两端各写一份必然漂移，
// 模拟触发的结论就不可信。数据的形状（jsonb 结构、枚举、上限）在 kit/monitorcontract，本包只管行为。
//
// 一个样本（一条查询结果序列）依次经过：
//
//	序列标签去掉 __name__、合并规则附加标签            （调用方）
//	→ Pipeline.Relabel        标签改写                 （之后算指纹：指纹 = 序列标签 + 规则标签 + 改写结果）
//	→ Pipeline.Resolve        关联 CMDB 资源、写入 ci_ 富化标签、条件丢弃、资源范围过滤
//	→ 级别抑制                                          （调用方：同指纹只留最高级别）
//	→ InhibitSnapshot.Match / Upstream  规则间抑制 / 拓扑抑制
//	→ 屏蔽规则；Resolution.Maintenance 为真时按「只屏蔽通知」处理  （调用方）
//	→ 落库、通知；活跃事件每轮经 Escalate 计算到期的升级通知
//
// 本包没有包级可变状态，不做任何 I/O，不 import 驱动与传输层；全部类型创建后只读、可并发使用。
// 输入的 map / slice 除注明「就地改写」者外均只读。
package monitorpipeline
