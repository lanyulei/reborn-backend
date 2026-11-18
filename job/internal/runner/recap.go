package runner

import (
	"bytes"
	"regexp"
	"strconv"

	"github.com/lanyulei/reborn/backend/kit/jobcontract"
)

// maxRecapLine 参与解析的单行长度上限：PLAY RECAP 的行很短，超长的行直接跳过。
const maxRecapLine = 4096

var (
	recapHeader = []byte("PLAY RECAP")
	// recapLine 一台主机的统计行：`10.0.0.1 : ok=3 changed=1 unreachable=0 failed=0 skipped=2 rescued=0 ignored=0`。
	// 主机名之后必有空白再跟冒号，所以 IPv6 地址里的冒号不会被误当作分隔符。
	recapLine  = regexp.MustCompile(`^(\S+)\s+:\s+(ok=\d+(?:\s+[a-z_]+=\d+)*)\s*$`)
	recapField = regexp.MustCompile(`([a-z_]+)=(\d+)`)
)

// recapParser 从 ansible-playbook 的输出里流式提取 PLAY RECAP 的各主机统计。输出里可能有多段
// RECAP（多个 Playbook 串联），同一主机以最后一次为准。只接受任务内的主机地址。
// 非并发安全（由读输出的那一个 goroutine 驱动）。
type recapParser struct {
	known   map[string]struct{}
	results map[string]jobcontract.HostResult
	order   []string
	line    []byte
	skip    bool // 当前行已超长，丢弃到行尾
	inRecap bool
}

func newRecapParser(hosts []jobcontract.TaskHost) *recapParser {
	p := &recapParser{known: make(map[string]struct{}, len(hosts)), results: map[string]jobcontract.HostResult{}}
	for _, h := range hosts {
		p.known[h.Address] = struct{}{}
	}
	return p
}

// Write 送入一段原始输出（未掩码：主机地址不应被凭据掩码改写）。
func (p *recapParser) Write(data []byte) {
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			p.appendLine(data)
			return
		}
		p.appendLine(data[:i])
		if !p.skip {
			p.feed(bytes.TrimRight(p.line, "\r"))
		}
		p.line, p.skip = p.line[:0], false
		data = data[i+1:]
	}
}

func (p *recapParser) appendLine(part []byte) {
	if p.skip {
		return
	}
	if len(p.line)+len(part) > maxRecapLine {
		p.skip, p.inRecap = true, false
		return
	}
	p.line = append(p.line, part...)
}

// Close 处理没有换行结尾的最后一行，并复位行状态：一次 ansible-playbook 的输出到此为止。滚动执行时
// 同一个解析器接着读下一批的输出，已得到的主机结果保留（各批的主机互不相同）。
func (p *recapParser) Close() {
	if !p.skip && len(p.line) > 0 {
		p.feed(bytes.TrimRight(p.line, "\r"))
	}
	p.line, p.skip, p.inRecap = p.line[:0], false, false
}

func (p *recapParser) feed(line []byte) {
	if bytes.HasPrefix(line, recapHeader) {
		p.inRecap = true
		return
	}
	if !p.inRecap {
		return
	}
	m := recapLine.FindSubmatch(line)
	if m == nil {
		// RECAP 以空行或别的内容结束。
		p.inRecap = false
		return
	}
	addr := string(m[1])
	if _, ok := p.known[addr]; !ok {
		return
	}
	r := jobcontract.HostResult{Address: addr}
	for _, f := range recapField.FindAllSubmatch(m[2], -1) {
		n, err := strconv.ParseInt(string(f[2]), 10, 32)
		if err != nil {
			continue
		}
		v := int32(n)
		switch string(f[1]) {
		case "ok":
			r.OK = v
		case "changed":
			r.Changed = v
		case "unreachable":
			r.Unreachable = v
		case "failed":
			r.Failed = v
		case "skipped":
			r.Skipped = v
		}
	}
	if _, seen := p.results[addr]; !seen {
		p.order = append(p.order, addr)
	}
	p.results[addr] = r
}

// Results 各主机的统计（按首次出现的顺序）。
func (p *recapParser) Results() []jobcontract.HostResult {
	out := make([]jobcontract.HostResult, 0, len(p.order))
	for _, addr := range p.order {
		out = append(out, p.results[addr])
	}
	return out
}
