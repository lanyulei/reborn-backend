package promclient

import "encoding/binary"

// Snappy 块格式（https://github.com/google/snappy/blob/main/format_description.txt）的最小编码器，
// 只供 Remote Write 使用。自己实现而不引入依赖：kit 被全部服务模块引用，为一个百行以内、格式冻结的
// 编码器给每个模块的依赖图加一个压缩库不划算。输出是标准的 Snappy 块（非流式帧），任何解码器都能解。
// 匹配策略是单候选的贪心哈希：压缩率不如参考实现，对标签高度重复的时序数据已足够（见基准）。
const (
	snappyTableBits = 14
	snappyTableSize = 1 << snappyTableBits
	// snappyMaxOffset 回溯距离上限：只用 2 字节偏移的拷贝元素。
	snappyMaxOffset = 1<<16 - 1
	// snappyMinMatch 值得编码成拷贝的最短匹配（哈希按 4 字节计算）。
	snappyMinMatch = 4
	// snappyMaxCopy 单个拷贝元素的最大长度。
	snappyMaxCopy = 64
)

// snappyEncode 把 src 编码为一个 Snappy 块，追加到 dst 并返回。len(src) 须小于 4GiB（调用方的请求体上限远小于此）。
func snappyEncode(dst, src []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(src)))
	if len(src) < snappyMinMatch {
		return snappyLiteral(dst, src)
	}
	// table 四字节序列的哈希 → 最近一次出现的位置 + 1（0 = 未出现）。
	var table [snappyTableSize]uint32
	lit, i := 0, 0
	for end := len(src) - snappyMinMatch; i <= end; {
		cur := binary.LittleEndian.Uint32(src[i:])
		h := (cur * 0x1e35a7bd) >> (32 - snappyTableBits)
		cand := int(table[h]) - 1
		table[h] = uint32(i + 1) // #nosec G115 -- 调用方保证 len(src) 小于 4GiB
		if cand < 0 || i-cand > snappyMaxOffset || binary.LittleEndian.Uint32(src[cand:]) != cur {
			i++
			continue
		}
		n := snappyMinMatch
		for i+n < len(src) && src[cand+n] == src[i+n] {
			n++
		}
		dst = snappyLiteral(dst, src[lit:i])
		dst = snappyCopy(dst, i-cand, n)
		i += n
		lit = i
	}
	return snappyLiteral(dst, src[lit:])
}

// snappyLiteral 追加一个字面量元素；空字面量不产生输出。
func snappyLiteral(dst, lit []byte) []byte {
	if len(lit) == 0 {
		return dst
	}
	n := uint32(len(lit) - 1) // #nosec G115 -- 调用方保证 len(src) 小于 4GiB
	// le 长度减一的小端字节：长度不小于 60 时，标签字节之后跟 1-4 个字节的长度。
	var le [4]byte
	binary.LittleEndian.PutUint32(le[:], n)
	switch {
	case n < 60:
		dst = append(dst, le[0]<<2)
	case n < 1<<8:
		dst = append(dst, 60<<2, le[0])
	case n < 1<<16:
		dst = append(dst, 61<<2, le[0], le[1])
	case n < 1<<24:
		dst = append(dst, 62<<2, le[0], le[1], le[2])
	default:
		dst = append(dst, 63<<2, le[0], le[1], le[2], le[3])
	}
	return append(dst, lit...)
}

// snappyCopy 追加拷贝元素（2 字节偏移，标签低两位 = 10），长度超过 64 时拆成多个。
func snappyCopy(dst []byte, offset, length int) []byte {
	for length > 0 {
		n := min(length, snappyMaxCopy)
		dst = append(dst, byte(n-1)<<2|2, byte(offset), byte(offset>>8)) // #nosec G115 -- n 为 1-64，offset 不超过 65535
		length -= n
	}
	return dst
}
