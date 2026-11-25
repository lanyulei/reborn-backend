package data

import (
	"strings"
	"testing"
)

// 两类作业各用一条语句领取：类别谓词是可与部分索引匹配的常量比较，不再有第三个参数；除类别谓词外
// 两条语句逐字相同（领取条件、排序、租约与尝试次数的写法不因类别而异）。
func TestClaimDueSQLKindPredicates(t *testing.T) {
	t.Parallel()
	const servicePred, internalPred = "AND kind = 'service'", "AND kind <> 'service'"
	if strings.Count(claimServiceSQL, servicePred) != 1 || strings.Contains(claimServiceSQL, "<>") {
		t.Errorf("service claim SQL = %s", claimServiceSQL)
	}
	if strings.Count(claimInternalSQL, internalPred) != 1 {
		t.Errorf("internal claim SQL = %s", claimInternalSQL)
	}
	for _, q := range []string{claimServiceSQL, claimInternalSQL} {
		if strings.Contains(q, "$3") || strings.Contains(q, "%!") || !strings.Contains(q, "FOR UPDATE SKIP LOCKED") {
			t.Errorf("claim SQL malformed: %s", q)
		}
	}
	if strings.Replace(claimServiceSQL, servicePred, "", 1) != strings.Replace(claimInternalSQL, internalPred, "", 1) {
		t.Error("the two claim statements differ beyond the kind predicate")
	}
}
