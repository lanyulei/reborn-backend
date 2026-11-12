package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// 导入：试运行只预览不落库；正式导入按名称 upsert，坏行独立失败；导出 CSV 含动态列。
func TestCmdb_ImportExport(t *testing.T) {
	t.Parallel()
	ts, svc := newTestHTTPServerWithServices(t)
	bearer := testBearer(t, svc.signer, e2eActorID)
	cmdbSetup(t, ts, bearer)
	createCI(t, ts, bearer, `{"modelCode":"host","name":"h1","attrs":{"ip":"10.0.0.1","cores":4}}`)

	rows := `[{"name":"h1","owner":"ops","attrs":{"cores":8}},{"name":"h2","attrs":{"ip":"10.0.0.2"}},{"name":"h3","attrs":{"ip":"bad"}},{"name":"h1b","attrs":{"ip":"10.0.0.1"}}]`
	type result struct {
		DryRun  bool `json:"dryRun"`
		Created int  `json:"created"`
		Updated int  `json:"updated"`
		Skipped int  `json:"skipped"`
		Failed  int  `json:"failed"`
		Results []struct {
			Row     int    `json:"row"`
			Action  string `json:"action"`
			Message string `json:"message"`
			CiID    string `json:"ciId"`
		} `json:"results"`
	}
	parse := func(body string) result {
		var r result
		if err := json.Unmarshal([]byte(body), &r); err != nil {
			t.Fatalf("parse import result %s: %v", body, err)
		}
		return r
	}

	// 试运行：h1 更新、h2 新建、h3 非法 IP 报错、h1b 唯一键与 h1 冲突（dry-run 校验不到唯一键，判定为 create）。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci:import", bearer,
		`{"modelCode":"host","dryRun":true,"rows":`+rows+`}`)
	if code != http.StatusOK {
		t.Fatalf("dry run: status = %d body = %s", code, body)
	}
	dry := parse(body)
	if !dry.DryRun || dry.Updated != 1 || dry.Created != 2 || dry.Failed != 1 ||
		dry.Results[0].Action != "update" || dry.Results[1].Action != "create" || dry.Results[2].Action != "error" ||
		!strings.Contains(dry.Results[2].Message, "ip") {
		t.Fatalf("dry run result wrong: %+v", dry)
	}
	// 试运行不落库：h2 不存在。
	if _, listBody := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci?page=1&pageSize=10&modelCode=host", bearer, ""); !strings.Contains(listBody, `"total":"1"`) {
		t.Fatalf("dry run must not persist: %s", listBody)
	}

	// 正式导入：h1 更新（版本 2）、h2 创建、h3 失败、h1b 唯一键冲突失败。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci:import", bearer,
		`{"modelCode":"host","dryRun":false,"rows":`+rows+`}`)
	if code != http.StatusOK {
		t.Fatalf("import: status = %d body = %s", code, body)
	}
	real := parse(body)
	if real.Updated != 1 || real.Created != 1 || real.Failed != 2 || real.Results[3].Action != "error" ||
		!strings.Contains(real.Results[3].Message, "CMDB_CI_DUPLICATE") && !strings.Contains(real.Results[3].Message, "already exists") {
		t.Fatalf("import result wrong: %+v", real)
	}
	h1, _ := getCI(t, ts, bearer, real.Results[0].CiID)
	if h1.Version != "2" || h1.Attrs["cores"] != float64(8) {
		t.Fatalf("h1 after import = %+v", h1)
	}
	// 重复导入同一批：h1/h2 无变化 → skip。
	_, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci:import", bearer,
		`{"modelCode":"host","dryRun":false,"rows":[{"name":"h1","owner":"ops","attrs":{"cores":8}},{"name":"h2","attrs":{"ip":"10.0.0.2"}}]}`)
	again := parse(body)
	if again.Skipped != 2 || again.Updated != 0 || again.Created != 0 {
		t.Fatalf("re-import should skip: %+v", again)
	}
	// 超过 1000 行 / 空行列表由 protovalidate 拒绝。
	if code, _ := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/cmdb/ci:import", bearer, `{"modelCode":"host","rows":[]}`); code != http.StatusBadRequest {
		t.Fatalf("empty rows: status = %d", code)
	}

	// 导出：CSV 表头含固定列与字段列，数据行含两台主机。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:export?modelCode=host", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("export: status = %d body = %s", code, body)
	}
	lines := strings.Split(strings.TrimSpace(strings.TrimPrefix(body, "\uFEFF")), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "name,status,env,owner,tags,externalId,id,version,updateTime,cores,ip") {
		t.Fatalf("export csv wrong: %q", body)
	}
	if !strings.Contains(body, "h1,active,,ops,,,") || !strings.Contains(body, ",,10.0.0.2\n") {
		t.Fatalf("export rows wrong: %q", body)
	}
	// 未知类型 404。
	if code, _ := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/cmdb/ci:export?modelCode=nope", bearer, ""); code != http.StatusNotFound {
		t.Fatalf("export unknown model: status = %d", code)
	}
}
