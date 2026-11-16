package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// deptJSON 是 e2e 断言用的部门响应子集。
type deptJSON struct {
	Id       string `json:"id"`
	ParentId string `json:"parentId"`
	Name     string `json:"name"`
	Code     string `json:"code"`
	Status   int32  `json:"status"`
	Leaders  []struct {
		UserId   string `json:"userId"`
		Username string `json:"username"`
	} `json:"leaders"`
	MemberCount int32 `json:"memberCount"`
}

// userDeptsJSON 是 e2e 断言用的用户所属部门子集。
type userDeptsJSON struct {
	Id    string `json:"id"`
	Depts []struct {
		Id        string `json:"id"`
		Name      string `json:"name"`
		IsPrimary bool   `json:"isPrimary"`
	} `json:"depts"`
}

// createDept 建部门辅助：parentID 为空建根部门。
func createDept(t *testing.T, ts *httptest.Server, token, name, parentID string, leaderIDs ...string) deptJSON {
	t.Helper()
	req := map[string]any{"name": name}
	if parentID != "" {
		req["parentId"] = parentID
	}
	if len(leaderIDs) > 0 {
		req["leaderIds"] = leaderIDs
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal create dept: %v", err)
	}
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept", token, string(raw))
	if code != http.StatusOK {
		t.Fatalf("create dept %s: status = %d body = %s", name, code, body)
	}
	var resp struct{ Dept deptJSON }
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("create dept %s: unmarshal %q: %v", name, body, err)
	}
	return resp.Dept
}

// getUserDepts 取用户详情中的所属部门。
func getUserDepts(t *testing.T, ts *httptest.Server, token, userID string) userDeptsJSON {
	t.Helper()
	code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user/"+userID, token, "")
	if code != http.StatusOK {
		t.Fatalf("get user %s: status = %d body = %s", userID, code, body)
	}
	var resp struct{ User userDeptsJSON }
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatalf("get user %s: unmarshal %q: %v", userID, body, err)
	}
	return resp.User
}

// 部门树：创建 / 负责人 / 更新 / 移动 / 删除守卫全链路。
func TestHTTPServer_DeptEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	alice := createUser(t, ts, bearer, "alice")
	bob := createUser(t, ts, bearer, "bob")

	// 创建根部门（多负责人，按顺序）与下级部门。
	root := createDept(t, ts, bearer, "研发中心", "", bob.Id, alice.Id)
	if root.Id == "" || root.ParentId != "" || root.Status != 1 {
		t.Fatalf("create root dept = %+v", root)
	}
	if len(root.Leaders) != 2 || root.Leaders[0].Username != "bob" || root.Leaders[1].Username != "alice" {
		t.Fatalf("root leaders = %+v, want [bob alice] in order", root.Leaders)
	}
	backend := createDept(t, ts, bearer, "后端组", root.Id)
	if backend.ParentId != root.Id {
		t.Fatalf("child dept parent = %q, want %q", backend.ParentId, root.Id)
	}
	ops := createDept(t, ts, bearer, "运维中心", "")

	// 同一上级下重名：409；不同上级下可同名。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept", bearer,
		`{"name":"后端组","parentId":"`+root.Id+`"}`)
	if code != http.StatusConflict || !strings.Contains(body, "DEPT_NAME_EXISTS") {
		t.Fatalf("duplicate name: status = %d body = %q", code, body)
	}
	createDept(t, ts, bearer, "后端组", ops.Id)

	// 上级不存在：404；负责人不存在：404。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept", bearer,
		`{"name":"孤儿","parentId":"018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"}`)
	if code != http.StatusNotFound || !strings.Contains(body, "DEPT_NOT_FOUND") {
		t.Fatalf("missing parent: status = %d body = %q", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept", bearer,
		`{"name":"无主","leaderIds":["018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"]}`)
	if code != http.StatusNotFound || !strings.Contains(body, "USER_NOT_FOUND") {
		t.Fatalf("missing leader: status = %d body = %q", code, body)
	}

	// 更新：编码 + 负责人整组替换；不带 leaders 则负责人不变。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/dept/"+root.Id, bearer,
		`{"code":"rd","leaders":{"userIds":["`+alice.Id+`"]}}`)
	if code != http.StatusOK {
		t.Fatalf("update dept: status = %d body = %s", code, body)
	}
	var updated struct{ Dept deptJSON }
	if err := json.Unmarshal([]byte(body), &updated); err != nil {
		t.Fatalf("update dept unmarshal %q: %v", body, err)
	}
	if updated.Dept.Code != "rd" || len(updated.Dept.Leaders) != 1 || updated.Dept.Leaders[0].Username != "alice" {
		t.Fatalf("update dept = %+v", updated.Dept)
	}
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/dept/"+root.Id, bearer, `{"status":0}`)
	if code != http.StatusOK {
		t.Fatalf("disable dept: status = %d body = %s", code, body)
	}
	if err := json.Unmarshal([]byte(body), &updated); err != nil {
		t.Fatalf("disable dept unmarshal %q: %v", body, err)
	}
	if updated.Dept.Status != 0 || len(updated.Dept.Leaders) != 1 {
		t.Fatalf("disable dept = %+v, leaders must be kept", updated.Dept)
	}
	// 清空负责人：显式空列表。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/dept/"+root.Id, bearer, `{"leaders":{}}`)
	if code != http.StatusOK || strings.Contains(body, `"userId"`) {
		t.Fatalf("clear leaders: status = %d body = %s", code, body)
	}
	// 空更新：校验拒绝。
	code, _ = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/dept/"+root.Id, bearer, `{}`)
	if code != http.StatusBadRequest {
		t.Fatalf("empty update: status = %d, want 400", code)
	}

	// 移动：不能移到自身下级之下；合法移动后上级变化。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+root.Id+"/move", bearer,
		`{"parentId":"`+backend.Id+`"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "DEPT_MOVE_INVALID") {
		t.Fatalf("move under descendant: status = %d body = %q", code, body)
	}
	// 目标上级下已有同名部门：409。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+backend.Id+"/move", bearer,
		`{"parentId":"`+ops.Id+`"}`)
	if code != http.StatusConflict || !strings.Contains(body, "DEPT_NAME_EXISTS") {
		t.Fatalf("move name conflict: status = %d body = %q", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+backend.Id+"/move", bearer, `{}`)
	if code != http.StatusOK {
		t.Fatalf("move to root: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/dept/"+backend.Id, bearer, "")
	if code != http.StatusOK || strings.Contains(body, `"parentId":"0`) {
		t.Fatalf("get moved dept: status = %d body = %s", code, body)
	}

	// UUID 大小写不敏感：同一负责人的大小写两种写法按一人处理；大写 ID 移动等价于小写。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/dept/"+ops.Id, bearer,
		`{"leaders":{"userIds":["`+alice.Id+`","`+strings.ToUpper(alice.Id)+`"]}}`)
	if code != http.StatusOK || strings.Count(body, `"userId"`) != 1 {
		t.Fatalf("case-variant duplicate leaders: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+strings.ToUpper(backend.Id)+"/move", bearer,
		`{"parentId":"`+strings.ToUpper(backend.Id)+`"}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "DEPT_MOVE_INVALID") {
		t.Fatalf("move under self (upper-case id): status = %d body = %q", code, body)
	}

	// 列表：平铺全量。
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/dept", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list depts: status = %d body = %s", code, body)
	}
	var list struct{ Items []deptJSON }
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		t.Fatalf("list depts unmarshal %q: %v", body, err)
	}
	if len(list.Items) != 4 {
		t.Fatalf("list depts = %d items, want 4", len(list.Items))
	}

	// 删除守卫：有下级 409；清空后可删；再查 404。
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+ops.Id, bearer, "")
	if code != http.StatusConflict || !strings.Contains(body, "DEPT_HAS_CHILDREN") {
		t.Fatalf("delete dept with children: status = %d body = %q", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+backend.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete dept: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/dept/"+backend.Id, bearer, "")
	if code != http.StatusNotFound || !strings.Contains(body, "DEPT_NOT_FOUND") {
		t.Fatalf("get deleted dept: status = %d body = %q", code, body)
	}
}

// 用户多部门：建号/编辑指定部门与主部门、部门成员增删、按部门筛选用户、删除守卫。
func TestHTTPServer_UserDeptEndToEnd(t *testing.T) {
	t.Parallel()
	ts, signer := newTestHTTPServer(t)
	bearer := testBearer(t, signer, e2eActorID)
	rd := createDept(t, ts, bearer, "研发中心", "")
	backend := createDept(t, ts, bearer, "后端组", rd.Id)
	ops := createDept(t, ts, bearer, "运维中心", "")

	// 建号同时指定多个部门与主部门。
	code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", bearer,
		`{"username":"carol","password":"plain-secret","nickname":"Carol","gender":"female",`+
			`"depts":{"deptIds":["`+backend.Id+`","`+ops.Id+`"],"primaryDeptId":"`+ops.Id+`"}}`)
	if code != http.StatusOK {
		t.Fatalf("create user with depts: status = %d body = %s", code, body)
	}
	var created struct{ User userDeptsJSON }
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("create user unmarshal %q: %v", body, err)
	}
	carol := created.User
	if len(carol.Depts) != 2 || carol.Depts[0].Id != ops.Id || !carol.Depts[0].IsPrimary || carol.Depts[1].IsPrimary {
		t.Fatalf("carol depts = %+v, want primary 运维中心 first", carol.Depts)
	}

	// 主部门不在列表内：校验拒绝；部门不存在：404 且不留下半成品账号。
	code, _ = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", bearer,
		`{"username":"dave","password":"plain-secret","nickname":"Dave","gender":"male",`+
			`"depts":{"deptIds":["`+backend.Id+`"],"primaryDeptId":"`+ops.Id+`"}}`)
	if code != http.StatusBadRequest {
		t.Fatalf("primary not in list: status = %d, want 400", code)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user", bearer,
		`{"username":"dave","password":"plain-secret","nickname":"Dave","gender":"male",`+
			`"depts":{"deptIds":["018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"]}}`)
	if code != http.StatusNotFound || !strings.Contains(body, "DEPT_NOT_FOUND") {
		t.Fatalf("create user with missing dept: status = %d body = %q", code, body)
	}
	dave := createUser(t, ts, bearer, "dave") // 上一步未落库，用户名仍可用

	// 编辑：整组替换（未指定主部门取第一个）；只改部门也算有效更新。
	code, body = doRequestWithToken(t, http.MethodPut, ts.URL+"/api/v1/system/user/"+carol.Id, bearer,
		`{"depts":{"deptIds":["`+backend.Id+`"]}}`)
	if code != http.StatusOK {
		t.Fatalf("update user depts: status = %d body = %s", code, body)
	}
	if got := getUserDepts(t, ts, bearer, carol.Id); len(got.Depts) != 1 || got.Depts[0].Id != backend.Id || !got.Depts[0].IsPrimary {
		t.Fatalf("carol depts after update = %+v", got.Depts)
	}

	// 部门成员：加入（首个部门自动成为主部门，重复加入幂等）。
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+rd.Id+"/member", bearer,
		`{"userIds":["`+dave.Id+`","`+carol.Id+`"]}`)
	if code != http.StatusOK {
		t.Fatalf("add dept members: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+rd.Id+"/member", bearer,
		`{"userIds":["`+dave.Id+`"]}`)
	if code != http.StatusOK {
		t.Fatalf("add dept members again: status = %d body = %s", code, body)
	}
	if got := getUserDepts(t, ts, bearer, dave.Id); len(got.Depts) != 1 || !got.Depts[0].IsPrimary {
		t.Fatalf("dave depts = %+v, want sole primary", got.Depts)
	}
	if got := getUserDepts(t, ts, bearer, carol.Id); len(got.Depts) != 2 || got.Depts[0].Id != backend.Id || got.Depts[1].IsPrimary {
		t.Fatalf("carol depts = %+v, want primary 后端组 kept", got.Depts)
	}
	code, body = doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/dept/"+rd.Id+"/member", bearer,
		`{"userIds":["018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d"]}`)
	if code != http.StatusNotFound || !strings.Contains(body, "USER_NOT_FOUND") {
		t.Fatalf("add missing user: status = %d body = %q", code, body)
	}

	// 按部门筛选用户：直属 vs 含下级。
	listIn := func(query string) listUsersJSON {
		t.Helper()
		code, body := doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/user?page=1&page_size=10&"+query, bearer, "")
		if code != http.StatusOK {
			t.Fatalf("list users %s: status = %d body = %s", query, code, body)
		}
		var list listUsersJSON
		if err := json.Unmarshal([]byte(body), &list); err != nil {
			t.Fatalf("list users unmarshal %q: %v", body, err)
		}
		return list
	}
	if got := listIn("dept_id=" + backend.Id); got.Total != "1" || got.Items[0].Username != "carol" {
		t.Fatalf("users in 后端组 = %+v", got)
	}
	if got := listIn("dept_id=" + rd.Id + "&include_sub_depts=true"); got.Total != "2" {
		t.Fatalf("users in 研发中心 subtree = %+v, want 2", got)
	}
	if got := listIn("dept_id=" + ops.Id); got.Total != "0" {
		t.Fatalf("users in 运维中心 = %+v, want 0", got)
	}

	// 部门成员列表（最小披露）：不含邮箱 / 手机号；直属 vs 含下级；关键字只匹配登录名/昵称。
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/system/dept/"+rd.Id+"/member?page=1&page_size=10&include_sub_depts=true", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list dept members: status = %d body = %s", code, body)
	}
	if strings.Contains(body, "email") || strings.Contains(body, "phone") {
		t.Fatalf("dept members must not disclose email/phone: %s", body)
	}
	var members struct {
		Items []userDeptsJSON `json:"items"`
		Total string          `json:"total"`
	}
	if err := json.Unmarshal([]byte(body), &members); err != nil {
		t.Fatalf("list dept members unmarshal %q: %v", body, err)
	}
	if members.Total != "2" || len(members.Items) != 2 {
		t.Fatalf("dept members = %+v, want 2", members)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/system/dept/"+rd.Id+"/member?page=1&page_size=10&include_sub_depts=true&keyword=example.com", bearer, "")
	if code != http.StatusOK || !strings.Contains(body, `"items":[]`) {
		t.Fatalf("member keyword must not match email: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet,
		ts.URL+"/api/v1/system/dept/018f3e1a-7b1c-7cc1-9d2a-3f5e7a9b1c3d/member?page=1&page_size=10", bearer, "")
	if code != http.StatusNotFound || !strings.Contains(body, "DEPT_NOT_FOUND") {
		t.Fatalf("members of missing dept: status = %d body = %q", code, body)
	}

	// 可选用户：仅可用用户的 ID 与显示名。
	if code, body := doRequestWithToken(t, http.MethodPost, ts.URL+"/api/v1/system/user/"+dave.Id+"/status", bearer, `{"status":2}`); code != http.StatusOK {
		t.Fatalf("disable dave: status = %d body = %s", code, body)
	}
	code, body = doRequestWithToken(t, http.MethodGet, ts.URL+"/api/v1/system/dept-user-candidate", bearer, "")
	if code != http.StatusOK {
		t.Fatalf("list user candidates: status = %d body = %s", code, body)
	}
	if !strings.Contains(body, `"carol"`) || strings.Contains(body, `"dave"`) || strings.Contains(body, "email") {
		t.Fatalf("user candidates = %s, want carol without dave / email", body)
	}

	// 有成员的部门不可删除。
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+backend.Id, bearer, "")
	if code != http.StatusConflict || !strings.Contains(body, "DEPT_HAS_MEMBERS") {
		t.Fatalf("delete dept with members: status = %d body = %q", code, body)
	}

	// 移出主部门：其余部门顶替为主部门；重复移出幂等。
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+backend.Id+"/member/"+carol.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("remove dept member: status = %d body = %s", code, body)
	}
	if got := getUserDepts(t, ts, bearer, carol.Id); len(got.Depts) != 1 || got.Depts[0].Id != rd.Id || !got.Depts[0].IsPrimary {
		t.Fatalf("carol depts after removal = %+v, want 研发中心 promoted", got.Depts)
	}
	code, _ = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+backend.Id+"/member/"+carol.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("remove dept member again: status = %d, want 200", code)
	}
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+backend.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete emptied dept: status = %d body = %s", code, body)
	}

	// 删除用户：部门归属随之清除，部门可删。
	for _, id := range []string{carol.Id, dave.Id} {
		if code, body := doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/user/"+id, bearer, ""); code != http.StatusOK {
			t.Fatalf("delete user: status = %d body = %s", code, body)
		}
	}
	code, body = doRequestWithToken(t, http.MethodDelete, ts.URL+"/api/v1/system/dept/"+rd.Id, bearer, "")
	if code != http.StatusOK {
		t.Fatalf("delete dept after users removed: status = %d body = %s", code, body)
	}
}
