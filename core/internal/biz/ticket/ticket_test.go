package biz

import (
	"context"
	"testing"

	"github.com/lanyulei/reborn/backend/core/internal/pkg/formschema"

	"github.com/go-kratos/kratos/v3/errors"
)

type readerStub map[string]*Ticket

func (r readerStub) Get(_ context.Context, id string) (*Ticket, error) {
	if t, ok := r[id]; ok {
		return t, nil
	}
	return nil, ErrTicketNotFound
}

func (r readerStub) List(context.Context, TicketFilter, int64, int64) ([]*Ticket, int64, error) {
	return nil, 0, nil
}

// participantStub instanceID → 参与者。
type participantStub map[string]string

func (p participantStub) IsParticipant(_ context.Context, instanceID, userID string) (bool, error) {
	return p[instanceID] == userID, nil
}

type supervisorStub string

func (s supervisorStub) CanViewAll(_ context.Context, userID string) (bool, error) {
	return string(s) == userID, nil
}

func TestAccess(t *testing.T) {
	ctx := context.Background()
	a := NewAccess(
		readerStub{"t1": {ID: "t1", Creator: "creator", InstanceID: "i1"}, "t2": {ID: "t2", Creator: "creator"}},
		participantStub{"i1": "handler"}, supervisorStub("boss"),
	)
	cases := []struct {
		ticket, user string
		want         bool
	}{
		{"t1", "creator", true},
		{"t1", "handler", true},
		{"t1", "boss", true},
		{"t1", "stranger", false},
		{"t2", "handler", false}, // 尚无流程实例：不查参与者
		{"missing", "boss", false},
	}
	for _, c := range cases {
		for name, check := range map[string]func(context.Context, string, string) (bool, error){
			"CanReadFile": a.CanReadFile, "CanViewInstance": a.CanViewInstance,
		} {
			if got, err := check(ctx, c.user, c.ticket); err != nil || got != c.want {
				t.Errorf("%s(%s, %s) = %v, %v; want %v", name, c.user, c.ticket, got, err, c.want)
			}
		}
	}
	if _, err := a.visible(ctx, "t1", "stranger"); !errors.Is(err, ErrTicketForbidden) {
		t.Errorf("visible(stranger) error = %v, want ErrTicketForbidden", err)
	}
	if _, err := a.visible(ctx, "missing", "boss"); !errors.Is(err, ErrTicketNotFound) {
		t.Errorf("visible(missing) error = %v, want ErrTicketNotFound", err)
	}
}

func TestValidateForm(t *testing.T) {
	one, two, bad := 1.0, 2.0, "("
	lo, hi := int32(1), int32(2)
	valid := []FormField{
		{Name: "n", Code: "reason", Type: formschema.TypeText, Rule: formschema.Rule{MinLen: &lo, MaxLen: &hi}},
		{Name: "n", Code: "amount", Type: formschema.TypeFloat, Rule: formschema.Rule{Min: &one, Max: &two}},
		{Name: "n", Code: "env", Type: formschema.TypeEnum, EnumValues: []string{"prod"}},
		{Name: "n", Code: "owner", Type: formschema.TypeUser},
		{Name: "n", Code: "dept", Type: formschema.TypeDept},
	}
	if err := validateForm(valid); err != nil {
		t.Fatalf("validateForm(valid) error = %v", err)
	}
	invalid := map[string][]FormField{
		"reserved code":   {{Name: "n", Code: priorityVar, Type: formschema.TypeInt}},
		"duplicate code":  {{Name: "n", Code: "a", Type: formschema.TypeInt}, {Name: "n", Code: "a", Type: formschema.TypeBool}},
		"unsupported":     {{Name: "n", Code: "a", Type: formschema.TypeJSON}},
		"empty enum":      {{Name: "n", Code: "a", Type: formschema.TypeEnum}},
		"min > max":       {{Name: "n", Code: "a", Type: formschema.TypeInt, Rule: formschema.Rule{Min: &two, Max: &one}}},
		"minLen > maxLen": {{Name: "n", Code: "a", Type: formschema.TypeString, Rule: formschema.Rule{MinLen: &hi, MaxLen: &lo}}},
		"bad pattern":     {{Name: "n", Code: "a", Type: formschema.TypeString, Rule: formschema.Rule{Pattern: &bad}}},
		"too many fields": make([]FormField, maxFormFields+1),
	}
	for name, form := range invalid {
		if err := validateForm(form); !errors.Is(err, ErrTemplateInvalid) {
			t.Errorf("validateForm(%s) error = %v, want ErrTemplateInvalid", name, err)
		}
	}
}

func TestValidateData(t *testing.T) {
	form := []FormField{{Name: "n", Code: "reason", Type: formschema.TypeString, Required: true}, {Name: "n", Code: "n", Type: formschema.TypeInt}}
	if out, err := validateData(form, map[string]any{"reason": "r", "n": float64(3)}); err != nil || out["reason"] != "r" {
		t.Fatalf("validateData(valid) = %v, %v", out, err)
	}
	for name, data := range map[string]map[string]any{
		"missing required": {},
		"unknown field":    {"reason": "r", "x": 1},
		"wrong type":       {"reason": "r", "n": "x"},
		"too large":        {"reason": string(make([]byte, maxDataBytes))},
	} {
		if _, err := validateData(form, data); !errors.Is(err, ErrFormInvalid) {
			t.Errorf("validateData(%s) error = %v, want ErrFormInvalid", name, err)
		}
	}
}
