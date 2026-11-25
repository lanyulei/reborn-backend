package biz

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
)

func newAssignCommanderUsecase(inc *Incident, outbox *fakeOutbox, webhooks fakeWebhookRepo) *IncidentUsecase {
	d := &Deps{
		Txm: fakeTxm{}, Writer: &mapWriter{byID: map[string]*Incident{inc.ID: inc}}, Members: fakeMembers{}, Admins: fakeAdmins{},
		Events: &recordingEvents{}, Links: &stubLinks{}, Dir: fakeDirectory{active: []string{"u1", "u2"}}, Notify: &fakeNotify{},
		Webhooks: webhooks, Outbox: outbox, Logger: testLogger(),
	}
	return NewIncidentUsecase(d, NewSettingsUsecase(fakeSettingRepo{}))
}

// 指派 / 转交指挥要发出 Webhook 的 commander 事件：它一直在可订阅列表里，却从未发出。只投给订阅了它
// （或未限定事件）的订阅。
func TestAssignCommanderEmitsCommanderWebhookEvent(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusOpen, CommanderID: "u1", Version: 1}
	outbox := &fakeOutbox{}
	uc := newAssignCommanderUsecase(inc, outbox, fakeWebhookRepo{enabled: []*Webhook{
		{ID: "w-commander", Events: []string{"commander"}},
		{ID: "w-status", Events: []string{"status"}},
		{ID: "w-all"},
	}})

	if _, err := uc.AssignCommander(context.Background(), "u1", "i1", "u2", "接管"); err != nil {
		t.Fatalf("AssignCommander() error = %v", err)
	}
	targets := make([]string, 0, len(outbox.items))
	for _, it := range outbox.items {
		if it.EventType != "commander" {
			t.Fatalf("outbox event type = %q, want commander", it.EventType)
		}
		var payload struct {
			Event  string `json:"event"`
			Detail struct {
				CommanderID string `json:"commander_id"`
				From        string `json:"from"`
			} `json:"detail"`
		}
		if err := json.Unmarshal(it.Payload, &payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		if payload.Event != "commander" || payload.Detail.CommanderID != "u2" || payload.Detail.From != "u1" {
			t.Fatalf("commander payload = %+v, want commander_id u2 from u1", payload)
		}
		targets = append(targets, it.WebhookID)
	}
	slices.Sort(targets)
	if !slices.Equal(targets, []string{"w-all", "w-commander"}) {
		t.Fatalf("delivered to %v, want the commander subscriber and the all-events subscriber only", targets)
	}
	if inc.CommanderID != "u2" {
		t.Fatalf("commander = %q, want u2", inc.CommanderID)
	}
}

// 指派给现任指挥：空操作，不发事件。
func TestAssignCommanderSameUserEmitsNothing(t *testing.T) {
	t.Parallel()
	inc := &Incident{ID: "i1", Number: "INC-1", Status: StatusOpen, CommanderID: "u1", Version: 1}
	outbox := &fakeOutbox{}
	uc := newAssignCommanderUsecase(inc, outbox, fakeWebhookRepo{enabled: []*Webhook{{ID: "w-commander", Events: []string{"commander"}}}})

	if _, err := uc.AssignCommander(context.Background(), "u1", "i1", "u1", ""); err != nil {
		t.Fatalf("AssignCommander() error = %v", err)
	}
	if len(outbox.items) != 0 {
		t.Fatalf("outbox items = %d, want none when the commander does not change", len(outbox.items))
	}
}
