package biz

import (
	"errors"
	"testing"
)

func TestDefaultSettingsValid(t *testing.T) {
	t.Parallel()
	s := DefaultSettings()
	if err := validateSettings(s); err != nil {
		t.Fatalf("default settings invalid: %v", err)
	}
	if s.Sev(SeverityS1).RespondM != 5 || !s.Sev(SeverityS4).AllowAutoApprove || s.Sev(SeverityS1).AllowAutoApprove {
		t.Fatalf("unexpected default severity settings: %+v", s.Severities)
	}
	if !s.ValidRootCause("") {
		t.Error("empty root cause must be accepted")
	}
	if !s.ValidRootCause(s.RootCauses[0].Code) || s.ValidRootCause("nope") {
		t.Error("ValidRootCause mismatch")
	}
}

func TestValidateSettings(t *testing.T) {
	t.Parallel()
	bad := []struct {
		name string
		mut  func(*Settings)
	}{
		{"unknown severity", func(s *Settings) { s.Severities[9] = SeveritySetting{} }},
		{"respond out of range", func(s *Settings) { s.Severities[SeverityS1] = SeveritySetting{RespondM: 2000} }},
		{"review days out of range", func(s *Settings) { s.Severities[SeverityS1] = SeveritySetting{ReviewDays: 61} }},
		{"no root causes", func(s *Settings) { s.RootCauses = nil }},
		{"duplicated root cause", func(s *Settings) { s.RootCauses = append(s.RootCauses, s.RootCauses[0]) }},
		{"blank root cause", func(s *Settings) { s.RootCauses = []RootCause{{Code: " ", Name: "x"}} }},
		{"unknown personal event", func(s *Settings) { s.PersonalEvents = []string{"nope"} }},
		{"storm threshold", func(s *Settings) { s.StormThreshold = 0 }},
		{"verify window", func(s *Settings) { s.VerifyWindowMinutes = 121 }},
		{"agent findings", func(s *Settings) { s.AgentMaxFindings = 1 }},
	}
	for _, c := range bad {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s := DefaultSettings()
			c.mut(s)
			if err := validateSettings(s); !errors.Is(err, ErrSettingInvalid) {
				t.Fatalf("validateSettings() error = %v, want ErrSettingInvalid", err)
			}
		})
	}
}

func TestSettingsMerge(t *testing.T) {
	t.Parallel()
	s := DefaultSettings()
	s.merge(&Settings{StormThreshold: 9, Severities: map[int32]SeveritySetting{SeverityS2: {RespondM: 1}}, Version: 3})
	if s.StormThreshold != 9 || s.Version != 3 {
		t.Fatalf("merge did not take saved scalars: %+v", s)
	}
	if s.Sev(SeverityS2).RespondM != 1 || s.Sev(SeverityS1).RespondM != 5 {
		t.Fatalf("merge of severities wrong: %+v", s.Severities)
	}
	if len(s.RootCauses) == 0 {
		t.Fatal("merge dropped default root causes")
	}
}
