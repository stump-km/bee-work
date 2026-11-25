package main

import "testing"

func TestJobForSessionScopesJobVisibility(t *testing.T) {
	g := &Gateway{
		jobs: map[string]*Job{
			"job-owned": {ID: "job-owned", Status: "done", Reply: "private answer", sessionID: "session-a"},
		},
	}

	if got := g.JobForSession("job-owned", "session-a"); got == nil {
		t.Fatal("owning session should be able to read its job")
	} else if got.Reply != "private answer" {
		t.Fatalf("unexpected reply: %q", got.Reply)
	}

	if got := g.JobForSession("job-owned", "session-b"); got != nil {
		t.Fatal("a different session must not read this job")
	}
	if got := g.JobForSession("missing", "session-a"); got != nil {
		t.Fatal("unknown job must return nil")
	}
	if got := g.JobForSession("job-owned", ""); got != nil {
		t.Fatal("empty session must not read a job")
	}
}

func TestJobForSessionReturnsCopy(t *testing.T) {
	original := &Job{ID: "job-owned", Status: "done", Reply: "original", sessionID: "session-a"}
	g := &Gateway{jobs: map[string]*Job{"job-owned": original}}

	got := g.JobForSession("job-owned", "session-a")
	if got == nil {
		t.Fatal("expected owning session to read job")
	}
	got.Reply = "mutated"
	if original.Reply != "original" {
		t.Fatal("caller mutation must not change the stored job")
	}
}
