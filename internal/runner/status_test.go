package runner

import "testing"

func TestWaitDecision(t *testing.T) {
	tests := []struct {
		name        string
		status      string
		wantDone    bool
		wantSuccess bool
		wantErr     bool
	}{
		{name: "running", status: "running", wantDone: false, wantSuccess: false},
		{name: "cancelling", status: "cancelling", wantDone: false, wantSuccess: false},
		{name: "completed", status: "completed", wantDone: true, wantSuccess: true},
		{name: "failed", status: "failed", wantDone: true, wantSuccess: false},
		{name: "error", status: "error", wantDone: true, wantSuccess: false},
		{name: "cancelled", status: "cancelled", wantDone: true, wantSuccess: false},
		{name: "idle", status: "idle", wantDone: true, wantSuccess: false, wantErr: true},
		{name: "empty", status: "", wantDone: true, wantSuccess: false, wantErr: true},
		{name: "unknown", status: "paused", wantDone: true, wantSuccess: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done, success, err := WaitDecision(tt.status)
			if done != tt.wantDone || success != tt.wantSuccess {
				t.Fatalf("WaitDecision(%q) = (%v, %v), want (%v, %v)", tt.status, done, success, tt.wantDone, tt.wantSuccess)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseBuildState(t *testing.T) {
	state, err := ParseBuildState([]byte(`{"status":"completed"}`))
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "completed" {
		t.Fatalf("status = %q", state.Status)
	}
}
