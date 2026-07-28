package runner

import "testing"

func TestParseListArgs(t *testing.T) {
	wide, err := parseListArgs([]string{"-o", "wide"})
	if err != nil {
		t.Fatal(err)
	}
	if !wide {
		t.Fatal("expected wide output")
	}
	if _, err := parseListArgs([]string{"-o", "json"}); err == nil {
		t.Fatal("expected unsupported output error")
	}
}

func TestParseLogsArgs(t *testing.T) {
	tail, err := parseLogsArgs([]string{"--tail=25"})
	if err != nil {
		t.Fatal(err)
	}
	if tail != 25 {
		t.Fatalf("tail = %d", tail)
	}
	if _, err := parseLogsArgs([]string{"--tail", "-1"}); err == nil {
		t.Fatal("expected negative tail error")
	}
}

func TestParseReplicaCount(t *testing.T) {
	replicas, err := parseReplicaCount("2147483647")
	if err != nil {
		t.Fatal(err)
	}
	if replicas != 2147483647 {
		t.Fatalf("replicas = %d", replicas)
	}
	for _, value := range []string{"-1", "2147483648", "not-a-number"} {
		if _, err := parseReplicaCount(value); err == nil {
			t.Fatalf("expected %q to fail", value)
		}
	}
}
