package scheduler

import "testing"

func TestParseAddrsNormalizesAndSortsIPInputs(t *testing.T) {
	addrs, err := ParseAddrs("10.0.0.2:9094, tcp://10.0.0.1:9094")
	if err != nil {
		t.Fatalf("ParseAddrs returned error: %v", err)
	}

	if len(addrs) != 2 {
		t.Fatalf("len(addrs) = %d, want 2", len(addrs))
	}

	want := []struct {
		original string
		addr     string
		nodeIP   string
	}{
		{original: "tcp://10.0.0.1:9094", addr: "tcp://10.0.0.1:9094", nodeIP: "10.0.0.1"},
		{original: "10.0.0.2:9094", addr: "tcp://10.0.0.2:9094", nodeIP: "10.0.0.2"},
	}
	for i := range want {
		if addrs[i].Original != want[i].original || addrs[i].Addr != want[i].addr || addrs[i].NodeIP != want[i].nodeIP {
			t.Fatalf("addr[%d] = %#v, want original=%q addr=%q nodeIP=%q", i, addrs[i], want[i].original, want[i].addr, want[i].nodeIP)
		}
	}
}

func TestParseAddrsRejectsEmptyInput(t *testing.T) {
	if _, err := ParseAddrs(" , "); err == nil {
		t.Fatal("expected empty input error")
	}
}

func TestNodeIPFromAddrOnlyReturnsLiteralIPs(t *testing.T) {
	if got := NodeIPFromAddr("tcp://192.0.2.10:9094"); got != "192.0.2.10" {
		t.Fatalf("NodeIPFromAddr literal IP = %q, want 192.0.2.10", got)
	}
	if got := NodeIPFromAddr("tcp://buildkitd.kova.svc:9094"); got != "" {
		t.Fatalf("NodeIPFromAddr hostname = %q, want empty string", got)
	}
}
