package app

import (
	"strings"
	"testing"
)

func TestAdmissionGenesisCLIRequiresExplicitTrustInputsBeforeAPI(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{name: "no explicit global namespace", args: []string{
			"kova", "--kubeconfig", "unused", "admission-genesis", "render-genesis",
		}, want: "explicit global --namespace and --kubeconfig"},
		{name: "no expected namespace UID", args: []string{
			"kova", "--kubeconfig", "unused", "--namespace", "jobs-57", "admission-genesis", "render-genesis",
		}, want: "explicit --namespace-uid"},
		{name: "no expected Genesis UID", args: []string{
			"kova", "--kubeconfig", "unused", "--namespace", "jobs-57", "admission-genesis", "export-receipt-secret",
		}, want: "explicit --genesis-uid"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := NewCLIApp().Run(tc.args)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected refusal %q before API access, got %v", tc.want, err)
			}
		})
	}
}
