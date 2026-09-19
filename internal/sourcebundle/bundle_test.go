package sourcebundle

import (
	"strings"
	"testing"
)

func TestValidateRequiresImmutableVerifiableSource(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, uri := range []string{
		"oci://registry.example.com/team/source@sha256:" + strings.Repeat("b", 64),
		"https://sources.example.com/source.zip",
	} {
		if err := Validate(uri, digest); err != nil {
			t.Fatalf("Validate(%q) = %v", uri, err)
		}
	}
	for _, uri := range []string{
		"oci://registry.example.com/team/source:latest",
		"http://sources.example.com/source.zip",
		"https://user:password@sources.example.com/source.zip",
		"https://sources.example.com/source.zip?token=secret",
	} {
		if err := Validate(uri, digest); err == nil {
			t.Fatalf("expected %q to be rejected", uri)
		}
	}
	if err := Validate("https://sources.example.com/source.zip", "sha256:short"); err == nil {
		t.Fatal("expected invalid content digest rejection")
	}
}
