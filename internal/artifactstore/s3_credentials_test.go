package artifactstore

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileCredentialProviderReadsRotatedCredentials(t *testing.T) {
	dir := t.TempDir()
	writeCredential(t, dir, "KOVA_S3_ACCESS_KEY", "first-access")
	writeCredential(t, dir, "KOVA_S3_SECRET_KEY", "first-secret")

	creds, err := s3Credentials(Config{
		S3CredentialProvider: S3CredentialProviderFile,
		S3CredentialDir:      dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := creds.Get()
	if err != nil {
		t.Fatal(err)
	}
	if first.AccessKeyID != "first-access" || first.SecretAccessKey != "first-secret" {
		t.Fatalf("unexpected initial credentials: %#v", first)
	}

	writeCredential(t, dir, "KOVA_S3_ACCESS_KEY", "second-access")
	writeCredential(t, dir, "KOVA_S3_SECRET_KEY", "second-secret")
	second, err := creds.Get()
	if err != nil {
		t.Fatal(err)
	}
	if second.AccessKeyID != "second-access" || second.SecretAccessKey != "second-secret" {
		t.Fatalf("unexpected rotated credentials: %#v", second)
	}
}

func TestS3CredentialProvidersValidateConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantError bool
	}{
		{name: "static", cfg: Config{S3AccessKey: "access", S3SecretKey: "secret"}},
		{name: "missing static key", cfg: Config{}, wantError: true},
		{name: "anonymous", cfg: Config{S3CredentialProvider: S3CredentialProviderAnonymous}},
		{name: "unknown", cfg: Config{S3CredentialProvider: "metadata"}, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := s3Credentials(test.cfg)
			if (err != nil) != test.wantError {
				t.Fatalf("s3Credentials() error = %v, wantError = %v", err, test.wantError)
			}
		})
	}
}

func writeCredential(t *testing.T, dir, name, value string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}
