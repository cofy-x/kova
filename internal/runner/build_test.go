package runner

import (
	"strings"
	"testing"
)

func TestStdinIsTerminalForNonFileReader(t *testing.T) {
	if stdinIsTerminal(strings.NewReader("zip")) {
		t.Fatal("strings.Reader must not be treated as terminal")
	}
}

func TestNonTerminalStdinKeepsPipeReaders(t *testing.T) {
	reader := strings.NewReader("input")
	if nonTerminalStdin(reader) != reader {
		t.Fatal("non-terminal stdin should be preserved")
	}
}
