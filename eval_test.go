package main

import (
	"strings"
	"testing"
)

func TestRedactPIIWithMultipleEmailFormats(t *testing.T) {
	input := "database connection failed for alice.smith@acme.com and user@example.com"
	got := redactPII(input)

	if got == input {
		t.Fatalf("expected PII to be redacted, got %q", got)
	}

	if got != "database connection failed for [REDACTED_EMAIL] and [REDACTED_EMAIL]" {
		t.Fatalf("unexpected redaction result: %q", got)
	}
}

func TestCompactHistoryKeepsRecentEvents(t *testing.T) {
	history := []string{"read log", "triaged issue", "approved ticket", "rejected ticket", "wrote report"}
	got := compactHistory(history, 3)

	if len(got) != 3 {
		t.Fatalf("expected 3 compacted entries, got %d", len(got))
	}

	if !strings.Contains(got[0], "Earlier") {
		t.Fatalf("expected compaction summary to be present, got %q", got[0])
	}

	if got[len(got)-1] != "wrote report" {
		t.Fatalf("expected newest event to be preserved, got %q", got[len(got)-1])
	}
}

func TestInferSeverityFromErrorSignals(t *testing.T) {
	severity := inferSeverity("panic: runtime error: invalid memory address")
	if severity != "HIGH" {
		t.Fatalf("expected HIGH severity for panic, got %q", severity)
	}
}
