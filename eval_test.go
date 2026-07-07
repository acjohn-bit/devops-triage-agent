package main

import (
	"context"
	"os"
	"path/filepath"
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

func TestStateStorePersistsHistory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "workflow.sqlite")
	store, err := openStateStore(dbPath)
	if err != nil {
		t.Fatalf("open state store: %v", err)
	}
	defer store.close()

	state := &workflowState{TraceID: "trace-123", History: []string{"started"}}
	if err := store.saveState(state); err != nil {
		t.Fatalf("save state: %v", err)
	}

	loaded, err := store.loadState()
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	if loaded.TraceID != "trace-123" {
		t.Fatalf("expected persisted trace id, got %q", loaded.TraceID)
	}
	if len(loaded.History) != 1 || loaded.History[0] != "started" {
		t.Fatalf("expected persisted history entry, got %v", loaded.History)
	}
}

func TestPromptForApprovalSupportsNonInteractiveInput(t *testing.T) {
	if !promptForApproval("y", true) {
		t.Fatal("expected non-interactive approval to succeed")
	}
}

func TestSelectAgentModelHonorsRoleSpecificOverride(t *testing.T) {
	original := os.Getenv("GEMINI_QA_MODEL")
	t.Cleanup(func() {
		if original == "" {
			_ = os.Unsetenv("GEMINI_QA_MODEL")
		} else {
			_ = os.Setenv("GEMINI_QA_MODEL", original)
		}
	})

	if err := os.Setenv("GEMINI_QA_MODEL", "gemini-2.5-pro"); err != nil {
		t.Fatalf("set env: %v", err)
	}

	if got := selectAgentModel("qa"); got != "gemini-2.5-pro" {
		t.Fatalf("expected QA model override to be used, got %q", got)
	}
}

func TestAgenticValidationFallback(t *testing.T) {
	// When no model is provided, agentic evaluator should fall back to deterministic review.
	proposed := &ProposedTicket{Title: "DB failure", Severity: "HIGH", RootCause: "database down", ProposedFix: "restart DB"}
	decision, rationale, err := evaluateProposedTicketWithAgent(context.Background(), nil, proposed)
	if err != nil {
		t.Fatalf("evaluator error: %v", err)
	}
	if decision != "APPROVED" && decision != "REJECTED" {
		t.Fatalf("unexpected decision: %q (rationale: %s)", decision, rationale)
	}
}

func TestIntegrationFlowAutoApprove(t *testing.T) {
	tmpDir := t.TempDir()
	originalLogsDir := defaultLogsDirValue
	originalTicketsDir := defaultTicketsDirValue
	originalStateDB := defaultStateDBValue
	defer func() {
		defaultLogsDirValue = originalLogsDir
		defaultTicketsDirValue = originalTicketsDir
		defaultStateDBValue = originalStateDB
		_ = os.Unsetenv("HITL_AUTO_APPROVE")
	}()

	defaultLogsDirValue = filepath.Join(tmpDir, "logs")
	defaultTicketsDirValue = filepath.Join(tmpDir, "tickets")
	defaultStateDBValue = filepath.Join(tmpDir, "state", "workflow_state.sqlite")

	if err := os.MkdirAll(defaultLogsDirValue, 0o755); err != nil {
		t.Fatalf("create logs dir: %v", err)
	}
	if err := os.MkdirAll(defaultTicketsDirValue, 0o755); err != nil {
		t.Fatalf("create tickets dir: %v", err)
	}

	logContent := "2026-07-07T12:00:00Z ERROR database connection failed for user@example.com"
	if err := os.WriteFile(filepath.Join(defaultLogsDirValue, "crash_001.log"), []byte(logContent), 0o644); err != nil {
		t.Fatalf("write sample log: %v", err)
	}

	if err := os.Setenv("HITL_AUTO_APPROVE", "1"); err != nil {
		t.Fatalf("set env: %v", err)
	}

	if err := runWorkflowWithInput("", true); err != nil {
		t.Fatalf("run workflow: %v", err)
	}

	files, err := os.ReadDir(defaultTicketsDirValue)
	if err != nil {
		t.Fatalf("read tickets dir: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("expected a ticket to be written")
	}
}
