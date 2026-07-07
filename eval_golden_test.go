package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGoldenWorkflowProducesTicket(t *testing.T) {
	tmpDir := t.TempDir()
	originalLogsDir := defaultLogsDirValue
	originalTicketsDir := defaultTicketsDirValue
	originalStatePath := defaultStatePathValue
	defer func() {
		defaultLogsDirValue = originalLogsDir
		defaultTicketsDirValue = originalTicketsDir
		defaultStatePathValue = originalStatePath
	}()

	defaultLogsDirValue = filepath.Join(tmpDir, "logs")
	defaultTicketsDirValue = filepath.Join(tmpDir, "tickets")
	defaultStatePathValue = filepath.Join(tmpDir, "state", "workflow_state.json")

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

	if err := runWorkflowWithInput("n\n", false); err != nil {
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
