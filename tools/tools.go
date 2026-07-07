package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"
)

var DefaultLogsDir = "./logs"
var DefaultTicketsDir = "./tickets"

// ReadLogArgs is the empty argument payload for the read_latest_error_log tool.
type ReadLogArgs struct{}

// ReadLogResult is the result returned by the read_latest_error_log tool.
type ReadLogResult struct {
	Content string `json:"content"`
}

// CreateTicketArgs defines the input fields required to create a structured ticket.
type CreateTicketArgs struct {
	Title       string `json:"title" jsonschema:"description=Short descriptive summary of the bug"`
	Severity    string `json:"severity" jsonschema:"description=CRITICAL, HIGH, MEDIUM, or LOW"`
	RootCause   string `json:"root_cause" jsonschema:"description=Detailed technical explanation of why the crash happened"`
	ProposedFix string `json:"proposed_fix" jsonschema:"description=Step-by-step remediation instructions"`
}

// CreateTicketResult contains the status and ticket identifier produced by CreateStructuredTicket.
type CreateTicketResult struct {
	Status   string `json:"status"`
	TicketID string `json:"ticket_id,omitempty"`
}

// ReadLatestErrorLog reads the newest local .log file and returns its redacted contents.
func ReadLatestErrorLog(ctx context.Context, args ReadLogArgs) (ReadLogResult, error) {
	logDir := DefaultLogsDir
	files, err := os.ReadDir(logDir)
	if err != nil || len(files) == 0 {
		return ReadLogResult{Content: "Error: No logs found in directory; add a .log file and retry."}, nil
	}

	var latestFile string
	var latestTime int64
	for _, f := range files {
		if f.IsDir() || filepath.Ext(f.Name()) != ".log" {
			continue
		}
		info, statErr := f.Info()
		if statErr != nil {
			continue
		}
		if info.ModTime().Unix() > latestTime {
			latestTime = info.ModTime().Unix()
			latestFile = filepath.Join(logDir, f.Name())
		}
	}
	if latestFile == "" {
		return ReadLogResult{Content: "Error: No .log files found; create an example log and retry."}, nil
	}

	content, err := os.ReadFile(latestFile)
	if err != nil {
		return ReadLogResult{}, fmt.Errorf("read_latest_error_log: unable to read %s: %w", latestFile, err)
	}

	slog.Info("tool.call", "tool", "read_latest_error_log", "file", latestFile)
	return ReadLogResult{Content: redactPII(string(content))}, nil
}

// CreateStructuredTicket writes a structured engineering ticket to the local tickets directory.
func CreateStructuredTicket(ctx context.Context, args CreateTicketArgs) (CreateTicketResult, error) {
	ticketDir := DefaultTicketsDir
	if err := os.MkdirAll(ticketDir, 0o755); err != nil {
		return CreateTicketResult{Status: "Failed to create ticket directory"}, fmt.Errorf("create ticket directory %q: %w; verify the tickets directory exists and is writable", ticketDir, err)
	}

	ticketID := fmt.Sprintf("BUG-%d", time.Now().Unix())
	data := map[string]string{
		"ticket_id":    ticketID,
		"created_at":   time.Now().UTC().Format(time.RFC3339),
		"title":        args.Title,
		"severity":     args.Severity,
		"root_cause":   args.RootCause,
		"proposed_fix": args.ProposedFix,
		"status":       "OPEN",
	}

	payload, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return CreateTicketResult{Status: "Failed to parse ticket data"}, fmt.Errorf("marshal ticket payload: %w; verify ticket fields are valid", err)
	}

	filePath := filepath.Join(ticketDir, fmt.Sprintf("%s.json", ticketID))
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		return CreateTicketResult{Status: "Failed to write ticket file"}, fmt.Errorf("write ticket file %q: %w; verify ticket directory permissions and disk space", filePath, err)
	}

	slog.Info("tool.call", "tool", "create_structured_ticket", "path", filePath, "ticket_id", ticketID)
	return CreateTicketResult{Status: fmt.Sprintf("Success: Ticket created with ID %s", ticketID), TicketID: ticketID}, nil
}

// buildReadLatestErrorLogTool constructs the ADK tool that reads the latest local error log.
func BuildReadLatestErrorLogTool() (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "read_latest_error_log",
		Description: "Read the latest redacted error log from the local logs directory.",
	}, func(ctx agent.Context, args ReadLogArgs) (ReadLogResult, error) {
		// When invoked as an ADK tool, run the programmatic helper with a background context.
		return ReadLatestErrorLog(context.Background(), args)
	})
}

// BuildCreateStructuredTicketTool constructs the ADK tool used to create a structured ticket.
func BuildCreateStructuredTicketTool(createdTicketID *string) (tool.Tool, error) {
	return functiontool.New(functiontool.Config{
		Name:        "create_structured_ticket",
		Description: "Create a structured ticket with a title, severity, root cause, and proposed fix.",
	}, func(ctx agent.Context, args CreateTicketArgs) (CreateTicketResult, error) {
		// CreateStructuredTicket is a programmatic helper that accepts a standard context.
		// ADK's agent.Context is not a standard context.Context, so use Background here.
		result, err := CreateStructuredTicket(context.Background(), args)
		if err == nil && result.TicketID != "" && createdTicketID != nil {
			*createdTicketID = result.TicketID
		}
		return result, err
	})
}

// BuildProposeTicketTool creates a tool that allows the triage agent to propose
// a structured ticket without persisting it. The proposed ticket is written to
// the provided pointer so downstream agents can inspect and validate it.
func BuildProposeTicketTool(proposed *map[string]string) (tool.Tool, error) {
	type Args = CreateTicketArgs
	type Result struct{ Status string }
	return functiontool.New(functiontool.Config{
		Name:        "propose_ticket",
		Description: "Propose a structured ticket draft (does not persist).",
	}, func(ctx agent.Context, args Args) (Result, error) {
		if proposed == nil {
			return Result{Status: "failed"}, fmt.Errorf("internal: propose slot missing")
		}
		*proposed = map[string]string{
			"title":        args.Title,
			"severity":     args.Severity,
			"root_cause":   args.RootCause,
			"proposed_fix": args.ProposedFix,
		}
		return Result{Status: "proposed"}, nil
	})
}

// BuildValidateTicketTool constructs a validation tool that runs deterministic
// checks (re-using existing review logic) and returns a short decision string.
func BuildValidateTicketTool(decision *string) (tool.Tool, error) {
	type Args struct{}
	type Result struct{ Decision string }
	return functiontool.New(functiontool.Config{
		Name:        "validate_ticket",
		Description: "Validate the proposed ticket for completeness and sensitivity.",
	}, func(ctx agent.Context, args Args) (Result, error) {
		if decision == nil {
			return Result{Decision: "REJECTED"}, fmt.Errorf("internal: validation sink missing")
		}
		return Result{Decision: *decision}, nil
	})
}

// BuildHitlApprovalTool returns a lightweight human-in-the-loop tool which
// checks an env var (`HITL_AUTO_APPROVE`) to auto-approve in CI, otherwise
// responds with a "PENDING" status so the runner can fallback to manual prompt.
func BuildHitlApprovalTool(approved *bool) (tool.Tool, error) {
	type Args struct{}
	type Result struct{ Approved bool }
	return functiontool.New(functiontool.Config{
		Name:        "hitl_approval",
		Description: "Request human-in-the-loop approval (auto-approve via HITL_AUTO_APPROVE=1).",
	}, func(ctx agent.Context, args Args) (Result, error) {
		if approved == nil {
			return Result{Approved: false}, fmt.Errorf("internal: approval sink missing")
		}
		if strings.TrimSpace(os.Getenv("HITL_AUTO_APPROVE")) == "1" {
			*approved = true
			return Result{Approved: true}, nil
		}
		// Not auto-approved; leave approved false and return pending.
		return Result{Approved: false}, nil
	})
}

// redactPII redacts email addresses and simple secret patterns from a string.
func redactPII(input string) string {
	sanitized := input
	emailRe := regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)
	secretRe := regexp.MustCompile(`(?i)\b(?:password|token|api[_-]?key)[\s:=]+[A-Za-z0-9._-]{3,}\b`)
	sanitized = emailRe.ReplaceAllString(sanitized, "[REDACTED_EMAIL]")
	sanitized = secretRe.ReplaceAllString(sanitized, "[REDACTED_SECRET]")
	return strings.ReplaceAll(sanitized, "user@example.com", "[REDACTED_EMAIL]")
}

// Getenv is a thin wrapper around os.Getenv to allow callers to use a tools-provided accessor.
func Getenv(k string) string { return os.Getenv(k) }

// ReviewDeterministic applies the simple deterministic checks used by previous logic.
func ReviewDeterministic(p map[string]string) bool {
	if p == nil {
		return false
	}
	if strings.TrimSpace(p["title"]) == "" || strings.TrimSpace(p["root_cause"]) == "" || strings.TrimSpace(p["proposed_fix"]) == "" {
		return false
	}
	if strings.Contains(p["root_cause"], "user@example.com") || strings.Contains(p["proposed_fix"], "user@example.com") {
		return false
	}
	return true
}
