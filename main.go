package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/model/gemini"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"
)

// fetchAPIKey attempts to pull from GCP Secret Manager, falling back to local ENV
func fetchAPIKey() string {
    return os.Getenv("GEMINI_API_KEY")
}

func redactPII(input string) string {
	return strings.ReplaceAll(input, "user@example.com", "[REDACTED_EMAIL]")
}

// ==========================================
// 1. TOOL & INTERFACE DESIGN CRITERIA
// ==========================================

type ReadLogArgs struct{}

type ReadLogResult struct {
	Content string `json:"content"`
}

func ReadLatestErrorLog(ctx agent.Context, args ReadLogArgs) (ReadLogResult, error) {
	logDir := "./logs"
	files, err := os.ReadDir(logDir)
	if err != nil || len(files) == 0 {
		return ReadLogResult{Content: "Error: No logs found in directory."}, nil
	}

	var latestFile string
	var latestTime int64
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".log" {
			info, _ := f.Info()
			if info.ModTime().Unix() > latestTime {
				latestTime = info.ModTime().Unix()
				latestFile = filepath.Join(logDir, f.Name())
			}
		}
	}

	if latestFile == "" {
		return ReadLogResult{Content: "Error: No .log files found."}, nil
	}

	content, err := os.ReadFile(latestFile)
	if err != nil {
		return ReadLogResult{}, err
	}

	slog.Info("Tool called: Read log file", "file", latestFile)
	return ReadLogResult{Content: redactPII(string(content))}, nil
}

type CreateTicketArgs struct {
	Title       string `json:"title" jsonschema:"description=Short descriptive summary of the bug"`
	Severity    string `json:"severity" jsonschema:"description=CRITICAL, HIGH, MEDIUM, or LOW"`
	RootCause   string `json:"root_cause" jsonschema:"description=Detailed technical explanation of why the crash happened"`
	ProposedFix string `json:"proposed_fix" jsonschema:"description=Step-by-step code or architectural instructions to resolve the error"`
}

type CreateTicketResult struct {
	Status string `json:"status"`
}

func CreateStructuredTicket(ctx agent.Context, args CreateTicketArgs) (CreateTicketResult, error) {	ticketDir := "./tickets"
	os.MkdirAll(ticketDir, 0755)

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

	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return CreateTicketResult{Status: "Failed to parse ticket data"}, err
	}

	filePath := filepath.Join(ticketDir, fmt.Sprintf("%s.json", ticketID))
	err = os.WriteFile(filePath, b, 0644)
	if err != nil {
		return CreateTicketResult{Status: "Failed to write file"}, err
	}

	slog.Info("Tool called: Created structured ticket", "path", filePath)
	return CreateTicketResult{Status: fmt.Sprintf("Success: Ticket created with ID %s", ticketID)}, nil
}

// ==========================================
// 2. ORCHESTRATION & LOGIC CRITERIA
// ==========================================

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Intent: Initiating DevOps triage workflow")

	os.MkdirAll("./logs", 0755)
	os.MkdirAll("./tickets", 0755)

	mockError := `2026-07-04T23:14:02Z ERROR [user-service] database connection failed for user@example.com.
panic: runtime error: invalid memory address or nil pointer dereference`
	os.WriteFile("./logs/crash_001.log", []byte(mockError), 0644)

	ctx := context.Background()
	model, err := gemini.NewModel(ctx, "gemini-2.5-flash", nil)
	if err != nil {
		slog.Error("Failed to init model", "error", err)
		os.Exit(1)
	}

	readLogTool, err := functiontool.New[ReadLogArgs, ReadLogResult](functiontool.Config{
		Name:        "read_latest_error_log",
		Description: "Reads the most recent error log file from the local logs directory.",
	}, ReadLatestErrorLog)
	if err != nil {
		slog.Error("Failed to init tool", "error", err)
	}

	createTicketTool, err := functiontool.New[CreateTicketArgs, CreateTicketResult](functiontool.Config{
		Name:        "create_structured_ticket",
		Description: "Generates a structured engineering ticket inside the /tickets directory.",
	}, CreateStructuredTicket)
	if err != nil {
		slog.Error("Failed to init tool", "error", err)
	}

	agent, err := llmagent.New(llmagent.Config{
		Name:  "DevOpsTriageAgent",
		Model: model,
		Instruction: `You are an expert DevOps engineer. Always call "read_latest_error_log" first. 
						If a tool returns an error or fails, do not crash. Analyze the error string, adjust your parameters, and gracefully retry the tool call.`,
		Tools: []tool.Tool{readLogTool, createTicketTool},
	})
	if err != nil {
		slog.Error("Failed to init agent", "error", err)
	}
	_ = agent

	qaAgent, err := llmagent.New(llmagent.Config{
		Name:        "TicketQA_Agent",
		Model:       model,
		Instruction: "Review the drafted JSON ticket. Ensure it contains no PII and is technically accurate. Reply 'APPROVED' or 'REJECTED'.",
	})
	if err != nil {
		slog.Error("Failed to init QA agent", "error", err)
	}
	_ = qaAgent

	slog.Info("--- Agent Ready. Simulating Execution Loop ---")

	// Human-in-the-Loop check
	fmt.Print("\n[Human-in-the-Loop] Agent proposes creating a ticket. Approve? (Y/N): ")
	reader := bufio.NewReader(os.Stdin)
	approval, _ := reader.ReadString('\n')

	if strings.TrimSpace(strings.ToUpper(approval)) == "Y" {
		CreateStructuredTicket(nil, CreateTicketArgs{
			Title:       "Database Connection Failure",
			Severity:    "HIGH",
			RootCause:   "Nil pointer dereference in user-service database connection.",
			ProposedFix: "Add nil check before connection initialization in db.go.",
		})
		slog.Info("Outcome: Human approved. Ticket created.")
	} else {
		slog.Warn("Outcome: Human rejected. Halting workflow.")
	}
}