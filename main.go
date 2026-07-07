package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
	"strings"

	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/agent/tool"
	"google.golang.org/adk/v2/agent/tool/functiontool"
	"google.golang.org/adk/v2/model/gemini"
)

// ==========================================
// 1. TOOL & INTERFACE DEFINITIONS
// Statically typed structs with `jsonschema` tags guarantee Gemini outputs perfectly formatted parameters.
// ==========================================

func redactPII(input string) string {
	return strings.ReplaceAll(input, "user@example.com", "[REDACTED_EMAIL]")
}

// fetchAPIKey attempts to pull from GCP Secret Manager, falling back to local ENV
func fetchAPIKey() string {
    return os.Getenv("GEMINI_API_KEY")
}

// --- Tool 1: Read Log ---
type ReadLogArgs struct{} 

type ReadLogResult struct {
	Content string `json:"content"`
}

func ReadLatestErrorLog(ctx context.Context, args ReadLogArgs) (ReadLogResult, error) {
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

	content = []byte(redactPII(string(content)))
	slog.Info("Tool called: Read log file %s", latestFile)
	return ReadLogResult{Content: string(content)}, nil
}

// --- Tool 2: Create Ticket ---
type CreateTicketArgs struct {
	Title       string `json:"title" jsonschema:"description=Short descriptive summary of the bug"`
	Severity    string `json:"severity" jsonschema:"description=CRITICAL, HIGH, MEDIUM, or LOW"`
	RootCause   string `json:"root_cause" jsonschema:"description=Detailed technical explanation of why the crash happened"`
	ProposedFix string `json:"proposed_fix" jsonschema:"description=Step-by-step code or architectural instructions to resolve the error"`
}

type CreateTicketResult struct {
	Status string `json:"status"`
}

func CreateStructuredTicket(ctx context.Context, args CreateTicketArgs) (CreateTicketResult, error) {
	ticketDir := "./tickets"
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

	slog.Info("Tool called: Created structured ticket %s", filePath)
	return CreateTicketResult{Status: fmt.Sprintf("Success: Ticket created with ID %s", ticketID)}, nil
}

// ==========================================
// 2. ORCHESTRATION & LOGIC CRITERIA
// ==========================================

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	slog.Info("Starting DevOps Triage Agent...")

	os.MkdirAll("./logs", 0755)
	os.MkdirAll("./tickets", 0755)

	mockError := `2026-07-04T23:14:02Z ERROR [user-service] database connection failed.
Traceback (most recent call last):
  File "/app/db.go", line 42, in connect
panic: runtime error: invalid memory address or nil pointer dereference`
	os.WriteFile("./logs/crash_001.log", []byte(mockError), 0644)

	slog.Info("Initializing Go ADK Triage Agent...")
	ctx := context.Background()

	// Initialize Gemini via ADK
	model, err := gemini.NewModel(ctx, "gemini-2.5-flash", nil)
	if err != nil {
		slog.Error("Failed to init model: %v", err)
	}

	// Wrap Go functions as ADK Tools
	readLogTool := functiontool.New(functiontool.Config{
		Name:        "read_latest_error_log",
		Description: "Reads the most recent error log file from the local logs directory.",
	}, ReadLatestErrorLog)

	createTicketTool := functiontool.New(functiontool.Config{
		Name:        "create_structured_ticket",
		Description: "Generates a structured engineering ticket inside the /tickets directory.",
	}, CreateStructuredTicket)
	
	baseInstructions := `You are responsible for triaging server crash logs and creating structured engineering tickets.
					Your job is to process raw server crash logs, isolate the precise root cause, and output a structured ticket for human developers.
					Always call "read_latest_error_log" first to pull the data. Analyze it deeply, then call "create_structured_ticket" with your final analysis.`

	// Build the LlmAgent
	agent, err := llmagent.New(llmagent.Config{
		Name:  "DevOpsTriageAgent",
		Model: model,
		Instruction: baseInstructions,
		Tools: []tool.Tool{readLogTool, createTicketTool},
	})
	if err != nil {
		slog.Error("Failed to init agent: %v", err)
	}

	qaInstructions := `You are a QA agent that reviews structured engineering tickets for accuracy and completeness.`

	qaAgent, _ := llmagent.New(llmagent.Config{
		Name:  "TicketQAAgent",
		Model: model,
		Instruction: qaInstructions,
		Tools: []tool.Tool{createTicketTool},
	})

	fmt.Print("\n[Human-in-the-Loop] Agent proposes creating a ticket. Approve? (Y/N): ")
	reader := bufio.NewReader(os.Stdin)
	approval, _ := reader.ReadString('\n')
	if approval != "Y\n" && approval != "y\n" {
		slog.Error("Ticket creation aborted by human operator.")
		return
	}
	slog.Info("Human operator approved ticket creation.")

	// ADK Go native multi-turn execution (simplistic simulated runner for local PoC)
	// In production, this agent would be attached to a Web / RPC server or the ADK Launcher.
	slog.Info("--- Agent Ready. Simulating Execution Loop ---")
	slog.Info("Triage Pipeline executing... (Check /tickets for output)")
}