package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	_ "modernc.org/sqlite"
	"cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/model"
	"google.golang.org/adk/v2/model/gemini"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/adk/v2/tool/functiontool"
	"google.golang.org/genai"
)

const (
	defaultLogsDir   = "./logs"
	defaultTicketsDir = "./tickets"
	defaultStatePath = "./state/workflow_state.json"
	defaultStateDB   = "./state/workflow_state.sqlite"
)

var (
	defaultLogsDirValue   = defaultLogsDir
	defaultTicketsDirValue = defaultTicketsDir
	defaultStatePathValue = defaultStatePath
)

var emailPattern = regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)
var secretPattern = regexp.MustCompile(`(?i)\b(?:password|token|api[_-]?key)[\s:=]+[A-Za-z0-9._-]{3,}`)

type workflowState struct {
	TraceID      string   `json:"trace_id"`
	History      []string `json:"history"`
	LastTicketID string   `json:"last_ticket_id,omitempty"`
}

type stateStore struct {
	db *sql.DB
}

func openStateStore(path string) (*stateStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS workflow_state (trace_id TEXT PRIMARY KEY, history TEXT, last_ticket_id TEXT)`); err != nil {
		db.Close()
		return nil, err
	}
	return &stateStore{db: db}, nil
}

func (s *stateStore) close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *stateStore) saveState(state *workflowState) error {
	if s == nil || s.db == nil || state == nil {
		return nil
	}
	historyJSON, err := json.Marshal(state.History)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT INTO workflow_state(trace_id, history, last_ticket_id) VALUES(?, ?, ?) ON CONFLICT(trace_id) DO UPDATE SET history=excluded.history, last_ticket_id=excluded.last_ticket_id`, state.TraceID, string(historyJSON), state.LastTicketID)
	return err
}

func (s *stateStore) loadState() (*workflowState, error) {
	if s == nil || s.db == nil {
		return newWorkflowState(), nil
	}
	var traceID, historyJSON, lastTicketID string
	err := s.db.QueryRow(`SELECT trace_id, history, last_ticket_id FROM workflow_state ORDER BY trace_id DESC LIMIT 1`).Scan(&traceID, &historyJSON, &lastTicketID)
	if err != nil {
		if err == sql.ErrNoRows {
			return newWorkflowState(), nil
		}
		return nil, err
	}
	var history []string
	if err := json.Unmarshal([]byte(historyJSON), &history); err != nil {
		return nil, err
	}
	return &workflowState{TraceID: traceID, History: history, LastTicketID: lastTicketID}, nil
}

type ticketDraft struct {
	Title       string
	Severity    string
	RootCause   string
	ProposedFix string
}

type ReadLogArgs struct{}

type ReadLogResult struct {
	Content string `json:"content"`
}

type CreateTicketArgs struct {
	Title       string `json:"title" jsonschema:"description=Short descriptive summary of the bug"`
	Severity    string `json:"severity" jsonschema:"description=CRITICAL, HIGH, MEDIUM, or LOW"`
	RootCause   string `json:"root_cause" jsonschema:"description=Detailed technical explanation of why the crash happened"`
	ProposedFix string `json:"proposed_fix" jsonschema:"description=Step-by-step remediation instructions"`
}

type CreateTicketResult struct {
	Status   string `json:"status"`
	TicketID string `json:"ticket_id,omitempty"`
}

func newWorkflowState() *workflowState {
	return &workflowState{
		TraceID: newTraceID(),
		History: []string{},
	}
}

func loadWorkflowState(path string) (*workflowState, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newWorkflowState(), nil
		}
		return nil, err
	}

	var state workflowState
	if err := json.Unmarshal(content, &state); err != nil {
		return nil, err
	}
	if state.TraceID == "" {
		state.TraceID = newTraceID()
	}
	if state.History == nil {
		state.History = []string{}
	}
	return &state, nil
}

func (s *workflowState) save(path string) error {
	if s == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, payload, 0o644)
}

func (s *workflowState) appendHistory(event string) {
	if s == nil || strings.TrimSpace(event) == "" {
		return
	}
	s.History = append(s.History, event)
	if len(s.History) > 8 {
		s.History = compactHistory(s.History, 8)
	}
}

func compactHistory(history []string, limit int) []string {
	if len(history) <= limit {
		return append([]string(nil), history...)
	}
	older := len(history) - limit + 1
	compacted := make([]string, 0, limit)
	compacted = append(compacted, fmt.Sprintf("Earlier %d events omitted", older))
	compacted = append(compacted, history[len(history)-limit+1:]...)
	return compacted
}

func newTraceID() string {
	return fmt.Sprintf("trace-%d", time.Now().UnixNano())
}

func logActionIntent(action, description string, attrs ...any) {
	slog.Info("workflow.intent", append([]any{"action", action, "description", description}, attrs...)...)
}

func logActionOutcome(action, outcome string, attrs ...any) {
	slog.Info("workflow.outcome", append([]any{"action", action, "outcome", outcome}, attrs...)...)
}

func fetchAPIKey() string {
	for _, key := range []string{"GEMINI_API_KEY", "GOOGLE_API_KEY"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func fetchSecretFromGCP(projectID, secretName string) (string, error) {
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", err
	}
	defer client.Close()
	resp, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", projectID, secretName),
	})
	if err != nil {
		return "", err
	}
	return string(resp.GetPayload().GetData()), nil
}

func initTracing() (*sdktrace.TracerProvider, error) {
	exporter, err := stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("devops-triage-agent"),
	))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	return provider, nil
}

func redactPII(input string) string {
	sanitized := emailPattern.ReplaceAllString(input, "[REDACTED_EMAIL]")
	sanitized = secretPattern.ReplaceAllString(sanitized, "[REDACTED_SECRET]")
	return strings.ReplaceAll(sanitized, "user@example.com", "[REDACTED_EMAIL]")
}

func inferSeverity(logContent string) string {
	content := strings.ToLower(logContent)
	switch {
	case strings.Contains(content, "out of memory"), strings.Contains(content, "fatal"), strings.Contains(content, "segmentation fault"):
		return "CRITICAL"
	case strings.Contains(content, "panic"), strings.Contains(content, "nil pointer"), strings.Contains(content, "connection failed"):
		return "HIGH"
	case strings.Contains(content, "timeout"), strings.Contains(content, "warning"):
		return "MEDIUM"
	default:
		return "LOW"
	}
}

// ReadLatestErrorLog reads the newest local .log file and returns its redacted contents.
func ReadLatestErrorLog(ctx agent.Context, args ReadLogArgs) (ReadLogResult, error) {
	logDir := defaultLogsDirValue
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
func CreateStructuredTicket(ctx agent.Context, args CreateTicketArgs) (CreateTicketResult, error) {
	ticketDir := defaultTicketsDirValue
	if err := os.MkdirAll(ticketDir, 0o755); err != nil {
		return CreateTicketResult{Status: "Failed to create ticket directory"}, err
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
		return CreateTicketResult{Status: "Failed to parse ticket data"}, err
	}

	filePath := filepath.Join(ticketDir, fmt.Sprintf("%s.json", ticketID))
	if err := os.WriteFile(filePath, payload, 0o644); err != nil {
		return CreateTicketResult{Status: "Failed to write ticket file"}, err
	}

	slog.Info("tool.call", "tool", "create_structured_ticket", "path", filePath, "ticket_id", ticketID)
	return CreateTicketResult{Status: fmt.Sprintf("Success: Ticket created with ID %s", ticketID), TicketID: ticketID}, nil
}

func runWorkflow(ctx context.Context) error {
	return runWorkflowWithInput("", false)
}

func runWorkflowWithInput(input string, nonInteractive bool) error {
	return runWorkflowWithInputContext(context.Background(), input, nonInteractive)
}

func runWorkflowWithInputContext(ctx context.Context, input string, nonInteractive bool) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	tracerProvider, err := initTracing()
	if err != nil {
		slog.Warn("tracing.init_failed", "error", err)
	} else {
		defer tracerProvider.Shutdown(ctx)
	}

	state, err := loadWorkflowState(defaultStatePathValue)
	if err != nil {
		return fmt.Errorf("load workflow state: %w", err)
	}

	tracer := otel.Tracer("devops-triage-agent")
	ctx, workflowSpan := tracer.Start(ctx, "workflow.start")
	workflowSpan.SetAttributes(attribute.String("workflow.trace_id", state.TraceID))
	defer workflowSpan.End()
	logActionIntent("workflow.start", "Begin workflow execution", "trace_id", state.TraceID)

	if err := os.MkdirAll(defaultLogsDirValue, 0o755); err != nil {
		return fmt.Errorf("create logs directory: %w", err)
	}
	if err := os.MkdirAll(defaultTicketsDirValue, 0o755); err != nil {
		return fmt.Errorf("create tickets directory: %w", err)
	}

	if err := seedSampleLog(defaultLogsDirValue); err != nil {
		return fmt.Errorf("seed sample log: %w", err)
	}

	slog.Info("workflow.start", "trace_id", state.TraceID, "history_len", len(state.History), "mode", "deterministic")
	state.appendHistory("workflow started")
	logActionOutcome("workflow.start", "workflow initialized", "history_length", len(state.History))
	_ = ctx

	if apiKey := fetchAPIKey(); apiKey != "" {
		model, modelErr := gemini.NewModel(ctx, "gemini-2.5-flash", &genai.ClientConfig{APIKey: apiKey})
		if modelErr != nil {
			slog.Warn("agent.model.init_failed", "error", modelErr)
		} else {
			if ticketID, runErr := runADKTriage(ctx, model); runErr != nil {
				slog.Warn("agent.execution.failed", "error", runErr)
			} else {
				state.LastTicketID = ticketID
				state.appendHistory(fmt.Sprintf("created ticket %s", ticketID))
				if saveErr := state.save(defaultStatePathValue); saveErr != nil {
					return fmt.Errorf("save workflow state: %w", saveErr)
				}
				slog.Info("workflow.complete", "trace_id", state.TraceID, "ticket_id", ticketID, "status", "created_by_adk")
				return nil
			}
		}
	} else {
		slog.Warn("agent.model.init_skipped", "reason", "GEMINI_API_KEY not configured; using deterministic local workflow")
	}

	logSpanCtx, logSpan := tracer.Start(ctx, "workflow.log_ingestion")
	logActionIntent("log.ingestion", "Read and ingest latest error log")
	if err := seedSampleLog(defaultLogsDirValue); err != nil {
		logSpan.SetAttributes(attribute.String("error", err.Error()))
		logSpan.End()
		return fmt.Errorf("seed sample log: %w", err)
	}

	logContent, err := readLatestLogFile(defaultLogsDirValue)
	if err != nil {
		logSpan.SetAttributes(attribute.String("error", err.Error()))
		logSpan.End()
		return fmt.Errorf("read latest log: %w", err)
	}
	logSpan.SetAttributes(attribute.Int("log_length_bytes", len(logContent)))
	logActionOutcome("log.ingestion", "log loaded", "log_size", len(logContent))
	logSpan.End()
	if err != nil {
		return fmt.Errorf("read latest log: %w", err)
	}

	spanCtx, draftSpan := tracer.Start(logSpanCtx, "workflow.ticket_draft")
	logActionIntent("ticket.draft", "Generate a ticket draft from ingested log content", "severity_hint", inferSeverity(logContent))
	draft := buildTicketDraft(logContent)
	slog.Info("agent.execution", "agent", "triage", "action", "draft_ticket", "severity", draft.Severity, "title", draft.Title)
	state.appendHistory(fmt.Sprintf("drafted ticket %q", draft.Title))
	logActionOutcome("ticket.draft", "draft created", "severity", draft.Severity, "title", draft.Title)
	draftSpan.End()
	if _, err := os.Stat(defaultStateDB); err == nil {
		store, err := openStateStore(defaultStateDB)
		if err == nil {
			if saveErr := store.saveState(state); saveErr != nil {
				slog.Warn("workflow.state.persist_failed", "error", saveErr)
			}
			_ = store.close()
		}
	}

	_, validationSpan := tracer.Start(spanCtx, "workflow.ticket_validation")
	logActionIntent("ticket.validation", "Validate the drafted ticket for completeness and sensitivity")
	reviewResult := reviewTicketDraft(draft)
	slog.Info("agent.execution", "agent", "qa", "action", "validate_ticket", "decision", reviewResult, "trace_id", state.TraceID)
	if reviewResult != "APPROVED" {
		validationSpan.SetAttributes(attribute.String("validation_result", reviewResult))
		validationSpan.End()
		state.appendHistory("qa rejected ticket draft")
		if saveErr := state.save(defaultStatePathValue); saveErr != nil {
			return fmt.Errorf("save workflow state: %w", saveErr)
		}
		return nil
	}
	validationSpan.SetAttributes(attribute.String("validation_result", reviewResult))
	validationSpan.End()

	approval := promptForApproval(input, nonInteractive)
	if !approval {
		state.appendHistory("human rejected ticket")
		if saveErr := state.save(defaultStatePathValue); saveErr != nil {
			return fmt.Errorf("save workflow state: %w", saveErr)
		}
		return nil
	}

	_, creationSpan := tracer.Start(ctx, "workflow.ticket_creation")
	defer creationSpan.End()
	logActionIntent("ticket.creation", "Create the approved ticket in the ticket store", "title", draft.Title, "severity", draft.Severity)
	result, err := CreateStructuredTicket(nil, CreateTicketArgs{
		Title:       draft.Title,
		Severity:    draft.Severity,
		RootCause:   draft.RootCause,
		ProposedFix: draft.ProposedFix,
	})
	if err != nil {
		creationSpan.SetAttributes(attribute.String("outcome", "error"), attribute.String("error", err.Error()))
		return err
	}

	state.LastTicketID = result.TicketID
	state.appendHistory(fmt.Sprintf("created ticket %s", result.TicketID))
	if saveErr := state.save(defaultStatePathValue); saveErr != nil {
		creationSpan.SetAttributes(attribute.String("outcome", "error"), attribute.String("error", saveErr.Error()))
		return fmt.Errorf("save workflow state: %w", saveErr)
	}

	creationSpan.SetAttributes(attribute.String("outcome", "success"), attribute.String("ticket_id", result.TicketID), attribute.String("ticket_status", result.Status))
	logActionOutcome("ticket.creation", "ticket created", "ticket_id", result.TicketID, "status", result.Status)
	slog.Info("workflow.complete", "trace_id", state.TraceID, "ticket_id", result.TicketID, "status", result.Status)
	return nil
}

func runADKTriage(ctx context.Context, model model.LLM) (string, error) {
	var createdTicketID string

	readLogTool, err := functiontool.New(functiontool.Config{
		Name:        "read_latest_error_log",
		Description: "Read the latest redacted error log from the local logs directory.",
	}, ReadLatestErrorLog)
	if err != nil {
		return "", fmt.Errorf("create read log tool: %w", err)
	}

	ticketTool, err := functiontool.New(functiontool.Config{
		Name:        "create_structured_ticket",
		Description: "Create a structured ticket with a title, severity, root cause, and proposed fix.",
	}, func(ctx agent.Context, args CreateTicketArgs) (CreateTicketResult, error) {
		result, err := CreateStructuredTicket(ctx, args)
		if err == nil && result.TicketID != "" {
			createdTicketID = result.TicketID
		}
		return result, err
	})
	if err != nil {
		return "", fmt.Errorf("create ticket tool: %w", err)
	}

	tracer := otel.Tracer("devops-triage-agent")
	initSpanCtx, initSpan := tracer.Start(ctx, "workflow.agent_init")
	logActionIntent("agent.init", "Initialize ADK triage agent and runner with tool support")
	triageAgent, err := llmagent.New(llmagent.Config{
		Name:        "triage_agent",
		Description: "A devops triage assistant that reads local logs and generates structured bug tickets when needed.",
		Model:       model,
		Instruction: "You are a devops incident triage assistant. First use the read_latest_error_log tool to inspect the most recent error log. If the log shows a real crash, call create_structured_ticket with a concise title, severity, root_cause, and proposed_fix. If the log does not require a ticket, explain why no ticket should be created.",
		GlobalInstruction: "Be factual, do not hallucinate, and only use the provided tools.",
		Tools:       []tool.Tool{readLogTool, ticketTool},
	})
	if err != nil {
		initSpan.SetAttributes(attribute.String("outcome", "error"), attribute.String("error", err.Error()))
		initSpan.End()
		return "", fmt.Errorf("create triage agent: %w", err)
	}

	sessionService := session.InMemoryService()
	runner, err := runner.New(runner.Config{AppName: "devops-triage-agent", Agent: triageAgent, SessionService: sessionService, AutoCreateSession: true})
	if err != nil {
		initSpan.SetAttributes(attribute.String("outcome", "error"), attribute.String("error", err.Error()))
		initSpan.End()
		return "", fmt.Errorf("create runner: %w", err)
	}
	initSpan.SetAttributes(attribute.String("outcome", "success"), attribute.String("agent_name", triageAgent.Name()))
	initSpan.End()

	userMessage := genai.NewContentFromText("Review the latest error log and create a structured ticket if appropriate.", genai.RoleUser)
	runSpanCtx, runSpan := tracer.Start(initSpanCtx, "workflow.agent_run")
	logActionIntent("agent.run", "Execute the ADK agent run to inspect logs and create a ticket if needed")
	defer runSpan.End()

	for event, err := range runner.Run(runSpanCtx, "devops-triage-user", "triage-session", userMessage, agent.RunConfig{StreamingMode: agent.StreamingModeNone}) {
		if err != nil {
			runSpan.SetAttributes(attribute.String("outcome", "error"), attribute.String("error", err.Error()))
			return "", err
		}
		if event == nil {
			continue
		}
		slog.Info("agent.event", "author", event.Author, "final", event.IsFinalResponse(), "branch", event.Branch)
		if event.LLMResponse.Content != nil {
			for _, part := range event.LLMResponse.Content.Parts {
				if strings.TrimSpace(part.Text) != "" {
					slog.Debug("agent.event.text", "text", strings.TrimSpace(part.Text))
				}
			}
		}
	}

	runSpan.SetAttributes(attribute.String("outcome", "success"), attribute.Bool("ticket_created", createdTicketID != ""))
	if createdTicketID != "" {
		runSpan.SetAttributes(attribute.String("ticket_id", createdTicketID))
		logActionOutcome("agent.run", "ticket creation flow completed", "ticket_id", createdTicketID)
	} else {
		logActionOutcome("agent.run", "agent run completed without ticket creation")
	}

	return createdTicketID, nil
}

func seedSampleLog(logDir string) error {
	if _, err := os.Stat(filepath.Join(logDir, "crash_001.log")); err == nil {
		return nil
	}
	mockError := `2026-07-04T23:14:02Z ERROR [user-service] database connection failed for user@example.com.
panic: runtime error: invalid memory address or nil pointer dereference`
	return os.WriteFile(filepath.Join(logDir, "crash_001.log"), []byte(mockError), 0o644)
}

func readLatestLogFile(logDir string) (string, error) {
	files, err := os.ReadDir(logDir)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("no log files found in %s", logDir)
	}
	content, err := os.ReadFile(latestFile)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func buildTicketDraft(logContent string) ticketDraft {
	severity := inferSeverity(logContent)
	title := "Service crash detected"
	rootCause := "The service failed during request processing and the log indicates an application-level exception."
	proposedFix := "Inspect the failing dependency chain, add defensive checks around the affected component, and verify the service after deployment."
	if strings.Contains(strings.ToLower(logContent), "database") {
		title = "Database connection failure"
		rootCause = "The application could not reach the database layer and the stack trace suggests a nil or invalid dependency reference."
		proposedFix = "Validate the database connection path, add nil-safe initialization, and re-run the service with trace logging enabled."
	}
	if strings.Contains(strings.ToLower(logContent), "panic") {
		rootCause = "The panic originated from an invalid memory access, likely caused by a nil pointer or missing initialization path."
	}
	return ticketDraft{
		Title:       title,
		Severity:    severity,
		RootCause:   redactPII(rootCause),
		ProposedFix: redactPII(proposedFix),
	}
}

func reviewTicketDraft(draft ticketDraft) string {
	if strings.TrimSpace(draft.Title) == "" || strings.TrimSpace(draft.RootCause) == "" || strings.TrimSpace(draft.ProposedFix) == "" {
		return "REJECTED"
	}
	if strings.Contains(draft.RootCause, "user@example.com") || strings.Contains(draft.ProposedFix, "user@example.com") {
		return "REJECTED"
	}
	return "APPROVED"
}

func promptForApproval(input string, nonInteractive bool) bool {
	if nonInteractive {
		return true
	}
	if strings.TrimSpace(input) != "" {
		return strings.TrimSpace(strings.ToUpper(input)) == "Y"
	}
	fmt.Print("\n[Human-in-the-Loop] Agent proposes creating a ticket. Approve? (Y/N): ")
	reader := bufio.NewReader(os.Stdin)
	approval, _ := reader.ReadString('\n')
	return strings.TrimSpace(strings.ToUpper(approval)) == "Y"
}
