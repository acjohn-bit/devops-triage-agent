package main

import (
    "bufio"
    "context"
    "fmt"
    "log/slog"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    

    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

    "devops-triage-agent/agents"
    "devops-triage-agent/tools"
    st "devops-triage-agent/state"
)

const (
    defaultLogsDir    = "./logs"
    defaultTicketsDir = "./tickets"
    defaultStateDB    = "./state/workflow_state.sqlite"
)

var (
    defaultLogsDirValue    = defaultLogsDir
    defaultTicketsDirValue = defaultTicketsDir
    defaultStateDBValue    = defaultStateDB
)

var emailPattern = regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)
var secretPattern = regexp.MustCompile(`(?i)\b(?:password|token|api[_-]?key)[\s:=]+[A-Za-z0-9._-]{3,}`)

type ticketDraft struct {
    Title       string
    Severity    string
    RootCause   string
    ProposedFix string
}

// ProposedTicket is the structured draft proposed by the triage agent.
type ProposedTicket struct {
    Title       string `json:"title"`
    Severity    string `json:"severity"`
    RootCause   string `json:"root_cause"`
    ProposedFix string `json:"proposed_fix"`
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

func selectAgentModel(role string) string {
    role = strings.ToLower(strings.TrimSpace(role))
    if role == "" {
        role = "triage"
    }

    var envVar string
    switch role {
    case "qa", "review", "validator":
        envVar = "GEMINI_QA_MODEL"
    case "hitl", "human", "approval":
        envVar = "GEMINI_HITL_MODEL"
    default:
        envVar = "GEMINI_TRIAGE_MODEL"
    }

    if value := strings.TrimSpace(os.Getenv(envVar)); value != "" {
        return value
    }

    defaults := map[string]string{
        "triage": "gemini-2.5-flash",
        "qa":     "gemini-2.5-flash",
        "hitl":   "gemini-2.5-flash",
    }
    return defaults[role]
}

func fetchSecretFromGCP(projectID, secretName string) (string, error) {
    // minimal helper; tests do not rely on GCP in this environment
    return "", fmt.Errorf("not implemented in test environment")
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

func runWorkflow(ctx context.Context) error { return runWorkflowWithInputContext(ctx, "", false) }
func runWorkflowWithInput(input string, nonInteractive bool) error { return runWorkflowWithInputContext(context.Background(), input, nonInteractive) }

func runWorkflowWithInputContext(ctx context.Context, input string, nonInteractive bool) error {
    logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
    slog.SetDefault(logger)

    tracerProvider, err := initTracing()
    if err != nil {
        slog.Warn("tracing.init_failed", "error", err)
    } else {
        defer tracerProvider.Shutdown(ctx)
    }

    store, err := st.OpenStateStore(defaultStateDBValue)
    if err != nil {
        return fmt.Errorf("open workflow state store: %w", err)
    }
    defer store.Close()

    state, err := store.LoadState()
    if err != nil {
        return fmt.Errorf("load workflow state: %w", err)
    }

    saver := st.NewAsyncStateSaver(store)
    defer func() {
        if closeErr := saver.Close(ctx); closeErr != nil {
            slog.Warn("workflow.state.shutdown_failed", "error", closeErr)
        }
    }()
    saver.Save(state)

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

    state.AppendHistory("workflow started")
    saver.Save(state)
    logActionOutcome("workflow.start", "workflow initialized", "history_length", len(state.History))

    if apiKey := fetchAPIKey(); apiKey != "" {
        tools.DefaultLogsDir = defaultLogsDirValue
        tools.DefaultTicketsDir = defaultTicketsDirValue

        state.AppendHistory("agent turn start")
        saver.Save(state)
        if ticketID, runErr := agents.RunADKTriage(ctx, apiKey); runErr != nil {
            slog.Warn("agent.execution.failed", "error", runErr)
        } else if ticketID != "" {
            state.LastTicketID = ticketID
            state.AppendHistory(fmt.Sprintf("created ticket %s", ticketID))
            saver.Save(state)
            slog.Info("workflow.complete", "trace_id", state.TraceID, "ticket_id", ticketID, "status", "created_by_adk")
            return nil
        }
    } else {
        slog.Warn("agent.model.init_skipped", "reason", "GEMINI_API_KEY not configured; using deterministic local workflow")
    }

    // Deterministic fallback path: read latest log, draft, validate, and create ticket locally.
    logContent, err := tools.ReadLatestErrorLog(ctx, tools.ReadLogArgs{})
    if err != nil {
        return fmt.Errorf("read latest log: %w", err)
    }

    draft := buildTicketDraft(logContent.Content)
    state.AppendHistory(fmt.Sprintf("drafted ticket %q", draft.Title))

    reviewResult := reviewTicketDraft(draft)
    if reviewResult != "APPROVED" {
        state.AppendHistory("qa rejected ticket draft")
        saver.Save(state)
        return nil
    }

    approval := promptForApproval(input, nonInteractive)
    if !approval {
        state.AppendHistory("human rejected ticket")
        saver.Save(state)
        return nil
    }

    res, err := tools.CreateStructuredTicket(ctx, tools.CreateTicketArgs{
        Title:       draft.Title,
        Severity:    draft.Severity,
        RootCause:   draft.RootCause,
        ProposedFix: draft.ProposedFix,
    })
    if err != nil {
        return err
    }

    state.LastTicketID = res.TicketID
    state.AppendHistory(fmt.Sprintf("created ticket %s", res.TicketID))
    saver.Save(state)
    logActionOutcome("ticket.creation", "ticket created", "ticket_id", res.TicketID, "status", res.Status)
    slog.Info("workflow.complete", "trace_id", state.TraceID, "ticket_id", res.TicketID, "status", res.Status)
    return nil
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
