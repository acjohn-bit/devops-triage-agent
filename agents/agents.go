package agents

import (
	"context"
	"fmt"
	"strings"
	
	"google.golang.org/adk/v2/agent"
	"google.golang.org/adk/v2/agent/llmagent"
	"google.golang.org/adk/v2/runner"
	"google.golang.org/adk/v2/session"
	"google.golang.org/adk/v2/tool"
	"google.golang.org/genai"
	"google.golang.org/adk/v2/model/gemini"

	"github.com/google/uuid"

	"devops-triage-agent/tools"
)

// selectAgentModel returns a model name for a given role, overridable via env vars.
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

	if value := strings.TrimSpace(tools.Getenv(envVar)); value != "" {
		return value
	}

	defaults := map[string]string{
		"triage": "gemini-2.5-flash",
		"qa":     "gemini-2.5-flash",
		"hitl":   "gemini-2.5-flash",
	}
	return defaults[role]
}

// EvaluateProposedTicketWithAgent runs an agentic evaluation of a proposed ticket.
func EvaluateProposedTicketWithAgent(ctx context.Context, modelName, apiKey string, proposed map[string]string) (string, string, error) {
	if proposed == nil {
		return "REJECTED", "no proposed ticket", nil
	}
	if modelName == "" || apiKey == "" {
		// deterministic fallback
		if tools.ReviewDeterministic(proposed) {
			return "APPROVED", "deterministic_fallback", nil
		}
		return "REJECTED", "deterministic_fallback", nil
	}
	m, err := gemini.NewModel(ctx, modelName, &genai.ClientConfig{APIKey: apiKey})
	if err != nil {
		return "REJECTED", "model_init_failed", err
	}

	instr := fmt.Sprintf("Evaluate the proposed ticket and decide whether it should be created.\nTicket: Title=%q, Severity=%q, RootCause=%q, ProposedFix=%q\nRespond with a single line beginning with APPROVED or REJECTED, followed by a brief rationale.", proposed["title"], proposed["severity"], proposed["root_cause"], proposed["proposed_fix"])
	evalAgent, err := llmagent.New(llmagent.Config{
		Name:        "qa_evaluator",
		Description: "Agentic evaluator that inspects a proposed ticket and returns APPROVED or REJECTED with rationale.",
		Model:       m,
		Instruction: instr,
		GlobalInstruction: "Be concise and factual. Output must start with APPROVED or REJECTED.",
	})
	if err != nil {
		return "REJECTED", "agent_init_failed", err
	}

	rs := session.InMemoryService()
	r, err := runner.New(runner.Config{AppName: "devops-triage-agent", Agent: evalAgent, SessionService: rs, AutoCreateSession: true})
	if err != nil {
		return "REJECTED", "runner_init_failed", err
	}

	prompt := genai.NewContentFromText("Please evaluate the ticket and respond as instructed.", genai.RoleUser)
	var collected string
	for event, err := range r.Run(ctx, uuid.NewString(), "qa-eval-session", prompt, agent.RunConfig{StreamingMode: agent.StreamingModeNone}) {
		if err != nil {
			return "REJECTED", "run_error", err
		}
		if event == nil || event.LLMResponse.Content == nil {
			continue
		}
		for _, p := range event.LLMResponse.Content.Parts {
			collected += strings.TrimSpace(p.Text) + "\n"
		}
	}

	lines := strings.Split(strings.TrimSpace(collected), "\n")
	if len(lines) == 0 {
		return "REJECTED", "empty_response", nil
	}
	first := strings.TrimSpace(lines[len(lines)-1])
	if strings.HasPrefix(strings.ToUpper(first), "APPROVED") {
		return "APPROVED", first, nil
	}
	if strings.HasPrefix(strings.ToUpper(first), "REJECTED") {
		return "REJECTED", first, nil
	}
	if strings.Contains(strings.ToLower(collected), "approve") {
		return "APPROVED", first, nil
	}
	return "REJECTED", first, nil
}

// RunADKTriage orchestrates triage->qa->hitl using tools package and returns created ticket ID.
func RunADKTriage(ctx context.Context, apiKey string) (string, error) {
	// configure tools defaults if needed (done by caller)
	var proposed map[string]string
	var decision string
	// --- triage agent ---
	triageModel := selectAgentModel("triage")
	triageM, _ := gemini.NewModel(ctx, triageModel, &genai.ClientConfig{APIKey: apiKey})
	readTool, _ := tools.BuildReadLatestErrorLogTool()
	proposeTool, _ := tools.BuildProposeTicketTool(&proposed)
	triageAgent, err := llmagent.New(llmagent.Config{
		Name: "triage_agent",
		Description: "Propose a structured ticket draft based on latest logs.",
		Model: triageM,
		Instruction: "Inspect logs with read_latest_error_log and, if appropriate, call propose_ticket with a concise title, severity, root_cause, and proposed_fix. Do NOT persist the ticket.",
		GlobalInstruction: "Be factual and only use the provided tools.",
		Tools: []tool.Tool{readTool, proposeTool},
	})
	if err != nil {
		return "", err
	}
	triageRunner, err := runner.New(runner.Config{AppName: "devops-triage-agent", Agent: triageAgent, SessionService: session.InMemoryService(), AutoCreateSession: true})
	if err != nil {
		return "", err
	}
	prompt := genai.NewContentFromText("Review the latest error log and, if relevant, call propose_ticket to draft a structured ticket.", genai.RoleUser)
	for event, err := range triageRunner.Run(ctx, uuid.NewString(), "triage-session", prompt, agent.RunConfig{StreamingMode: agent.StreamingModeNone}) {
		if err != nil {
			return "", err
		}
		_ = event
	}
	if proposed == nil {
		return "", nil
	}
	// --- QA ---
	qaModel := selectAgentModel("qa")
	decision, _, err = EvaluateProposedTicketWithAgent(ctx, qaModel, apiKey, proposed)
	if err != nil {
		return "", err
	}
	if decision != "APPROVED" {
		return "", nil
	}
	// --- HITL (auto-approve via env) ---
	var hitlApproved bool
	hitlTool, err := tools.BuildHitlApprovalTool(&hitlApproved)
	if err != nil {
		return "", err
	}
	hitlAgent, err := llmagent.New(llmagent.Config{
		Name: "hitl_agent",
		Description: "Request human-in-the-loop approval (auto-approve available).",
		Model: triageM,
		Instruction: "Call hitl_approval to check for an automated approval flag; otherwise defer to an external human.",
		GlobalInstruction: "Do not act without explicit approval.",
		Tools: []tool.Tool{hitlTool},
	})
	if err != nil {
		return "", err
	}
	hitlRunner, err := runner.New(runner.Config{AppName: "devops-triage-agent", Agent: hitlAgent, SessionService: session.InMemoryService(), AutoCreateSession: true})
	if err != nil {
		return "", err
	}
	hitlPrompt := genai.NewContentFromText("Request approval for creating the proposed ticket. Use hitl_approval tool.", genai.RoleUser)
	for event, err := range hitlRunner.Run(ctx, uuid.NewString(), "hitl-session", hitlPrompt, agent.RunConfig{StreamingMode: agent.StreamingModeNone}) {
		if err != nil {
			return "", err
		}
		_ = event
	}
	if !hitlApproved {
		return "", nil
	}

	// Persist ticket
	res, err := tools.CreateStructuredTicket(ctx, tools.CreateTicketArgs{
		Title: proposed["title"],
		Severity: proposed["severity"],
		RootCause: proposed["root_cause"],
		ProposedFix: proposed["proposed_fix"],
	})
	if err != nil {
		return "", err
	}
	return res.TicketID, nil
}
