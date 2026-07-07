# Automated DevOps Triage Agent (Go ADK MVP)

The Automated DevOps Triage Agent routes simulated error logs through a deterministic workflow that can create structured tickets and persist workflow state locally. The implementation now includes JSON-structured workflow logging via slog, PII redaction, a non-interactive approval path for evaluation and automation, SQLite-backed workflow state persistence, OpenTelemetry tracing output, and a lightweight Terraform scaffold for Cloud Run deployment.

## Project Architecture
- **Language:** Go 1.26.4
- **Framework:** Google ADK (`google.golang.org/adk/v2`)
- **Model:** `gemini-2.5-flash`
- **Simulated Input:** Local `/logs` directory containing `.log` files.
- **Simulated Output:** Local `/tickets` directory generating strict `.json` payloads.

## Running Locally

```bash
go run .
```

## Testing

```bash
go test ./...
```
