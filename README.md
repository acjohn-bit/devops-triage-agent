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

## Secrets and Model Access

This agent uses the Gemini model and expects an API key to be available in one of the following environment variables:

- `GEMINI_API_KEY`
- `GOOGLE_API_KEY`

For local development, set the variable in your shell before running:

```bash
export GEMINI_API_KEY="your-gemini-api-key"
go run .
```

For production deployment, secret management should be handled by your cloud provider or runtime environment. On Google Cloud, use Secret Manager to inject the model credential into the container environment and keep local state in the SQLite file at `./state/workflow_state.sqlite`.

## Terraform Usage

The `terraform/` directory contains a minimal scaffold to deploy the agent as a Cloud Run service.

```bash
cd terraform
terraform init
terraform apply
```

After the apply succeeds, update the Cloud Run service image or environment variables as needed for your deployment.

## Testing

```bash
go test ./...
```
