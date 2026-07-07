# Automated DevOps Triage Agent (Go ADK MVP)

The **Automated DevOps Triage Agent** is an enterprise MVP built with the Go Agent Development Kit (ADK) that automates the translation of unstructured system failures into actionable, structured engineering tasks. When a simulated server crashes, the agent dynamically ingests the raw stack trace, analyzes the technical root cause, and autonomously generates a strictly-typed, Jira-style bug ticket. Because this is an intentional Minimum Viable Product (MVP), the surrounding enterprise infrastructure is simulated locally via file I/O: an input `/logs` directory acts as the real-time server telemetry stream, and an output `/tickets` directory serves as the mock API endpoint where the agent securely writes the formatted JSON payloads using Go struct-enforced tools.

## Project Architecture
- **Language:** Go 1.23
- **Framework:** Google ADK (`google.golang.org/adk/v2`)
- **Model:** `gemini-2.5-flash`
- **Simulated Input:** Local `/logs` directory containing `.log` files.
- **Simulated Output:** Local `/tickets` directory generating strict `.json` payloads.

---

## How to Run Locally

### Prerequisites
1. [Go 1.23+](https://go.dev/dl/) installed on your machine.
2. A Gemini API key (get one from [Google AI Studio](https://aistudio.google.com/)).

### Setup Instructions
1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR-USERNAME/devops-triage-agent.git](https://github.com/YOUR-USERNAME/devops-triage-agent.git)
   cd devops-triage-agent
