# main.tf - Infrastructure as Code for ADK Deployment
provider "google" {
  project = "devops-triage-project"
  region  = "us-central1"
}

resource "google_cloud_run_service" "adk_agent" {
  name     = "triage-agent-service"
  location = "us-central1"
  template {
    spec {
      containers {
        image = "gcr.io/devops-triage-project/agent:latest"
      }
    }
  }
}