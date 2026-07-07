terraform {
  required_version = ">= 1.5.0"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_cloud_run_v2_service" "triage_agent" {
  name     = "devops-triage-agent"
  location = var.region
  template {
    containers {
      image = "us-docker.pkg.dev/${var.project_id}/triage-agent/agent:latest"
      env {
        name  = "GEMINI_API_KEY"
        value = "set-via-secret-manager"
      }
    }
  }
}
