variable "project_id" {
  description = "Google Cloud project ID for the triage agent deployment"
  type        = string
  default     = "devops-triage-project"
}

variable "region" {
  description = "Primary deployment region"
  type        = string
  default     = "us-central1"
}
