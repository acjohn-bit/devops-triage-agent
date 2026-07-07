FROM golang:1.23-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build the agent binary
COPY . .
RUN go build -o triage-agent main.go

# Use a clean, minimal runtime image
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/triage-agent .

# Run the binary
CMD ["./triage-agent"]