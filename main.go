package main

import (
    "context"
    "log/slog"
    "os"
)

func main() {
    if err := runWorkflow(context.Background()); err != nil {
        slog.Error("workflow.failed", "error", err)
        os.Exit(1)
    }
}
