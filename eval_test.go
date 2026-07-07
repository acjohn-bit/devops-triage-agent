package main

import "testing"

func TestAgentEvaluation(t *testing.T) {
    mockInput := "database connection failed"
    expectedOutput := "APPROVED"
    
    if redactPII(mockInput) == expectedOutput {
        t.Errorf("Eval failure: expected %s", expectedOutput)
    }
}