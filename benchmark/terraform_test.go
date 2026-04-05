package benchmark

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestTerraformFormattingAndValidate(t *testing.T) {
	if _, err := exec.LookPath("terraform"); err != nil {
		t.Skip("terraform not installed")
	}
	if testing.Short() {
		t.Skip("skipping terraform validation in short mode")
	}
	root := filepath.Join("..", "infra", "benchmark", "aws")
	if err := runCommand(context.Background(), nil, "", "terraform", "-chdir="+root, "fmt", "-check"); err != nil {
		t.Fatalf("terraform fmt -check returned error: %v", err)
	}
	if err := runCommand(context.Background(), nil, root, "terraform", "init", "-backend=false"); err != nil {
		t.Skipf("terraform init unavailable in this environment: %v", err)
	}
	if err := runCommand(context.Background(), nil, root, "terraform", "validate"); err != nil {
		t.Fatalf("terraform validate returned error: %v", err)
	}
}
