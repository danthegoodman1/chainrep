package benchmark

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestAWSBenchmarkAcceptance(t *testing.T) {
	if os.Getenv("CHAINREP_AWS_ACCEPTANCE") != "1" {
		t.Skip("set CHAINREP_AWS_ACCEPTANCE=1 to run live AWS acceptance")
	}
	if _, err := exec.LookPath("terraform"); err != nil {
		t.Skip("terraform not installed")
	}
	repoRoot := filepath.Join("..")
	runDir, err := RunBenchmark(context.Background(), RunOptions{
		ProfilePath: filepath.Join(repoRoot, "profiles", "bench", "aws_i8ge_steady.yaml"),
		RepoRoot:    repoRoot,
		RunName:     "acceptance",
	})
	if err != nil {
		t.Fatalf("RunBenchmark returned error: %v", err)
	}
	if _, err := AnalyzeRun(runDir); err != nil {
		t.Fatalf("AnalyzeRun returned error: %v", err)
	}
}
