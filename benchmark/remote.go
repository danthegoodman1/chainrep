package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SSHConfig struct {
	User         string
	PrivateKey   string
	JumpPublicIP string
	DisableJump  bool
}

func (c SSHConfig) baseArgs(target string) []string {
	args := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-i", c.PrivateKey,
	}
	if !c.DisableJump && c.JumpPublicIP != "" && target != c.JumpPublicIP {
		args = append(args, "-J", fmt.Sprintf("%s@%s", c.User, c.JumpPublicIP))
	}
	args = append(args, fmt.Sprintf("%s@%s", c.User, target))
	return args
}

func SSH(ctx context.Context, ssh SSHConfig, target string, command string) error {
	args := ssh.baseArgs(target)
	args = append(args, command)
	return runCommand(ctx, nil, "", "ssh", args...)
}

func SSHCapture(ctx context.Context, ssh SSHConfig, target string, command string) ([]byte, error) {
	args := ssh.baseArgs(target)
	args = append(args, command)
	return captureCommand(ctx, nil, "", "ssh", args...)
}

func SCP(ctx context.Context, ssh SSHConfig, source string, targetHost string, targetPath string) error {
	args := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-i", ssh.PrivateKey,
	}
	if !ssh.DisableJump && ssh.JumpPublicIP != "" && targetHost != ssh.JumpPublicIP {
		args = append(args, "-J", fmt.Sprintf("%s@%s", ssh.User, ssh.JumpPublicIP))
	}
	args = append(args, source, fmt.Sprintf("%s@%s:%s", ssh.User, targetHost, targetPath))
	return runCommand(ctx, nil, "", "scp", args...)
}

func SCPFrom(ctx context.Context, ssh SSHConfig, sourceHost string, sourcePath string, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("mkdir scp destination: %w", err)
	}
	args := []string{
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-i", ssh.PrivateKey,
	}
	if !ssh.DisableJump && ssh.JumpPublicIP != "" && sourceHost != ssh.JumpPublicIP {
		args = append(args, "-J", fmt.Sprintf("%s@%s", ssh.User, ssh.JumpPublicIP))
	}
	args = append(args, fmt.Sprintf("%s@%s:%s", ssh.User, sourceHost, sourcePath), localPath)
	return runCommand(ctx, nil, "", "scp", args...)
}

func waitForSSH(ctx context.Context, ssh SSHConfig, host string) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := SSH(ctx, SSHConfig{
			User:         ssh.User,
			PrivateKey:   ssh.PrivateKey,
			JumpPublicIP: ssh.JumpPublicIP,
			DisableJump:  ssh.DisableJump,
		}, host, "true")
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
