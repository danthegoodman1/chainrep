package benchmark

import (
	"context"
	"fmt"
	"time"

	"github.com/danthegoodman1/chainrep/quickstart"
	"github.com/danthegoodman1/chainrep/transport/grpcx"
)

func WaitForClusterReady(ctx context.Context, manifestPath string, timeout time.Duration) error {
	manifest, err := quickstart.Load(manifestPath)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	admin := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	transport := grpcx.NewClientTransport(pool)
	key := "chainrep-bench-ready"
	value := "ready"
	var lastErr error
	for time.Now().Before(deadline) {
		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _, _, err := putWithRefresh(reqCtx, admin, transport, key, value)
		cancel()
		if err == nil {
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			_, _, result, readErr := getWithRefresh(readCtx, admin, transport, key)
			readCancel()
			if readErr == nil && result.Found && result.Value == value {
				return nil
			}
			lastErr = readErr
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timed out waiting for ready probe")
	}
	return lastErr
}
