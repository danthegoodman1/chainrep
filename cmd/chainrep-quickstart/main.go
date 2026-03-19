package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/danthegoodman1/chainrep/adminhttp"
	"github.com/danthegoodman1/chainrep/client"
	"github.com/danthegoodman1/chainrep/coordinator"
	coordruntime "github.com/danthegoodman1/chainrep/coordinator/runtime"
	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/quickstart"
	"github.com/danthegoodman1/chainrep/storage"
	"github.com/danthegoodman1/chainrep/transport/grpcx"
)

const (
	quickstartHeartbeatInterval = 250 * time.Millisecond
	quickstartLivenessInterval  = 250 * time.Millisecond
	quickstartSuspectAfter      = 750 * time.Millisecond
	quickstartDeadAfter         = 1500 * time.Millisecond
	quickstartActivationLoop    = 100 * time.Millisecond
	quickstartRPCDeadline       = 5 * time.Second
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usageError("")
	}
	switch args[0] {
	case "coordinator":
		return runCoordinator(args[1:])
	case "storage":
		return runStorage(args[1:])
	case "get", "set", "del":
		return runCLI(args)
	default:
		return usageError(args[0])
	}
}

func usageError(command string) error {
	if command == "" {
		return fmt.Errorf("usage: chainrep-quickstart <coordinator|storage|get|set|del> [flags]")
	}
	return fmt.Errorf("unknown command %q (expected coordinator, storage, get, set, or del)", command)
}

func runCoordinator(args []string) error {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	manifestPath := fs.String("manifest", quickstart.DefaultManifestPath, "quickstart manifest path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := quickstart.Load(*manifestPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()

	server, err := coordserver.OpenWithConfig(ctx, coordruntime.NewInMemoryStore(), nil, coordserver.ServerConfig{
		LivenessPolicy: coordserver.LivenessPolicy{
			SuspectAfter: quickstartSuspectAfter,
			DeadAfter:    quickstartDeadAfter,
		},
		DispatchRetryInterval: time.Hour,
		NodeClientFactory:     grpcx.NewDynamicNodeClientFactory(pool),
	})
	if err != nil {
		return fmt.Errorf("open coordinator server: %w", err)
	}
	defer func() { _ = server.Close() }()

	if server.Current().Cluster.SlotCount == 0 {
		if _, err := server.Bootstrap(ctx, coordruntime.Command{
			ID:              "quickstart-bootstrap",
			ExpectedVersion: 0,
			Kind:            coordruntime.CommandKindBootstrap,
			Bootstrap: &coordruntime.BootstrapCommand{
				Config: coordinatorConfig(cfg),
			},
		}); err != nil {
			return fmt.Errorf("bootstrap coordinator: %w", err)
		}
	}

	lis, err := net.Listen("tcp", cfg.Coordinator.RPCAddress)
	if err != nil {
		return fmt.Errorf("listen coordinator grpc %q: %w", cfg.Coordinator.RPCAddress, err)
	}
	grpcServer := grpcx.NewCoordinatorGRPCServer(server)
	defer func() { _ = grpcServer.Close() }()
	go serveGRPC(grpcServer, lis)

	var admin *adminhttp.Server
	if cfg.Coordinator.AdminAddress != "" {
		admin = adminhttp.NewCoordinator(server, adminhttp.Config{
			Address:  cfg.Coordinator.AdminAddress,
			Gatherer: server.MetricsRegistry(),
		})
		go serveAdmin(admin)
		defer func() { _ = admin.Close(context.Background()) }()
	}

	adminClient := grpcx.NewCoordinatorAdminClient(cfg.Coordinator.RPCAddress, pool)
	go runTicker(ctx, quickstartLivenessInterval, func() {
		evalCtx, cancel := context.WithTimeout(context.Background(), quickstartRPCDeadline)
		defer cancel()
		_ = adminClient.EvaluateLiveness(evalCtx)
	})

	<-ctx.Done()
	return nil
}

func runStorage(args []string) error {
	fs := flag.NewFlagSet("storage", flag.ContinueOnError)
	manifestPath := fs.String("manifest", quickstart.DefaultManifestPath, "quickstart manifest path")
	nodeID := fs.String("node", "", "node id from manifest")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *nodeID == "" {
		return fmt.Errorf("storage requires --node")
	}
	cfg, err := quickstart.Load(*manifestPath)
	if err != nil {
		return err
	}
	nodeCfg, ok := cfg.NodeByID(*nodeID)
	if !ok {
		return fmt.Errorf("node %q not found in manifest", *nodeID)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()

	reporter := grpcx.NewCoordinatorReporterClient(nodeCfg.ID, cfg.Coordinator.RPCAddress, pool)
	repl := grpcx.NewReplicationTransport(pool)
	node, err := storage.OpenNode(
		ctx,
		storage.Config{
			NodeID:         nodeCfg.ID,
			RPCAddress:     nodeCfg.RPCAddress,
			FailureDomains: nodeCfg.FailureDomains,
		},
		storage.NewInMemoryBackend(),
		storage.NewInMemoryLocalStateStore(),
		reporter,
		repl,
	)
	if err != nil {
		return fmt.Errorf("open storage node %q: %w", nodeCfg.ID, err)
	}
	defer func() { _ = node.Close() }()

	lis, err := net.Listen("tcp", nodeCfg.RPCAddress)
	if err != nil {
		return fmt.Errorf("listen storage grpc %q: %w", nodeCfg.RPCAddress, err)
	}
	grpcServer := grpcx.NewStorageGRPCServer(node)
	defer func() { _ = grpcServer.Close() }()
	go serveGRPC(grpcServer, lis)

	var admin *adminhttp.Server
	if nodeCfg.AdminAddress != "" {
		admin = adminhttp.NewStorage(node, adminhttp.Config{
			Address:  nodeCfg.AdminAddress,
			Gatherer: node.MetricsRegistry(),
		})
		go serveAdmin(admin)
		defer func() { _ = admin.Close(context.Background()) }()
	}

	registered := false
	go runTicker(ctx, quickstartHeartbeatInterval, func() {
		hbCtx, cancel := context.WithTimeout(context.Background(), quickstartRPCDeadline)
		defer cancel()
		if !registered {
			if err := node.Register(hbCtx); err != nil {
				return
			}
			registered = true
		}
		_ = reporter.ReportNodeHeartbeat(hbCtx, nodeStatusFromState(node.State()))
	})
	go runTicker(ctx, quickstartActivationLoop, func() {
		actCtx, cancel := context.WithTimeout(context.Background(), quickstartRPCDeadline)
		defer cancel()
		activateCatchingUpReplicas(actCtx, node)
	})

	<-ctx.Done()
	return nil
}

func runCLI(args []string) error {
	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	manifestPath := fs.String("manifest", quickstart.DefaultManifestPath, "quickstart manifest path")
	timeout := fs.Duration("timeout", quickstartRPCDeadline, "per-request timeout")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	cfg, err := quickstart.Load(*manifestPath)
	if err != nil {
		return err
	}

	rest := fs.Args()
	switch command {
	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("usage: chainrep-quickstart get [--manifest path] <key>")
		}
		return runGet(cfg, rest[0], *timeout)
	case "set":
		if len(rest) != 2 {
			return fmt.Errorf("usage: chainrep-quickstart set [--manifest path] <key> <value>")
		}
		return runSet(cfg, rest[0], rest[1], *timeout)
	case "del":
		if len(rest) != 1 {
			return fmt.Errorf("usage: chainrep-quickstart del [--manifest path] <key>")
		}
		return runDel(cfg, rest[0], *timeout)
	default:
		return usageError(command)
	}
}

func runGet(cfg quickstart.Config, key string, timeout time.Duration) error {
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	admin := grpcx.NewCoordinatorAdminClient(cfg.Coordinator.RPCAddress, pool)
	transport := grpcx.NewClientTransport(pool)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	route, target, result, err := getWithRefresh(ctx, admin, transport, key)
	if err != nil {
		return err
	}
	printRead(commandOutput{
		Operation: "get",
		Key:       key,
		Route:     route,
		Target:    target,
		Read:      &result,
	})
	return nil
}

func runSet(cfg quickstart.Config, key string, value string, timeout time.Duration) error {
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	admin := grpcx.NewCoordinatorAdminClient(cfg.Coordinator.RPCAddress, pool)
	transport := grpcx.NewClientTransport(pool)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	route, target, result, err := putWithRefresh(ctx, admin, transport, key, value)
	if err != nil {
		return err
	}
	printCommit(commandOutput{
		Operation: "set",
		Key:       key,
		Route:     route,
		Target:    target,
		Commit:    &result,
	})
	return nil
}

func runDel(cfg quickstart.Config, key string, timeout time.Duration) error {
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	admin := grpcx.NewCoordinatorAdminClient(cfg.Coordinator.RPCAddress, pool)
	transport := grpcx.NewClientTransport(pool)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	route, target, result, err := deleteWithRefresh(ctx, admin, transport, key)
	if err != nil {
		return err
	}
	printCommit(commandOutput{
		Operation: "del",
		Key:       key,
		Route:     route,
		Target:    target,
		Commit:    &result,
	})
	return nil
}

type commandOutput struct {
	Operation string
	Key       string
	Route     coordserver.SlotRoute
	Target    string
	Read      *storage.ReadResult
	Commit    *storage.CommitResult
}

func printRead(out commandOutput) {
	fmt.Printf("operation=%s\n", out.Operation)
	fmt.Printf("key=%s\n", out.Key)
	fmt.Printf("slot=%d\n", out.Route.Slot)
	fmt.Printf("chain_version=%d\n", out.Route.ChainVersion)
	fmt.Printf("contacted_node=%s\n", out.Route.TailNodeID)
	fmt.Printf("contacted_endpoint=%s\n", out.Target)
	fmt.Printf("found=%t\n", out.Read.Found)
	if out.Read.Found {
		fmt.Printf("value=%s\n", out.Read.Value)
	}
	printMetadata(out.Read.Metadata)
}

func printCommit(out commandOutput) {
	fmt.Printf("operation=%s\n", out.Operation)
	fmt.Printf("key=%s\n", out.Key)
	fmt.Printf("slot=%d\n", out.Route.Slot)
	fmt.Printf("chain_version=%d\n", out.Route.ChainVersion)
	fmt.Printf("contacted_node=%s\n", out.Route.HeadNodeID)
	fmt.Printf("contacted_endpoint=%s\n", out.Target)
	fmt.Printf("applied=%t\n", out.Commit.Applied)
	fmt.Printf("sequence=%d\n", out.Commit.Sequence)
	printMetadata(out.Commit.Metadata)
}

func printMetadata(metadata *storage.ObjectMetadata) {
	if metadata == nil {
		fmt.Println("metadata=<nil>")
		return
	}
	fmt.Printf("metadata.version=%d\n", metadata.Version)
	fmt.Printf("metadata.created_at=%s\n", metadata.CreatedAt.UTC().Format(time.RFC3339Nano))
	fmt.Printf("metadata.updated_at=%s\n", metadata.UpdatedAt.UTC().Format(time.RFC3339Nano))
}

func getWithRefresh(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	transport *grpcx.ClientTransport,
	key string,
) (coordserver.SlotRoute, string, storage.ReadResult, error) {
	return withSnapshotRetry(
		ctx,
		admin,
		key,
		func(route coordserver.SlotRoute, target string) (storage.ReadResult, error) {
			if !route.Readable || route.TailNodeID == "" {
				return storage.ReadResult{}, fmt.Errorf("%w: slot %d is not readable", client.ErrNoRoute, route.Slot)
			}
			result, err := transport.Get(ctx, target, storage.ClientGetRequest{
				Slot:                 route.Slot,
				Key:                  key,
				ExpectedChainVersion: route.ChainVersion,
			})
			return result, err
		},
		func(route coordserver.SlotRoute) string {
			return routeTarget(route.TailEndpoint, route.TailNodeID)
		},
	)
}

func putWithRefresh(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	transport *grpcx.ClientTransport,
	key string,
	value string,
) (coordserver.SlotRoute, string, storage.CommitResult, error) {
	return withSnapshotRetry(
		ctx,
		admin,
		key,
		func(route coordserver.SlotRoute, target string) (storage.CommitResult, error) {
			if !route.Writable || route.HeadNodeID == "" {
				return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", client.ErrNoRoute, route.Slot)
			}
			result, err := transport.Put(ctx, target, storage.ClientPutRequest{
				Slot:                 route.Slot,
				Key:                  key,
				Value:                value,
				ExpectedChainVersion: route.ChainVersion,
			})
			return result, err
		},
		func(route coordserver.SlotRoute) string {
			return routeTarget(route.HeadEndpoint, route.HeadNodeID)
		},
	)
}

func deleteWithRefresh(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	transport *grpcx.ClientTransport,
	key string,
) (coordserver.SlotRoute, string, storage.CommitResult, error) {
	return withSnapshotRetry(
		ctx,
		admin,
		key,
		func(route coordserver.SlotRoute, target string) (storage.CommitResult, error) {
			if !route.Writable || route.HeadNodeID == "" {
				return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", client.ErrNoRoute, route.Slot)
			}
			result, err := transport.Delete(ctx, target, storage.ClientDeleteRequest{
				Slot:                 route.Slot,
				Key:                  key,
				ExpectedChainVersion: route.ChainVersion,
			})
			return result, err
		},
		func(route coordserver.SlotRoute) string {
			return routeTarget(route.HeadEndpoint, route.HeadNodeID)
		},
	)
}

func withSnapshotRetry[T any](
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	key string,
	call func(coordserver.SlotRoute, string) (T, error),
	targetFor func(coordserver.SlotRoute) string,
) (coordserver.SlotRoute, string, T, error) {
	var zero T
	snapshot, err := admin.RoutingSnapshot(ctx)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, fmt.Errorf("fetch routing snapshot: %w", err)
	}
	route, err := client.RouteForKey(snapshot, key)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, err
	}
	target := targetFor(route)
	result, err := call(route, target)
	if err == nil {
		return route, target, result, nil
	}
	if !isRoutingMismatchError(err) {
		return coordserver.SlotRoute{}, "", zero, err
	}
	snapshot, err = admin.RoutingSnapshot(ctx)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, fmt.Errorf("refresh routing snapshot: %w", err)
	}
	route, err = client.RouteForKey(snapshot, key)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, err
	}
	target = targetFor(route)
	result, err = call(route, target)
	if err != nil {
		return coordserver.SlotRoute{}, "", zero, err
	}
	return route, target, result, nil
}

func isRoutingMismatchError(err error) bool {
	var mismatch *storage.RoutingMismatchError
	return errors.As(err, &mismatch) || strings.Contains(err.Error(), storage.ErrRoutingMismatch.Error())
}

func routeTarget(endpoint string, fallbackNodeID string) string {
	if endpoint != "" {
		return endpoint
	}
	return fallbackNodeID
}

func activateCatchingUpReplicas(ctx context.Context, node *storage.Node) {
	for slot, replica := range node.State().Replicas {
		if replica.State != storage.ReplicaStateCatchingUp {
			continue
		}
		_ = node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot})
	}
}

func nodeStatusFromState(state storage.NodeState) storage.NodeStatus {
	status := storage.NodeStatus{NodeID: state.NodeID}
	for _, replica := range state.Replicas {
		status.ReplicaCount++
		switch replica.State {
		case storage.ReplicaStateActive:
			status.ActiveCount++
		case storage.ReplicaStateCatchingUp:
			status.CatchingUpCount++
		case storage.ReplicaStateLeaving:
			status.LeavingCount++
		}
	}
	return status
}

func runTicker(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	fn()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

func serveGRPC(server interface {
	Serve(net.Listener) error
}, lis net.Listener) {
	if err := server.Serve(lis); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		fmt.Fprintf(os.Stderr, "grpc serve error: %v\n", err)
	}
}

func serveAdmin(server *adminhttp.Server) {
	if err := server.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "closed network connection") {
		fmt.Fprintf(os.Stderr, "admin serve error: %v\n", err)
	}
}

func coordinatorConfig(cfg quickstart.Config) coordinator.Config {
	return coordinator.Config{
		SlotCount:         cfg.Coordinator.SlotCount,
		ReplicationFactor: cfg.Coordinator.ReplicationFactor,
	}
}
