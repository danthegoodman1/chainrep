package client

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/danthegoodman1/chainrep/coordserver"
	"github.com/danthegoodman1/chainrep/storage"
)

var (
	ErrInvalidConfig     = errors.New("invalid client router config")
	ErrSnapshotNotLoaded = errors.New("client routing snapshot not loaded")
	ErrNoRoute           = errors.New("client route unavailable")
)

type SnapshotSource interface {
	RoutingSnapshot(ctx context.Context) (coordserver.RoutingSnapshot, error)
}

type Transport interface {
	Get(ctx context.Context, nodeID string, req storage.ClientGetRequest) (storage.ReadResult, error)
	Put(ctx context.Context, nodeID string, req storage.ClientPutRequest) (storage.CommitResult, error)
	Delete(ctx context.Context, nodeID string, req storage.ClientDeleteRequest) (storage.CommitResult, error)
}

type Router struct {
	source    SnapshotSource
	transport Transport
	snapshot  *coordserver.RoutingSnapshot
}

func NewRouter(source SnapshotSource, transport Transport) (*Router, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: snapshot source must not be nil", ErrInvalidConfig)
	}
	if transport == nil {
		return nil, fmt.Errorf("%w: transport must not be nil", ErrInvalidConfig)
	}
	return &Router{
		source:    source,
		transport: transport,
	}, nil
}

func (r *Router) Refresh(ctx context.Context) error {
	snapshot, err := r.source.RoutingSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("err in r.source.RoutingSnapshot: %w", err)
	}
	cloned := cloneSnapshot(snapshot)
	r.snapshot = &cloned
	return nil
}

func (r *Router) Snapshot() (coordserver.RoutingSnapshot, bool) {
	if r.snapshot == nil {
		return coordserver.RoutingSnapshot{}, false
	}
	return cloneSnapshot(*r.snapshot), true
}

func (r *Router) Get(ctx context.Context, key string) (storage.ReadResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.ReadResult{}, err
	}
	return r.getWithSnapshot(ctx, key, snapshot, true)
}

func (r *Router) Put(ctx context.Context, key string, value string) (storage.CommitResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.CommitResult{}, err
	}
	return r.putWithSnapshot(ctx, key, value, snapshot, true)
}

func (r *Router) Delete(ctx context.Context, key string) (storage.CommitResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.CommitResult{}, err
	}
	return r.deleteWithSnapshot(ctx, key, snapshot, true)
}

func (r *Router) loadedSnapshot() (coordserver.RoutingSnapshot, error) {
	if r.snapshot == nil {
		return coordserver.RoutingSnapshot{}, ErrSnapshotNotLoaded
	}
	return cloneSnapshot(*r.snapshot), nil
}

func (r *Router) getWithSnapshot(
	ctx context.Context,
	key string,
	snapshot coordserver.RoutingSnapshot,
	allowRefresh bool,
) (storage.ReadResult, error) {
	route, err := routeForKey(snapshot, key)
	if err != nil {
		return storage.ReadResult{}, err
	}
	if !route.Readable || route.TailNodeID == "" {
		return storage.ReadResult{}, fmt.Errorf("%w: slot %d is not readable", ErrNoRoute, route.Slot)
	}
	result, err := r.transport.Get(ctx, route.TailNodeID, storage.ClientGetRequest{
		Slot:                 route.Slot,
		Key:                  key,
		ExpectedChainVersion: route.ChainVersion,
	})
	if err != nil {
		if allowRefresh && isRoutingMismatch(err) {
			if refreshErr := r.Refresh(ctx); refreshErr != nil {
				return storage.ReadResult{}, refreshErr
			}
			refreshed, _ := r.Snapshot()
			return r.getWithSnapshot(ctx, key, refreshed, false)
		}
		return storage.ReadResult{}, err
	}
	return result, nil
}

func (r *Router) putWithSnapshot(
	ctx context.Context,
	key string,
	value string,
	snapshot coordserver.RoutingSnapshot,
	allowRefresh bool,
) (storage.CommitResult, error) {
	route, err := routeForKey(snapshot, key)
	if err != nil {
		return storage.CommitResult{}, err
	}
	if !route.Writable || route.HeadNodeID == "" {
		return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", ErrNoRoute, route.Slot)
	}
	result, err := r.transport.Put(ctx, route.HeadNodeID, storage.ClientPutRequest{
		Slot:                 route.Slot,
		Key:                  key,
		Value:                value,
		ExpectedChainVersion: route.ChainVersion,
	})
	if err != nil {
		if allowRefresh && isRoutingMismatch(err) {
			if refreshErr := r.Refresh(ctx); refreshErr != nil {
				return storage.CommitResult{}, refreshErr
			}
			refreshed, _ := r.Snapshot()
			return r.putWithSnapshot(ctx, key, value, refreshed, false)
		}
		return storage.CommitResult{}, err
	}
	return result, nil
}

func (r *Router) deleteWithSnapshot(
	ctx context.Context,
	key string,
	snapshot coordserver.RoutingSnapshot,
	allowRefresh bool,
) (storage.CommitResult, error) {
	route, err := routeForKey(snapshot, key)
	if err != nil {
		return storage.CommitResult{}, err
	}
	if !route.Writable || route.HeadNodeID == "" {
		return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", ErrNoRoute, route.Slot)
	}
	result, err := r.transport.Delete(ctx, route.HeadNodeID, storage.ClientDeleteRequest{
		Slot:                 route.Slot,
		Key:                  key,
		ExpectedChainVersion: route.ChainVersion,
	})
	if err != nil {
		if allowRefresh && isRoutingMismatch(err) {
			if refreshErr := r.Refresh(ctx); refreshErr != nil {
				return storage.CommitResult{}, refreshErr
			}
			refreshed, _ := r.Snapshot()
			return r.deleteWithSnapshot(ctx, key, refreshed, false)
		}
		return storage.CommitResult{}, err
	}
	return result, nil
}

func routeForKey(snapshot coordserver.RoutingSnapshot, key string) (coordserver.SlotRoute, error) {
	if snapshot.SlotCount <= 0 || len(snapshot.Slots) != snapshot.SlotCount {
		return coordserver.SlotRoute{}, fmt.Errorf("%w: invalid snapshot slot count %d", ErrNoRoute, snapshot.SlotCount)
	}
	slot := int(crc32.ChecksumIEEE([]byte(key)) % uint32(snapshot.SlotCount))
	return snapshot.Slots[slot], nil
}

func isRoutingMismatch(err error) bool {
	var mismatch *storage.RoutingMismatchError
	return errors.As(err, &mismatch)
}

func cloneSnapshot(snapshot coordserver.RoutingSnapshot) coordserver.RoutingSnapshot {
	cloned := coordserver.RoutingSnapshot{
		Version:   snapshot.Version,
		SlotCount: snapshot.SlotCount,
		Slots:     append([]coordserver.SlotRoute(nil), snapshot.Slots...),
	}
	return cloned
}
