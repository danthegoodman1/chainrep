package pebble

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	cockroachpebble "github.com/cockroachdb/pebble"

	"github.com/danthegoodman1/chainrep/storage"
)

const (
	keyReplicaMeta   byte = 'm'
	keyCommittedData byte = 'c'
	keyStagedOp      byte = 's'
	keyLocalReplica  byte = 'l'
)

const (
	opCreateReplica               = "create_replica"
	opDeleteReplica               = "delete_replica"
	opInstallSnapshot             = "install_snapshot"
	opSetHighestCommittedSequence = "set_highest_committed_sequence"
	opCommitSequence              = "commit_sequence"
	opUpsertLocalReplica          = "upsert_local_replica"
	opDeleteLocalReplica          = "delete_local_replica"
	opCleanupStagedOnOpen         = "cleanup_staged_on_open"
)

type Store struct {
	owner *owner
}

type owner struct {
	db        *cockroachpebble.DB
	closeErr  error
	closeOnce sync.Once
}

type durableWriteHooks struct {
	mu                 sync.Mutex
	beforeDurableWrite func(op string) error
}

var testHooks durableWriteHooks

type Backend struct {
	owner *owner
}

type LocalStore struct {
	owner *owner
}

type stagedValue struct {
	Kind     storage.OperationKind `json:"kind"`
	Key      string                `json:"key"`
	Value    string                `json:"value"`
	Metadata storage.ObjectMetadata `json:"metadata"`
}

func Open(path string) (*Store, error) {
	db, err := cockroachpebble.Open(path, &cockroachpebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("err in pebble.Open: %w", err)
	}
	owner := &owner{db: db}
	if err := owner.cleanupStagedOperationsOnOpen(); err != nil {
		closeErr := owner.Close()
		if closeErr != nil {
			return nil, errors.Join(fmt.Errorf("err in cleanupStagedOperationsOnOpen: %w", err), closeErr)
		}
		return nil, fmt.Errorf("err in cleanupStagedOperationsOnOpen: %w", err)
	}
	return &Store{owner: owner}, nil
}

func (s *Store) Backend() storage.Backend {
	return &Backend{owner: s.owner}
}

func (s *Store) LocalStateStore() storage.LocalStateStore {
	return &LocalStore{owner: s.owner}
}

func (s *Store) Close() error {
	return s.owner.Close()
}

func (o *owner) Close() error {
	o.closeOnce.Do(func() {
		o.closeErr = o.db.Close()
		if o.closeErr != nil {
			o.closeErr = fmt.Errorf("err in o.db.Close: %w", o.closeErr)
		}
	})
	return o.closeErr
}

func (b *Backend) Close() error {
	return b.owner.Close()
}

func (l *LocalStore) Close() error {
	return l.owner.Close()
}

func (b *Backend) CreateReplica(slot int) error {
	if _, err := b.HighestCommittedSequence(slot); err == nil {
		return fmt.Errorf("%w: slot %d", storage.ErrReplicaExists, slot)
	} else if !errors.Is(err, storage.ErrUnknownReplica) {
		return err
	}
	batch := b.owner.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(replicaMetaKey(slot), encodeUint64(0), nil); err != nil {
		return fmt.Errorf("err in batch.Set(replica meta): %w", err)
	}
	if err := b.owner.commitBatch(batch, opCreateReplica); err != nil {
		return fmt.Errorf("err in owner.commitBatch(create replica): %w", err)
	}
	return nil
}

func (b *Backend) DeleteReplica(slot int) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	batch := b.owner.db.NewBatch()
	defer batch.Close()
	if err := deletePrefix(b.owner.db, batch, committedPrefix(slot)); err != nil {
		return err
	}
	if err := deletePrefix(b.owner.db, batch, stagedPrefix(slot)); err != nil {
		return err
	}
	if err := batch.Delete(replicaMetaKey(slot), nil); err != nil {
		return fmt.Errorf("err in batch.Delete(replica meta): %w", err)
	}
	if err := b.owner.commitBatch(batch, opDeleteReplica); err != nil {
		return fmt.Errorf("err in owner.commitBatch(delete replica): %w", err)
	}
	return nil
}

func (b *Backend) Snapshot(slot int) (storage.Snapshot, error) {
	return b.CommittedSnapshot(slot)
}

func (b *Backend) InstallSnapshot(slot int, snap storage.Snapshot) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	batch := b.owner.db.NewBatch()
	defer batch.Close()
	if err := deletePrefix(b.owner.db, batch, committedPrefix(slot)); err != nil {
		return err
	}
	if err := deletePrefix(b.owner.db, batch, stagedPrefix(slot)); err != nil {
		return err
	}
	keys := sortedSnapshotKeys(snap)
	for _, key := range keys {
		encoded, err := json.Marshal(snap[key])
		if err != nil {
			return fmt.Errorf("err in json.Marshal(committed snapshot): %w", err)
		}
		if err := batch.Set(committedKey(slot, key), encoded, nil); err != nil {
			return fmt.Errorf("err in batch.Set(committed snapshot): %w", err)
		}
	}
	if err := b.owner.commitBatch(batch, opInstallSnapshot); err != nil {
		return fmt.Errorf("err in owner.commitBatch(install snapshot): %w", err)
	}
	return nil
}

func (b *Backend) SetHighestCommittedSequence(slot int, sequence uint64) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	batch := b.owner.db.NewBatch()
	defer batch.Close()
	if err := deletePrefix(b.owner.db, batch, stagedPrefix(slot)); err != nil {
		return err
	}
	if err := batch.Set(replicaMetaKey(slot), encodeUint64(sequence), nil); err != nil {
		return fmt.Errorf("err in batch.Set(replica meta): %w", err)
	}
	if err := b.owner.commitBatch(batch, opSetHighestCommittedSequence); err != nil {
		return fmt.Errorf("err in owner.commitBatch(set highest committed sequence): %w", err)
	}
	return nil
}

func (b *Backend) StagePut(slot int, sequence uint64, key string, value string, metadata storage.ObjectMetadata) error {
	return b.stageOperation(slot, sequence, stagedValue{
		Kind:     storage.OperationKindPut,
		Key:      key,
		Value:    value,
		Metadata: metadata,
	})
}

func (b *Backend) StageDelete(slot int, sequence uint64, key string, metadata storage.ObjectMetadata) error {
	return b.stageOperation(slot, sequence, stagedValue{
		Kind:     storage.OperationKindDelete,
		Key:      key,
		Metadata: metadata,
	})
}

func (b *Backend) CommitSequence(slot int, sequence uint64) error {
	highestCommitted, err := b.HighestCommittedSequence(slot)
	if err != nil {
		return err
	}
	if sequence != highestCommitted+1 {
		return fmt.Errorf(
			"%w: slot %d expected commit sequence %d, got %d",
			storage.ErrSequenceMismatch,
			slot,
			highestCommitted+1,
			sequence,
		)
	}
	operation, err := b.loadStagedOperation(slot, sequence)
	if err != nil {
		return err
	}

	batch := b.owner.db.NewBatch()
	defer batch.Close()
	switch operation.Kind {
	case storage.OperationKindPut:
		encoded, err := json.Marshal(storage.CommittedObject{
			Value:    operation.Value,
			Metadata: operation.Metadata,
		})
		if err != nil {
			return fmt.Errorf("err in json.Marshal(committed put): %w", err)
		}
		if err := batch.Set(committedKey(slot, operation.Key), encoded, nil); err != nil {
			return fmt.Errorf("err in batch.Set(committed put): %w", err)
		}
	case storage.OperationKindDelete:
		if err := batch.Delete(committedKey(slot, operation.Key), nil); err != nil && !errors.Is(err, cockroachpebble.ErrNotFound) {
			return fmt.Errorf("err in batch.Delete(committed delete): %w", err)
		}
	default:
		return fmt.Errorf("%w: unsupported operation kind %q", storage.ErrInvalidConfig, operation.Kind)
	}
	if err := batch.Delete(stagedKey(slot, sequence), nil); err != nil {
		return fmt.Errorf("err in batch.Delete(staged op): %w", err)
	}
	if err := batch.Set(replicaMetaKey(slot), encodeUint64(sequence), nil); err != nil {
		return fmt.Errorf("err in batch.Set(replica meta): %w", err)
	}
	if err := b.owner.commitBatch(batch, opCommitSequence); err != nil {
		return fmt.Errorf("err in owner.commitBatch(commit sequence): %w", err)
	}
	return nil
}

func (b *Backend) CommittedSnapshot(slot int) (storage.Snapshot, error) {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return nil, err
	}
	prefix := committedPrefix(slot)
	iter, err := b.owner.db.NewIter(&cockroachpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("err in db.NewIter(committed): %w", err)
	}
	defer iter.Close()
	snapshot := storage.Snapshot{}
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key()[len(prefix):])
		var object storage.CommittedObject
		if err := json.Unmarshal(iter.Value(), &object); err != nil {
			return nil, fmt.Errorf("err in json.Unmarshal(committed snapshot): %w", err)
		}
		snapshot[key] = object
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("err in iter.Error(committed): %w", err)
	}
	return snapshot, nil
}

func (b *Backend) GetCommitted(slot int, key string) (storage.CommittedObject, bool, error) {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return storage.CommittedObject{}, false, err
	}
	value, closer, err := b.owner.db.Get(committedKey(slot, key))
	if errors.Is(err, cockroachpebble.ErrNotFound) {
		return storage.CommittedObject{}, false, nil
	}
	if err != nil {
		return storage.CommittedObject{}, false, fmt.Errorf("err in db.Get(committed): %w", err)
	}
	defer closer.Close()
	var object storage.CommittedObject
	if err := json.Unmarshal(slices.Clone(value), &object); err != nil {
		return storage.CommittedObject{}, false, fmt.Errorf("err in json.Unmarshal(committed): %w", err)
	}
	return object, true, nil
}

func (b *Backend) HighestCommittedSequence(slot int) (uint64, error) {
	value, closer, err := b.owner.db.Get(replicaMetaKey(slot))
	if errors.Is(err, cockroachpebble.ErrNotFound) {
		return 0, fmt.Errorf("%w: slot %d", storage.ErrUnknownReplica, slot)
	}
	if err != nil {
		return 0, fmt.Errorf("err in db.Get(replica meta): %w", err)
	}
	defer closer.Close()
	return decodeUint64(value)
}

func (b *Backend) StagedSequences(slot int) ([]uint64, error) {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return nil, err
	}
	prefix := stagedPrefix(slot)
	iter, err := b.owner.db.NewIter(&cockroachpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, fmt.Errorf("err in db.NewIter(staged): %w", err)
	}
	defer iter.Close()
	sequences := make([]uint64, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		sequence, err := decodeSequence(iter.Key()[len(prefix):])
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, sequence)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("err in iter.Error(staged): %w", err)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (l *LocalStore) LoadNode(_ context.Context, nodeID string) (storage.PersistedNodeState, error) {
	prefix := localReplicaPrefix(nodeID)
	iter, err := l.owner.db.NewIter(&cockroachpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return storage.PersistedNodeState{}, fmt.Errorf("err in db.NewIter(local): %w", err)
	}
	defer iter.Close()
	state := storage.PersistedNodeState{
		NodeID:   nodeID,
		Replicas: make([]storage.PersistedReplica, 0),
	}
	for iter.First(); iter.Valid(); iter.Next() {
		var replica storage.PersistedReplica
		if err := json.Unmarshal(iter.Value(), &replica); err != nil {
			return storage.PersistedNodeState{}, fmt.Errorf("err in json.Unmarshal(local replica): %w", err)
		}
		state.Replicas = append(state.Replicas, replica)
	}
	if err := iter.Error(); err != nil {
		return storage.PersistedNodeState{}, fmt.Errorf("err in iter.Error(local): %w", err)
	}
	sort.Slice(state.Replicas, func(i, j int) bool {
		return state.Replicas[i].Assignment.Slot < state.Replicas[j].Assignment.Slot
	})
	return state, nil
}

func (l *LocalStore) UpsertReplica(_ context.Context, nodeID string, replica storage.PersistedReplica) error {
	encoded, err := json.Marshal(replica)
	if err != nil {
		return fmt.Errorf("err in json.Marshal(local replica): %w", err)
	}
	if err := l.owner.setSync(localReplicaKey(nodeID, replica.Assignment.Slot), encoded, opUpsertLocalReplica); err != nil {
		return fmt.Errorf("err in owner.setSync(local replica): %w", err)
	}
	return nil
}

func (l *LocalStore) DeleteReplica(_ context.Context, nodeID string, slot int) error {
	if err := l.owner.deleteSync(localReplicaKey(nodeID, slot), opDeleteLocalReplica); err != nil && !errors.Is(err, cockroachpebble.ErrNotFound) {
		return fmt.Errorf("err in owner.deleteSync(local replica): %w", err)
	}
	return nil
}

func (b *Backend) stageOperation(slot int, sequence uint64, operation stagedValue) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	if _, err := b.loadStagedOperation(slot, sequence); err == nil {
		return fmt.Errorf("%w: slot %d sequence %d already staged", storage.ErrSequenceMismatch, slot, sequence)
	} else if !errors.Is(err, storage.ErrSequenceMismatch) {
		return err
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("err in json.Marshal(staged op): %w", err)
	}
	if err := b.owner.setSync(stagedKey(slot, sequence), encoded, "stage_operation"); err != nil {
		return fmt.Errorf("err in owner.setSync(staged op): %w", err)
	}
	return nil
}

func (b *Backend) loadStagedOperation(slot int, sequence uint64) (stagedValue, error) {
	value, closer, err := b.owner.db.Get(stagedKey(slot, sequence))
	if errors.Is(err, cockroachpebble.ErrNotFound) {
		return stagedValue{}, fmt.Errorf("%w: slot %d sequence %d is not staged", storage.ErrSequenceMismatch, slot, sequence)
	}
	if err != nil {
		return stagedValue{}, fmt.Errorf("err in db.Get(staged op): %w", err)
	}
	defer closer.Close()
	var operation stagedValue
	if err := json.Unmarshal(value, &operation); err != nil {
		return stagedValue{}, fmt.Errorf("err in json.Unmarshal(staged op): %w", err)
	}
	return operation, nil
}

func deletePrefix(db *cockroachpebble.DB, batch *cockroachpebble.Batch, prefix []byte) error {
	iter, err := db.NewIter(&cockroachpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return fmt.Errorf("err in batch.NewIter(prefix delete): %w", err)
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(slices.Clone(iter.Key()), nil); err != nil {
			return fmt.Errorf("err in batch.Delete(prefix entry): %w", err)
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("err in iter.Error(prefix delete): %w", err)
	}
	return nil
}

func (o *owner) cleanupStagedOperationsOnOpen() error {
	prefix := []byte{keyStagedOp}
	iter, err := o.db.NewIter(&cockroachpebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return fmt.Errorf("err in db.NewIter(staged cleanup): %w", err)
	}
	defer iter.Close()

	batch := o.db.NewBatch()
	defer batch.Close()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(slices.Clone(iter.Key()), nil); err != nil {
			return fmt.Errorf("err in batch.Delete(staged cleanup): %w", err)
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("err in iter.Error(staged cleanup): %w", err)
	}
	if count == 0 {
		return nil
	}
	if err := o.commitBatch(batch, opCleanupStagedOnOpen); err != nil {
		return fmt.Errorf("err in owner.commitBatch(staged cleanup): %w", err)
	}
	return nil
}

func (o *owner) commitBatch(batch *cockroachpebble.Batch, op string) error {
	if err := runBeforeDurableWrite(op); err != nil {
		return err
	}
	if err := batch.Commit(cockroachpebble.Sync); err != nil {
		return err
	}
	return nil
}

func (o *owner) setSync(key []byte, value []byte, op string) error {
	if err := runBeforeDurableWrite(op); err != nil {
		return err
	}
	if err := o.db.Set(key, value, cockroachpebble.Sync); err != nil {
		return err
	}
	return nil
}

func (o *owner) deleteSync(key []byte, op string) error {
	if err := runBeforeDurableWrite(op); err != nil {
		return err
	}
	if err := o.db.Delete(key, cockroachpebble.Sync); err != nil {
		return err
	}
	return nil
}

func runBeforeDurableWrite(op string) error {
	testHooks.mu.Lock()
	hook := testHooks.beforeDurableWrite
	testHooks.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(op)
}

func setBeforeDurableWriteForTest(hook func(op string) error) func() {
	testHooks.mu.Lock()
	prev := testHooks.beforeDurableWrite
	testHooks.beforeDurableWrite = hook
	testHooks.mu.Unlock()
	return func() {
		testHooks.mu.Lock()
		testHooks.beforeDurableWrite = prev
		testHooks.mu.Unlock()
	}
}

func replicaMetaKey(slot int) []byte {
	return append([]byte{keyReplicaMeta}, encodeSlot(slot)...)
}

func committedPrefix(slot int) []byte {
	return append([]byte{keyCommittedData}, encodeSlot(slot)...)
}

func committedKey(slot int, key string) []byte {
	return append(committedPrefix(slot), []byte(key)...)
}

func stagedPrefix(slot int) []byte {
	return append([]byte{keyStagedOp}, encodeSlot(slot)...)
}

func stagedKey(slot int, sequence uint64) []byte {
	return append(stagedPrefix(slot), encodeUint64(sequence)...)
}

func localReplicaPrefix(nodeID string) []byte {
	prefix := []byte{keyLocalReplica}
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(nodeID)))
	prefix = append(prefix, length...)
	prefix = append(prefix, []byte(nodeID)...)
	return prefix
}

func localReplicaKey(nodeID string, slot int) []byte {
	return append(localReplicaPrefix(nodeID), encodeSlot(slot)...)
}

func encodeSlot(slot int) []byte {
	return encodeUint64(uint64(slot))
}

func encodeUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func decodeUint64(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("%w: invalid uint64 length %d", storage.ErrStateMismatch, len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

func decodeSequence(value []byte) (uint64, error) {
	return decodeUint64(value)
}

func prefixUpperBound(prefix []byte) []byte {
	upper := slices.Clone(prefix)
	for index := len(upper) - 1; index >= 0; index-- {
		if upper[index] == 0xFF {
			continue
		}
		upper[index]++
		return upper[:index+1]
	}
	return nil
}

func sortedSnapshotKeys(snapshot storage.Snapshot) []string {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
