package runtime

import "context"

type InMemoryStore struct {
	checkpoint *Checkpoint
	wal        []LogRecord
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{}
}

func (s *InMemoryStore) LoadLatestCheckpoint(_ context.Context) (Checkpoint, bool, error) {
	if s.checkpoint == nil {
		return Checkpoint{}, false, nil
	}
	return cloneCheckpoint(*s.checkpoint), true, nil
}

func (s *InMemoryStore) LoadWAL(_ context.Context, afterIndex uint64) ([]LogRecord, error) {
	records := make([]LogRecord, 0, len(s.wal))
	for _, record := range s.wal {
		if record.Index > afterIndex {
			records = append(records, cloneLogRecord(record))
		}
	}
	return records, nil
}

func (s *InMemoryStore) AppendWAL(_ context.Context, record LogRecord) error {
	s.wal = append(s.wal, cloneLogRecord(record))
	return nil
}

func (s *InMemoryStore) SaveCheckpoint(_ context.Context, checkpoint Checkpoint) error {
	cloned := cloneCheckpoint(checkpoint)
	s.checkpoint = &cloned
	return nil
}

func (s *InMemoryStore) TruncateWAL(_ context.Context, throughIndex uint64) error {
	kept := s.wal[:0]
	for _, record := range s.wal {
		if record.Index > throughIndex {
			kept = append(kept, cloneLogRecord(record))
		}
	}
	s.wal = kept
	return nil
}

func cloneCheckpoint(checkpoint Checkpoint) Checkpoint {
	return Checkpoint{State: cloneState(checkpoint.State)}
}

func cloneLogRecord(record LogRecord) LogRecord {
	return LogRecord{
		Index:   record.Index,
		Command: cloneCommand(record.Command),
	}
}
