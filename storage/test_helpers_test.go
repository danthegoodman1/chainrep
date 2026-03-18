package storage

import "time"

func testObjectMetadata(version uint64) ObjectMetadata {
	base := time.Unix(0, 0).UTC()
	return ObjectMetadata{
		Version:   version,
		CreatedAt: base.Add(time.Duration(version) * time.Second),
		UpdatedAt: base.Add(time.Duration(version) * time.Second),
	}
}

func valueSnapshot(entries map[string]string) Snapshot {
	snapshot := make(Snapshot, len(entries))
	for key, value := range entries {
		snapshot[key] = CommittedObject{
			Value:    value,
			Metadata: ObjectMetadata{},
		}
	}
	return snapshot
}

func snapshotValues(snapshot Snapshot) map[string]string {
	values := make(map[string]string, len(snapshot))
	for key, object := range snapshot {
		values[key] = object.Value
	}
	return values
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(delta time.Duration) {
	c.now = c.now.Add(delta)
}
