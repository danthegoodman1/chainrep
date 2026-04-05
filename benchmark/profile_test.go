package benchmark

import "testing"

func TestLoadProfileDefaultsAndValidate(t *testing.T) {
	profile, err := LoadProfile("../profiles/bench/aws_i8ge_steady.yaml")
	if err != nil {
		t.Fatalf("LoadProfile returned error: %v", err)
	}
	if profile.AWS.ClientInstanceType != "c8g.8xlarge" {
		t.Fatalf("ClientInstanceType = %q, want c8g.8xlarge", profile.AWS.ClientInstanceType)
	}
	if profile.Cluster.SlotCount != 1024 {
		t.Fatalf("SlotCount = %d, want 1024", profile.Cluster.SlotCount)
	}
	if len(profile.Workload.Scenarios) != 3 {
		t.Fatalf("len(Scenarios) = %d, want 3", len(profile.Workload.Scenarios))
	}
}
