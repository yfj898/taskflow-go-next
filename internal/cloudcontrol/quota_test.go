package cloudcontrol

import "testing"

func TestDeploymentQuotaDemand(t *testing.T) {
	t.Parallel()
	demand, err := DeploymentQuotaDemand(KubernetesDeploymentSpec{
		Image:    "nginx:1.27",
		Replicas: 3,
		CPU:      "500m",
		Memory:   "256Mi",
	})
	if err != nil {
		t.Fatal(err)
	}
	if demand.CPUMilli != 1500 || demand.MemoryMiB != 768 || demand.Replicas != 3 {
		t.Fatalf("unexpected demand: %+v", demand)
	}
}

func TestQuotaQuantityParsing(t *testing.T) {
	t.Parallel()
	cpuCases := map[string]int64{"": 0, "250m": 250, "1": 1000, "1.25": 1250, "0.0001": 1}
	for raw, want := range cpuCases {
		got, err := ParseCPUMilli(raw)
		if err != nil || got != want {
			t.Fatalf("ParseCPUMilli(%q)=(%d,%v), want %d", raw, got, err, want)
		}
	}
	memoryCases := map[string]int64{"": 0, "128Mi": 128, "1Gi": 1024, "100M": 96, "1048577": 2}
	for raw, want := range memoryCases {
		got, err := ParseMemoryMiB(raw)
		if err != nil || got != want {
			t.Fatalf("ParseMemoryMiB(%q)=(%d,%v), want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"-1", "bad", "10x"} {
		if _, err := ParseCPUMilli(raw); err == nil {
			t.Fatalf("expected invalid cpu %q", raw)
		}
		if _, err := ParseMemoryMiB(raw); err == nil {
			t.Fatalf("expected invalid memory %q", raw)
		}
	}
}
