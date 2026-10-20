package cloudcontrol

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type QuotaDemand struct {
	CPUMilli  int64 `json:"cpu_milli"`
	MemoryMiB int64 `json:"memory_mib"`
	Replicas  int64 `json:"replicas"`
}

func (q QuotaDemand) NonNegative() bool {
	return q.CPUMilli >= 0 && q.MemoryMiB >= 0 && q.Replicas >= 0
}

func DeploymentQuotaDemand(spec KubernetesDeploymentSpec) (QuotaDemand, error) {
	if spec.Replicas < 0 {
		return QuotaDemand{}, errors.New("replicas must be non-negative")
	}
	cpuPerReplica, err := ParseCPUMilli(spec.CPU)
	if err != nil {
		return QuotaDemand{}, err
	}
	memoryPerReplica, err := ParseMemoryMiB(spec.Memory)
	if err != nil {
		return QuotaDemand{}, err
	}
	replicas := int64(spec.Replicas)
	cpu, ok := safeMultiply(cpuPerReplica, replicas)
	if !ok {
		return QuotaDemand{}, errors.New("cpu quota demand overflows int64")
	}
	memory, ok := safeMultiply(memoryPerReplica, replicas)
	if !ok {
		return QuotaDemand{}, errors.New("memory quota demand overflows int64")
	}
	return QuotaDemand{CPUMilli: cpu, MemoryMiB: memory, Replicas: replicas}, nil
}

// ParseCPUMilli supports the common Kubernetes CPU forms used by this
// project, e.g. 500m, 1, 1.5. Fractional cores are rounded up to one millicore
// so admission never undercounts requested CPU.
func ParseCPUMilli(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	if strings.HasSuffix(raw, "m") {
		value, err := strconv.ParseInt(strings.TrimSuffix(raw, "m"), 10, 64)
		if err != nil || value < 0 {
			return 0, fmt.Errorf("invalid cpu quantity %q", raw)
		}
		return value, nil
	}
	cores, err := strconv.ParseFloat(raw, 64)
	if err != nil || cores < 0 || math.IsInf(cores, 0) || math.IsNaN(cores) {
		return 0, fmt.Errorf("invalid cpu quantity %q", raw)
	}
	milli := math.Ceil(cores * 1000)
	if milli > math.MaxInt64 {
		return 0, fmt.Errorf("cpu quantity %q is too large", raw)
	}
	return int64(milli), nil
}

// ParseMemoryMiB accepts binary Kubernetes-style suffixes (Ki/Mi/Gi/Ti) and
// decimal SI suffixes (K/M/G/T). Values are rounded up to a whole MiB to avoid
// admitting more memory than the quota ledger accounts for.
func ParseMemoryMiB(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	type unit struct {
		suffix string
		bytes  float64
	}
	units := []unit{
		{"Ti", 1 << 40},
		{"Gi", 1 << 30},
		{"Mi", 1 << 20},
		{"Ki", 1 << 10},
		{"T", 1e12},
		{"G", 1e9},
		{"M", 1e6},
		{"K", 1e3},
	}
	multiplier := float64(1)
	number := raw
	for _, candidate := range units {
		if strings.HasSuffix(raw, candidate.suffix) {
			multiplier = candidate.bytes
			number = strings.TrimSuffix(raw, candidate.suffix)
			break
		}
	}
	value, err := strconv.ParseFloat(number, 64)
	if err != nil || value < 0 || math.IsInf(value, 0) || math.IsNaN(value) {
		return 0, fmt.Errorf("invalid memory quantity %q", raw)
	}
	mib := math.Ceil(value * multiplier / float64(1<<20))
	if mib > math.MaxInt64 {
		return 0, fmt.Errorf("memory quantity %q is too large", raw)
	}
	return int64(mib), nil
}

func safeMultiply(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > math.MaxInt64/b {
		return 0, false
	}
	return a * b, true
}
