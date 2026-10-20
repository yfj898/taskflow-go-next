package queue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type CompactionPolicy struct {
	Retention          time.Duration
	MaxRetainedEntries int64
}

type CompactionResult struct {
	TrimmedEntries int64
	CutoffID       string
	ProtectedID    string
	Protected      bool
}

type Compactor interface {
	Compact(context.Context, time.Time, CompactionPolicy) (CompactionResult, error)
}

var safeStreamTrimScript = redis.NewScript(`
local function compare_ids(left, right)
  local left_dash = string.find(left, '-', 1, true)
  local right_dash = string.find(right, '-', 1, true)
  if not left_dash or not right_dash then return 0 end
  local left_ms = tonumber(string.sub(left, 1, left_dash - 1)) or 0
  local left_seq = tonumber(string.sub(left, left_dash + 1)) or 0
  local right_ms = tonumber(string.sub(right, 1, right_dash - 1)) or 0
  local right_seq = tonumber(string.sub(right, right_dash + 1)) or 0
  if left_ms < right_ms then return -1 end
  if left_ms > right_ms then return 1 end
  if left_seq < right_seq then return -1 end
  if left_seq > right_seq then return 1 end
  return 0
end

local desired = ARGV[1]
local protected = ''
local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
for _, group in ipairs(groups) do
  local name = nil
  local last_delivered = '0-0'
  local pending_count = 0
  for i = 1, #group, 2 do
    local field = group[i]
    local value = group[i + 1]
    if field == 'name' then
      name = value
    elseif field == 'last-delivered-id' then
      last_delivered = value
    elseif field == 'pending' then
      pending_count = tonumber(value) or 0
    end
  end
  local group_boundary = last_delivered
  if pending_count > 0 and name then
    local pending = redis.call('XPENDING', KEYS[1], name)
    local lower = pending[2]
    if lower and compare_ids(lower, group_boundary) < 0 then
      group_boundary = lower
    end
  end
  if protected == '' or compare_ids(group_boundary, protected) < 0 then
    protected = group_boundary
  end
end

local cutoff = desired
local constrained = 0
if protected ~= '' and compare_ids(cutoff, protected) > 0 then
  cutoff = protected
  constrained = 1
end
if cutoff == '' or cutoff == '0-0' then
  return {0, cutoff, protected, constrained}
end
local trimmed = redis.call('XTRIM', KEYS[1], 'MINID', cutoff)
return {trimmed, cutoff, protected, constrained}
`)

// Compact trims only a prefix that every consumer group has already delivered
// and that contains no pending entry. The consumer-group snapshot, PEL
// inspection, and XTRIM run atomically inside Redis so a concurrently-created
// consumer group cannot appear between the safety check and the trim.
//
// This is deliberately stricter than a plain XTRIM MAXLEN: deleting a pending
// entry's payload would break XAUTOCLAIM crash recovery even if the PEL
// metadata itself still existed.
func (q *RedisTaskQueue) Compact(ctx context.Context, now time.Time, policy CompactionPolicy) (CompactionResult, error) {
	if policy.Retention <= 0 && policy.MaxRetainedEntries <= 0 {
		return CompactionResult{}, nil
	}
	if err := q.ensureGroup(ctx); err != nil {
		return CompactionResult{}, err
	}

	desired, err := q.desiredCompactionCutoff(ctx, now, policy)
	if err != nil || desired == "" || desired == "0-0" {
		return CompactionResult{}, err
	}

	raw, err := safeStreamTrimScript.Run(ctx, q.client, []string{q.key}, desired).Result()
	if err != nil {
		return CompactionResult{}, fmt.Errorf("safe xtrim minid %s: %w", desired, err)
	}
	values, ok := raw.([]interface{})
	if !ok || len(values) != 4 {
		return CompactionResult{}, fmt.Errorf("unexpected safe xtrim response: %#v", raw)
	}

	trimmed, err := redisResultInt64(values[0])
	if err != nil {
		return CompactionResult{}, fmt.Errorf("parse safe xtrim count: %w", err)
	}
	constrained, err := redisResultInt64(values[3])
	if err != nil {
		return CompactionResult{}, fmt.Errorf("parse safe xtrim protected flag: %w", err)
	}

	return CompactionResult{
		TrimmedEntries: trimmed,
		CutoffID:       fmt.Sprint(values[1]),
		ProtectedID:    fmt.Sprint(values[2]),
		Protected:      constrained == 1,
	}, nil
}

func (q *RedisTaskQueue) desiredCompactionCutoff(ctx context.Context, now time.Time, policy CompactionPolicy) (string, error) {
	desired := ""
	if policy.Retention > 0 {
		cutoffMS := now.Add(-policy.Retention).UnixMilli()
		if cutoffMS > 0 {
			desired = fmt.Sprintf("%d-0", cutoffMS)
		}
	}

	if policy.MaxRetainedEntries > 0 {
		messages, err := q.client.XRevRangeN(ctx, q.key, "+", "-", policy.MaxRetainedEntries).Result()
		if err != nil {
			return "", fmt.Errorf("xrevrange compaction cutoff: %w", err)
		}
		if int64(len(messages)) == policy.MaxRetainedEntries && len(messages) > 0 {
			countCutoff := messages[len(messages)-1].ID
			if desired == "" || compareStreamIDs(countCutoff, desired) > 0 {
				desired = countCutoff
			}
		}
	}
	return desired, nil
}

func compareStreamIDs(left, right string) int {
	leftMS, leftSeq := parseStreamID(left)
	rightMS, rightSeq := parseStreamID(right)
	if leftMS < rightMS {
		return -1
	}
	if leftMS > rightMS {
		return 1
	}
	if leftSeq < rightSeq {
		return -1
	}
	if leftSeq > rightSeq {
		return 1
	}
	return 0
}

func parseStreamID(id string) (uint64, uint64) {
	msRaw, seqRaw, ok := strings.Cut(id, "-")
	if !ok {
		return 0, 0
	}
	ms, err := strconv.ParseUint(msRaw, 10, 64)
	if err != nil {
		return 0, 0
	}
	seq, err := strconv.ParseUint(seqRaw, 10, 64)
	if err != nil {
		return 0, 0
	}
	return ms, seq
}

func redisResultInt64(value interface{}) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return strconv.ParseInt(fmt.Sprint(value), 10, 64)
	}
}
