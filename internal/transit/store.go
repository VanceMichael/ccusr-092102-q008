package transit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store 是只追加的 JSONL 事件日志。每次写入后 fsync,
// 服务重启时从头回放即可恢复全部在途状态。
type Store struct {
	path string
	mu   sync.Mutex
	f    *os.File
	seq  uint64
	keys map[string]uint64 // 幂等键 → 序号
}

// OpenStore 打开(必要时创建)事件日志,并返回全部历史事件供回放。
func OpenStore(path string) (*Store, []Event, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, nil, err
		}
	}
	var events []Event
	keys := make(map[string]uint64)
	var maxSeq uint64

	if data, err := os.ReadFile(path); err == nil {
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 1<<20), 8<<20)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var e Event
			if err := json.Unmarshal(line, &e); err != nil {
				return nil, nil, fmt.Errorf("事件日志损坏,第 %d 条无法解析: %w", len(events)+1, err)
			}
			events = append(events, e)
			keys[e.IdempotencyKey] = e.Seq
			if e.Seq > maxSeq {
				maxSeq = e.Seq
			}
		}
		if err := sc.Err(); err != nil {
			return nil, nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	return &Store{path: path, f: f, seq: maxSeq, keys: keys}, events, nil
}

// Append 追加一条事件。若幂等键已存在(离线补传/重复回调),
// 返回 duplicate=true 且不重复写入。
func (s *Store) Append(e *Event) (seq uint64, duplicate bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e.IdempotencyKey == "" {
		e.IdempotencyKey = e.Source + "|" + e.ID
	}
	if seq, ok := s.keys[e.IdempotencyKey]; ok {
		return seq, true, nil
	}
	s.seq++
	e.Seq = s.seq
	if e.RecordedAt.IsZero() {
		e.RecordedAt = time.Now().UTC()
	}
	data, err := json.Marshal(e)
	if err != nil {
		return 0, false, err
	}
	if _, err := s.f.Write(append(data, '\n')); err != nil {
		return 0, false, err
	}
	if err := s.f.Sync(); err != nil {
		return 0, false, err
	}
	s.keys[e.IdempotencyKey] = e.Seq
	return e.Seq, false, nil
}

// Has 报告幂等键是否已存在,用于命令预检(重试直接返回重复)。
func (s *Store) Has(key string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq, ok := s.keys[key]
	return seq, ok
}

// Close 关闭日志文件。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}

// Path 返回日志文件路径。
func (s *Store) Path() string { return s.path }
