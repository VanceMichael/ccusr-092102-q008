package transit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Store 是只增不改的事件存储:每条事件先落盘(fsync)再应用到内存投影,
// 重启时按序重放即可恢复全部在途状态。幂等键索引同样由重放重建。
type Store struct {
	mu     sync.RWMutex
	path   string
	file   *os.File
	events []Event
	byID   map[string]int // event id -> events 下标
	byKey  map[string]int // idempotency key -> events 下标
	proj   *projection
	th     Thresholds
	now    func() time.Time
}

// Open 打开(必要时创建)事件日志并重放历史事件。
func Open(path string, th Thresholds, now func() time.Time) (*Store, error) {
	if now == nil {
		now = time.Now
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	s := &Store{
		path:  path,
		byID:  map[string]int{},
		byKey: map[string]int{},
		proj:  newProjection(),
		th:    th,
		now:   now,
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 0, 1<<20), 1<<22)
		for sc.Scan() {
			line := bytes.TrimSpace(sc.Bytes())
			if len(line) == 0 {
				continue
			}
			var ev Event
			if err := json.Unmarshal(line, &ev); err != nil {
				return nil, fmt.Errorf("重放事件日志 %s 失败: %w", path, err)
			}
			if err := s.applyLocked(ev); err != nil {
				return nil, fmt.Errorf("重放事件 %s 失败: %w", ev.ID, err)
			}
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
	case os.IsNotExist(err):
	default:
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.file = f
	return s, nil
}

// Close 关闭底层日志文件。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

// applyLocked 索引并投影一条事件,调用方须持有写锁(或处于 Open 重放阶段)。
func (s *Store) applyLocked(ev Event) error {
	if err := s.proj.apply(ev); err != nil {
		return err
	}
	s.byID[ev.ID] = len(s.events)
	if ev.IdempotencyKey != "" {
		s.byKey[ev.IdempotencyKey] = len(s.events)
	}
	s.events = append(s.events, ev)
	return nil
}

// Append 追加一条事件。若幂等键已存在,直接返回原事件并标记 duplicate,
// 不落盘、不改变任何状态——离线补传与重复回调因此不会二次生效。
func (s *Store) Append(ev Event) (stored Event, duplicate bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.IdempotencyKey != "" {
		if idx, ok := s.byKey[ev.IdempotencyKey]; ok {
			return s.events[idx], true, nil
		}
	}
	ev.Seq = int64(len(s.events)) + 1
	if ev.ID == "" {
		ev.ID = fmt.Sprintf("ev-%08d", ev.Seq)
	}
	if ev.RecordedAt.IsZero() {
		ev.RecordedAt = s.now()
	}
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = ev.RecordedAt
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return Event{}, false, err
	}
	if _, err := s.file.Write(append(raw, '\n')); err != nil {
		return Event{}, false, err
	}
	if err := s.file.Sync(); err != nil {
		return Event{}, false, err
	}
	if err := s.applyLocked(ev); err != nil {
		return Event{}, false, err
	}
	return ev, false, nil
}

// Now 返回服务当前时间(可注入,便于测试)。
func (s *Store) Now() time.Time { return s.now() }

// Thresholds 返回生效的阈值配置。
func (s *Store) Thresholds() Thresholds { return s.th }

// EventByID 按事件 ID 查询。
func (s *Store) EventByID(id string) (Event, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	idx, ok := s.byID[id]
	if !ok {
		return Event{}, false
	}
	return s.events[idx], true
}

// LotEvents 返回与某批次相关的全部事件(按落盘顺序)。
func (s *Store) LotEvents(lotID string) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.proj.lotEvents[lotID]
	out := make([]Event, 0, len(ids))
	for _, id := range ids {
		if idx, ok := s.byID[id]; ok {
			out = append(out, s.events[idx])
		}
	}
	return out
}

// --- 投影只读访问(均返回拷贝,调用方可安全持有) ---

// Lot 按批次号查询。
func (s *Store) Lot(id string) (*LotView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.proj.lots[id]
	if !ok {
		return nil, false
	}
	return v.clone(), true
}

// LotByBox 按箱码查询其当前所属批次。
func (s *Store) LotByBox(code string) (*LotView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	lotID, ok := s.proj.boxes[code]
	if !ok {
		return nil, false
	}
	return s.proj.lots[lotID].clone(), true
}

// HasBox 报告箱码是否已登记。
func (s *Store) HasBox(code string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.proj.boxes[code]
	return ok
}

// Lots 返回全部批次视图。
func (s *Store) Lots() []*LotView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*LotView, 0, len(s.proj.lots))
	for _, v := range s.proj.lots {
		out = append(out, v.clone())
	}
	return out
}

// Batch 查询捕捞批次。
func (s *Store) Batch(id string) (*CatchBatchView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.proj.batches[id]
	if !ok {
		return nil, false
	}
	c := *v
	return &c, true
}

// Order 查询客户订单。
func (s *Store) Order(id string) (*OrderView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.proj.orders[id]
	if !ok {
		return nil, false
	}
	c := *v
	return &c, true
}

// Orders 返回全部订单。
func (s *Store) Orders() []*OrderView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*OrderView, 0, len(s.proj.orders))
	for _, v := range s.proj.orders {
		c := *v
		out = append(out, &c)
	}
	return out
}

// Flight 查询航班。
func (s *Store) Flight(id string) (*FlightView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.proj.flights[id]
	if !ok {
		return nil, false
	}
	c := *v
	return &c, true
}

// Flights 返回全部航班。
func (s *Store) Flights() []*FlightView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*FlightView, 0, len(s.proj.flights))
	for _, v := range s.proj.flights {
		c := *v
		out = append(out, &c)
	}
	return out
}
