package transit

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Service 是命令入口:校验 → 追加事件(幂等)→ 应用投影 → 规则引擎派生事件。
// 所有命令在单把互斥锁下串行执行,派生事件与触发事件处于同一临界区。
type Service struct {
	mu    sync.Mutex
	store *Store
	l     *Ledger
	now   func() time.Time
}

// NewService 创建服务并回放历史事件恢复在途状态。
func NewService(store *Store, events []Event, now func() time.Time) *Service {
	s := &Service{store: store, l: NewLedger(), now: now}
	for _, e := range events {
		s.l.Apply(e)
	}
	// 崩溃恢复:对最后一条外部事件重跑规则引擎。
	// 派生事件 ID 由父事件幂等键决定,已持久化的会被存储层去重,
	// 崩溃窗口内(主事件已落盘、派生事件未落盘)丢失的在此补齐。
	for i := len(events) - 1; i >= 0; i-- {
		if !events[i].Derived {
			s.react(events[i], 0)
			break
		}
	}
	return s
}

// preflight 命令级幂等预检:携带已应用过的 event_id 的重试
// 直接返回重复,不再触发校验冲突(例如重复注册批次)。
func (s *Service) preflight(meta Meta) (*CommandResult, bool) {
	if meta.EventID == "" {
		return nil, false
	}
	source := meta.Source
	if source == "" {
		source = "api"
	}
	if seq, ok := s.store.Has(source + "|" + meta.EventID); ok {
		return &CommandResult{Duplicate: true, Seq: seq}, true
	}
	return nil, false
}

// CommandResult 是命令的通用结果。Duplicate=true 表示幂等命中,
// 该请求已被应用过,本次未产生任何状态变化。
type CommandResult struct {
	Duplicate bool   `json:"duplicate"`
	Seq       uint64 `json:"seq,omitempty"`
	LotID     string `json:"lot_id,omitempty"`
}

func newEventID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "evt-" + hex.EncodeToString(b[:])
}

func (s *Service) newEvent(meta Meta, t EventType, payload any) *Event {
	id := meta.EventID
	if id == "" {
		id = newEventID()
	}
	source := meta.Source
	if source == "" {
		source = "api"
	}
	at := meta.OccurredAt
	if at.IsZero() {
		at = s.now()
	}
	return &Event{
		ID:             id,
		Type:           t,
		Source:         source,
		OccurredAt:     at.UTC(),
		IdempotencyKey: source + "|" + id,
		Payload:        marshalPayload(payload),
	}
}

// dispatch 追加并应用一条事件,随后运行规则引擎。
func (s *Service) dispatch(e *Event) (*CommandResult, error) {
	seq, dup, err := s.store.Append(e)
	if err != nil {
		return nil, err
	}
	if dup {
		return &CommandResult{Duplicate: true, Seq: seq}, nil
	}
	s.l.Apply(*e)
	s.react(*e, 0)
	return &CommandResult{Seq: seq}, nil
}

// emitDerived 产生一条派生事件(同一临界区内追加并应用)。
// 派生事件 ID 由父事件幂等键 + 类型 + 作用域构成,天然幂等。
func (s *Service) emitDerived(parent Event, t EventType, scope string, at time.Time, payload any, depth int) {
	e := &Event{
		ID:             fmt.Sprintf("derived:%s:%s:%s", parent.IdempotencyKey, t, scope),
		Type:           t,
		Source:         "rule-engine",
		OccurredAt:     at.UTC(),
		Derived:        true,
		IdempotencyKey: fmt.Sprintf("rule-engine|derived:%s:%s:%s", parent.IdempotencyKey, t, scope),
		Payload:        marshalPayload(payload),
	}
	seq, dup, err := s.store.Append(e)
	if err != nil || dup {
		return
	}
	e.Seq = seq
	s.l.Apply(*e)
	s.react(*e, depth+1)
}

// react 是规则引擎:根据刚应用的事件产生派生事件。回放时不运行。
func (s *Service) react(e Event, depth int) {
	if depth > 3 {
		return
	}
	switch e.Type {
	case EvObservation:
		p, _ := decodePayload[ObservationPayload](e)
		if lot := s.l.lots[p.LotID]; lot != nil {
			s.reactObservation(e, lot, p, depth)
		}
	case EvLotSealed:
		if lot := s.l.lots[payloadLotID(e)]; lot != nil {
			// 封运完成,若海关合格结论已在等待,则生效为一次放行
			s.maybeRelease(e, lot, e.OccurredAt, depth)
			s.maybeRaisePriority(e, lot, depth)
		}
	case EvHandover:
		p, _ := decodePayload[HandoverPayload](e)
		lot := s.l.lots[p.LotID]
		if lot == nil {
			return
		}
		if s.l.conservationOK(lot, p.BoxCount, p.LiveWeightKg) {
			// 守恒恢复:此前因守恒差异被中止的批次自动解除
			if lot.Halted && lot.HaltKind == "discrepancy" {
				s.emitDerived(e, EvLotResumed, lot.ID, e.OccurredAt, ResumePayload{
					LotID: lot.ID, Auto: true, Reason: "后续交接守恒校验通过",
				}, depth)
			}
		} else {
			s.emitDerived(e, EvHandoverDiscrepancy, lot.ID, e.OccurredAt, DiscrepancyPayload{
				LotID: lot.ID, Context: "交接",
				DeclaredCount: p.BoxCount, ExpectedCount: lot.BoxCount(),
				DeclaredWeight: p.LiveWeightKg, ExpectedWeight: lot.LiveWeight(),
			}, depth)
		}
		s.maybeRaisePriority(e, lot, depth)
	case EvLotDelivered:
		p, _ := decodePayload[DeliverPayload](e)
		if lot := s.l.lots[p.LotID]; lot != nil && !s.l.conservationOK(lot, p.BoxCount, p.LiveWeightKg) {
			s.emitDerived(e, EvHandoverDiscrepancy, lot.ID, e.OccurredAt, DiscrepancyPayload{
				LotID: lot.ID, Context: "交付",
				DeclaredCount: p.BoxCount, ExpectedCount: lot.BoxCount(),
				DeclaredWeight: p.LiveWeightKg, ExpectedWeight: lot.LiveWeight(),
			}, depth)
		}
	case EvLotResumed:
		if lot := s.l.lots[payloadLotID(e)]; lot != nil {
			// 中止期间到达的海关合格结论在解除后补生效
			s.maybeRelease(e, lot, e.OccurredAt, depth)
			s.maybeRaisePriority(e, lot, depth)
		}
	case EvLotPicked, EvLotHalted, EvMetricViolated, EvLotOpened:
		if lot := s.l.lots[payloadLotID(e)]; lot != nil {
			s.maybeRaisePriority(e, lot, depth)
		}
	case EvFlightDelayed:
		p, _ := decodePayload[DelayFlightPayload](e)
		for _, lot := range s.l.boundLots(p.FlightID) {
			s.maybeRaisePriority(e, lot, depth)
		}
	}
}

func (s *Service) reactObservation(e Event, lot *Lot, p ObservationPayload, depth int) {
	// 1. 环境指标越界
	switch p.Kind {
	case ObsTemperature:
		if lot.TempMax > lot.TempMin && (p.Value < lot.TempMin || p.Value > lot.TempMax) {
			s.emitDerived(e, EvMetricViolated, lot.ID, e.OccurredAt, MetricViolatedPayload{
				LotID: lot.ID, Kind: p.Kind, Value: p.Value,
				Threshold: fmt.Sprintf("应在 %.1f~%.1f ℃", lot.TempMin, lot.TempMax),
			}, depth)
		}
	case ObsDissolvedOxygen:
		if lot.DOMin > 0 && p.Value < lot.DOMin {
			s.emitDerived(e, EvMetricViolated, lot.ID, e.OccurredAt, MetricViolatedPayload{
				LotID: lot.ID, Kind: p.Kind, Value: p.Value,
				Threshold: fmt.Sprintf("应 ≥ %.1f mg/L", lot.DOMin),
			}, depth)
		}
	case ObsMortality:
		if rate := lot.MortalityRate(); rate > 0.05 {
			s.emitDerived(e, EvMetricViolated, lot.ID, e.OccurredAt, MetricViolatedPayload{
				LotID: lot.ID, Kind: "mortality_rate", Value: rate * 100,
				Threshold: "死亡率应 ≤ 5%",
			}, depth)
		}
	}

	// 2. 检验结论:不合格 → 中止(封识破损且已封运 → 开封回检);复查合格 → 解除检验中止
	if p.Conclusion == "fail" {
		switch p.Kind {
		case ObsSeal:
			switch {
			case lot.Sealed && stageRank[lot.Stage] < stageRank[StageDeparted]:
				s.emitDerived(e, EvLotOpened, lot.ID, e.OccurredAt, OpenPayload{
					LotID: lot.ID, Reason: "封识破损或封识检查不通过",
				}, depth)
			case lot.Sealed:
				// 已在途中发现封识破损:不回退环节,中止待落地核查
				if !lot.Halted {
					s.emitDerived(e, EvLotHalted, lot.ID, e.OccurredAt, HaltPayload{
						LotID: lot.ID, Kind: "inspection", Reason: "运输途中发现封识破损,待落地核查",
					}, depth)
				}
			case !lot.Halted:
				s.emitDerived(e, EvLotHalted, lot.ID, e.OccurredAt, HaltPayload{
					LotID: lot.ID, Kind: "inspection", Reason: "封识检查不通过",
				}, depth)
			}
		case ObsMortality:
			if !lot.Halted {
				s.emitDerived(e, EvLotHalted, lot.ID, e.OccurredAt, HaltPayload{
					LotID: lot.ID, Kind: "inspection", Reason: "死亡抽检不通过",
				}, depth)
			}
		case ObsSecurity:
			if !lot.Halted {
				s.emitDerived(e, EvLotHalted, lot.ID, e.OccurredAt, HaltPayload{
					LotID: lot.ID, Kind: "inspection", Reason: "安检不通过",
				}, depth)
			}
		case ObsCustoms:
			if !lot.Halted {
				s.emitDerived(e, EvLotHalted, lot.ID, e.OccurredAt, HaltPayload{
					LotID: lot.ID, Kind: "inspection",
					Reason: fmt.Sprintf("海关查验不通过(决定号 %s)", p.DecisionID),
				}, depth)
			}
		}
	} else if p.Conclusion == "pass" && lot.Halted && lot.HaltKind == "inspection" {
		s.emitDerived(e, EvLotResumed, lot.ID, e.OccurredAt, ResumePayload{
			LotID: lot.ID, Auto: true, Reason: "复查合格,解除检验中止",
		}, depth)
	}

	// 3. 联合核验三项结论齐全 → 核验通过
	if lot.Joint.PassedAt == nil && lot.Joint.MortalityPass && lot.Joint.SealOK && lot.Joint.SecurityPass {
		basis := fmt.Sprintf("死亡抽检合格于 %s;封识正常于 %s;安检合格于 %s",
			lot.Joint.MortalityAt.Format("15:04:05"), lot.Joint.SealAt.Format("15:04:05"), lot.Joint.SecurityAt.Format("15:04:05"))
		s.emitDerived(e, EvJointCheckPassed, lot.ID, e.OccurredAt, JointCheckPassedPayload{
			LotID: lot.ID, Basis: basis,
		}, depth)
	}

	// 4. 海关合格结论 → 一次放行(按决定号幂等;未封运则暂存待封运后生效)
	if p.Kind == ObsCustoms && p.Conclusion == "pass" {
		switch {
		case lot.Released && lot.ReleaseDecisionID == p.DecisionID:
			// 恢复重跑时,本事件可能正是放行的触发源,不计为重复回调
			if lot.ReleaseSourceKey != e.IdempotencyKey {
				s.emitDerived(e, EvReleaseDuplicate, lot.ID, e.OccurredAt, ReleaseDupPayload{
					LotID: lot.ID, DecisionID: p.DecisionID,
				}, depth)
			}
		case lot.Released:
			s.emitDerived(e, EvReleaseConflict, lot.ID, e.OccurredAt, ReleaseConflictPayload{
				LotID: lot.ID, KeptDecisionID: lot.ReleaseDecisionID, RejectedDecisionID: p.DecisionID,
			}, depth)
		default:
			s.maybeRelease(e, lot, e.OccurredAt, depth)
		}
	}

	// 5. 接近生存阈值 → 自动提升处置优先级
	s.maybeRaisePriority(e, lot, depth)
}

// maybeRelease 在"已封运、未中止、海关结论合格、尚未放行"时产生一次放行。
// 放行只会由该函数触发,决定号幂等,从机制上杜绝二次放行。
func (s *Service) maybeRelease(parent Event, lot *Lot, at time.Time, depth int) {
	if lot.Released || lot.Halted || !lot.Sealed || lot.Customs.Conclusion != "pass" {
		return
	}
	s.emitDerived(parent, EvLotReleased, lot.ID, at, ReleasePayload{
		LotID: lot.ID, DecisionID: lot.Customs.DecisionID,
		Basis:     "海关查验合格,联合核验已通过并完成封运",
		SourceKey: parent.IdempotencyKey,
	}, depth)
}

func (s *Service) maybeRaisePriority(parent Event, lot *Lot, depth int) {
	if lot == nil || lot.Closed || lot.Stage == StageDelivered {
		return
	}
	computed, reason := s.l.computePriority(lot, s.now())
	if priorityRank(computed) > priorityRank(lot.Priority) {
		s.emitDerived(parent, EvPriorityRaised, lot.ID+":"+string(computed), s.now(), PriorityRaisedPayload{
			LotID: lot.ID, From: lot.Priority, To: computed, Reason: reason,
		}, depth)
	}
}

// ---- 命令 ----

func (s *Service) RegisterLot(meta Meta, p RegisterLotPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	if p.LotID == "" || p.CatchBatch == "" || p.Tank == "" || p.Destination == "" || p.Custodian == "" {
		return nil, invalidf("批次号、捕捞批次、暂养池、目的地、责任方均不能为空")
	}
	if s.l.lots[p.LotID] != nil {
		return nil, conflictf("批次 %s 已存在", p.LotID)
	}
	if len(p.Boxes) == 0 {
		return nil, invalidf("批次至少包含一箱")
	}
	seen := map[string]bool{}
	for _, b := range p.Boxes {
		if b.Code == "" || b.WeightKg <= 0 {
			return nil, invalidf("箱码不能为空且重量必须为正:%q", b.Code)
		}
		if seen[b.Code] || s.l.boxes[b.Code] != nil {
			return nil, conflictf("箱码 %s 重复", b.Code)
		}
		seen[b.Code] = true
	}
	if p.SurvivalBudgetHours <= 0 {
		p.SurvivalBudgetHours = 8
	}
	return s.dispatch(s.newEvent(meta, EvLotRegistered, p))
}

// ObservationInput 是一条观测上报(负载 + 来源信息)。
type ObservationInput struct {
	Meta    Meta
	Payload ObservationPayload
}

// ObsResult 是单条观测的处理结果。
type ObsResult struct {
	Duplicate bool   `json:"duplicate"`
	Seq       uint64 `json:"seq,omitempty"`
	Error     string `json:"error,omitempty"`
}

// RecordObservations 批量追加观测(离线补传场景)。各条相互独立,
// 幂等键重复的条目被安全忽略,不会造成二次放行或重复扣重。
func (s *Service) RecordObservations(items []ObservationInput) []ObsResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]ObsResult, 0, len(items))
	for _, in := range items {
		res := ObsResult{}
		if r, dup := s.preflight(in.Meta); dup {
			res.Duplicate = true
			res.Seq = r.Seq
			results = append(results, res)
			continue
		}
		if err := s.validateObservation(in.Payload); err != nil {
			res.Error = err.Error()
			results = append(results, res)
			continue
		}
		r, err := s.dispatch(s.newEvent(in.Meta, EvObservation, in.Payload))
		if err != nil {
			res.Error = err.Error()
		} else {
			res.Duplicate = r.Duplicate
			res.Seq = r.Seq
		}
		results = append(results, res)
	}
	return results
}

func (s *Service) validateObservation(p ObservationPayload) *Error {
	lot := s.l.lots[p.LotID]
	if lot == nil {
		return notFoundf("批次 %s 不存在", p.LotID)
	}
	if lot.Closed {
		return conflictf("批次 %s 已关闭(拆分/合并后清空)", p.LotID)
	}
	if lot.Stage == StageDelivered {
		return conflictf("批次 %s 已交付,不再接受观测", p.LotID)
	}
	switch p.Kind {
	case ObsTemperature, ObsDissolvedOxygen, ObsMortality, ObsSeal, ObsSecurity, ObsCustoms:
	default:
		return invalidf("未知观测类别 %q", p.Kind)
	}
	if p.BoxCode != "" && lot.Boxes[p.BoxCode] == nil {
		return notFoundf("箱码 %s 不属于批次 %s", p.BoxCode, p.LotID)
	}
	if p.Kind == ObsMortality && (p.DeadWeightKg < 0 || p.DeadCount < 0) {
		return invalidf("死亡重量与数量不能为负")
	}
	if p.Kind == ObsCustoms && p.Conclusion == "pass" && p.DecisionID == "" {
		return invalidf("海关合格结论必须携带决定号")
	}
	return nil
}

func (s *Service) Handover(meta Meta, p HandoverPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(p.LotID)
	if err != nil {
		return nil, err
	}
	if lot.Stage == StageDelivered {
		return nil, conflictf("批次 %s 已交付", p.LotID)
	}
	if lot.Halted && lot.HaltKind != "discrepancy" {
		return nil, conflictf("批次 %s 处于中止状态(%s),恢复后才能交接", p.LotID, lot.HaltReason)
	}
	if p.ToParty == "" || p.FromParty == "" {
		return nil, invalidf("交接双方不能为空")
	}
	if p.BoxCount < 0 || p.LiveWeightKg < 0 {
		return nil, invalidf("箱数与重量不能为负")
	}
	return s.dispatch(s.newEvent(meta, EvHandover, p))
}

func (s *Service) Pick(meta Meta, lotID string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Halted {
		return nil, conflictf("批次 %s 处于中止状态(%s)", lotID, lot.HaltReason)
	}
	if lot.Stage != StageHolding {
		return nil, conflictf("批次 %s 当前环节为 %s,不能出库", lotID, StageLabel(lot.Stage))
	}
	return s.dispatch(s.newEvent(meta, EvLotPicked, PickPayload{LotID: lotID}))
}

func (s *Service) Seal(meta Meta, lotID, sealCode string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(lotID)
	if err != nil {
		return nil, err
	}
	if sealCode == "" {
		return nil, invalidf("封识号不能为空")
	}
	if lot.Halted {
		return nil, conflictf("批次 %s 处于中止状态(%s)", lotID, lot.HaltReason)
	}
	if lot.Sealed {
		return nil, conflictf("批次 %s 已封运", lotID)
	}
	if lot.PickedAt == nil {
		return nil, conflictf("批次 %s 尚未出库,不能封运", lotID)
	}
	if lot.Joint.PassedAt == nil {
		return nil, conflictf("批次 %s 联合核验未通过,不能封运", lotID)
	}
	return s.dispatch(s.newEvent(meta, EvLotSealed, SealPayload{LotID: lotID, SealCode: sealCode}))
}

func (s *Service) Open(meta Meta, lotID, reason string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(lotID)
	if err != nil {
		return nil, err
	}
	if !lot.Sealed {
		return nil, conflictf("批次 %s 未封运,无需开封", lotID)
	}
	if stageRank[lot.Stage] >= stageRank[StageDeparted] {
		return nil, conflictf("批次 %s 已起飞,不能开封", lotID)
	}
	if reason == "" {
		return nil, invalidf("开封原因不能为空")
	}
	return s.dispatch(s.newEvent(meta, EvLotOpened, OpenPayload{LotID: lotID, Reason: reason}))
}

func (s *Service) Split(meta Meta, p SplitPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(p.LotID)
	if err != nil {
		return nil, err
	}
	if err := s.requireReassignable(lot); err != nil {
		return nil, err
	}
	if p.NewLotID == "" || s.l.lots[p.NewLotID] != nil {
		return nil, conflictf("子批次号 %q 为空或已存在", p.NewLotID)
	}
	if len(p.BoxCodes) == 0 {
		return nil, invalidf("拆分箱码列表不能为空")
	}
	seen := map[string]bool{}
	for _, code := range p.BoxCodes {
		if lot.Boxes[code] == nil {
			return nil, notFoundf("箱码 %s 不属于批次 %s", code, p.LotID)
		}
		if seen[code] {
			return nil, invalidf("箱码 %s 重复", code)
		}
		seen[code] = true
	}
	return s.dispatch(s.newEvent(meta, EvLotSplit, p))
}

func (s *Service) Merge(meta Meta, p MergePayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	if len(p.LotIDs) < 2 {
		return nil, invalidf("合并至少需要两个批次")
	}
	if p.NewLotID == "" || s.l.lots[p.NewLotID] != nil {
		return nil, conflictf("合并批次号 %q 为空或已存在", p.NewLotID)
	}
	var first *Lot
	for _, id := range p.LotIDs {
		lot, err := s.requireLot(id)
		if err != nil {
			return nil, err
		}
		if err := s.requireReassignable(lot); err != nil {
			return nil, err
		}
		if first == nil {
			first = lot
			continue
		}
		if lot.Destination != first.Destination || lot.Stage != first.Stage {
			return nil, conflictf("仅目的地与环节相同的批次可以合并:%s 与 %s 不一致", id, first.ID)
		}
	}
	return s.dispatch(s.newEvent(meta, EvLotsMerged, p))
}

func (s *Service) Rebind(meta Meta, p RebindPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(p.LotID)
	if err != nil {
		return nil, err
	}
	if err := s.requireReassignable(lot); err != nil {
		return nil, err
	}
	if p.OrderID == "" && p.Destination == "" && p.FlightID == "" {
		return nil, invalidf("改配必须指定新的订单、目的地或航班")
	}
	if p.Reason == "" {
		return nil, invalidf("改配原因不能为空")
	}
	return s.dispatch(s.newEvent(meta, EvLotRebound, p))
}

// requireReassignable 实现核心规则:仅尚未封运的货物可以重新分配。
func (s *Service) requireReassignable(lot *Lot) *Error {
	if lot.Closed {
		return conflictf("批次 %s 已关闭", lot.ID)
	}
	if lot.Halted {
		return conflictf("批次 %s 处于中止状态(%s)", lot.ID, lot.HaltReason)
	}
	if !preSeal(lot.Stage) {
		return conflictf("批次 %s 已封运(环节:%s),不能改配;确需调整请先开封回到检查环节", lot.ID, StageLabel(lot.Stage))
	}
	return nil
}

func (s *Service) AssignFlight(meta Meta, p AssignFlightPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(p.LotID)
	if err != nil {
		return nil, err
	}
	if err := s.requireReassignable(lot); err != nil {
		return nil, err
	}
	if s.l.flights[p.FlightID] == nil {
		return nil, notFoundf("航班 %s 未登记", p.FlightID)
	}
	return s.dispatch(s.newEvent(meta, EvLotRebound, RebindPayload{
		LotID: p.LotID, FlightID: p.FlightID, Reason: "指定航班",
	}))
}

func (s *Service) RegisterFlight(meta Meta, p RegisterFlightPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	if p.FlightID == "" || p.Destination == "" {
		return nil, invalidf("航班号与目的地不能为空")
	}
	if f := s.l.flights[p.FlightID]; f != nil && !f.LoadingCutoff.IsZero() {
		return nil, conflictf("航班 %s 已登记", p.FlightID)
	}
	return s.dispatch(s.newEvent(meta, EvFlightRegistered, p))
}

func (s *Service) DelayFlight(meta Meta, p DelayFlightPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	f := s.l.flights[p.FlightID]
	if f == nil {
		return nil, notFoundf("航班 %s 未登记", p.FlightID)
	}
	if f.Status != "SCHEDULED" {
		return nil, conflictf("航班 %s 状态为 %s,不能延误", p.FlightID, f.Status)
	}
	if p.NewCutoff.IsZero() || p.NewDepartAt.IsZero() {
		return nil, invalidf("新截载与新起飞时间不能为空")
	}
	return s.dispatch(s.newEvent(meta, EvFlightDelayed, p))
}

func (s *Service) DepartFlight(meta Meta, flightID string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	f := s.l.flights[flightID]
	if f == nil {
		return nil, notFoundf("航班 %s 未登记", flightID)
	}
	if f.Status != "SCHEDULED" {
		return nil, conflictf("航班 %s 状态为 %s,不能起飞", flightID, f.Status)
	}
	return s.dispatch(s.newEvent(meta, EvFlightDeparted, FlightRefPayload{FlightID: flightID}))
}

func (s *Service) ArriveFlight(meta Meta, flightID string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	f := s.l.flights[flightID]
	if f == nil {
		return nil, notFoundf("航班 %s 未登记", flightID)
	}
	if f.Status != "DEPARTED" {
		return nil, conflictf("航班 %s 状态为 %s,不能到港", flightID, f.Status)
	}
	return s.dispatch(s.newEvent(meta, EvFlightArrived, FlightRefPayload{FlightID: flightID}))
}

func (s *Service) Load(meta Meta, p LoadPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(p.LotID)
	if err != nil {
		return nil, err
	}
	if lot.Halted {
		return nil, conflictf("批次 %s 处于中止状态(%s)", p.LotID, lot.HaltReason)
	}
	if !lot.Released {
		return nil, conflictf("批次 %s 尚未放行,不能装机", p.LotID)
	}
	if lot.Stage != StageReleased {
		return nil, conflictf("批次 %s 当前环节为 %s,不能装机", p.LotID, StageLabel(lot.Stage))
	}
	if p.FlightID != lot.FlightID {
		return nil, conflictf("批次 %s 绑定航班为 %s,不能装入 %s", p.LotID, lot.FlightID, p.FlightID)
	}
	if f := s.l.flights[p.FlightID]; f != nil && !f.LoadingCutoff.IsZero() && s.now().After(f.LoadingCutoff) {
		return nil, conflictf("航班 %s 已过截载时间 %s", p.FlightID, f.LoadingCutoff.Format("15:04"))
	}
	return s.dispatch(s.newEvent(meta, EvLotLoaded, p))
}

func (s *Service) Deliver(meta Meta, p DeliverPayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(p.LotID)
	if err != nil {
		return nil, err
	}
	if lot.Halted {
		return nil, conflictf("批次 %s 处于中止状态(%s)", p.LotID, lot.HaltReason)
	}
	if lot.Stage != StageArrived {
		return nil, conflictf("批次 %s 当前环节为 %s,不能交付", p.LotID, StageLabel(lot.Stage))
	}
	if p.ToParty == "" {
		return nil, invalidf("接收方不能为空")
	}
	return s.dispatch(s.newEvent(meta, EvLotDelivered, p))
}

func (s *Service) Halt(meta Meta, lotID, reason string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Halted {
		return nil, conflictf("批次 %s 已处于中止状态", lotID)
	}
	if reason == "" {
		return nil, invalidf("中止原因不能为空")
	}
	return s.dispatch(s.newEvent(meta, EvLotHalted, HaltPayload{LotID: lotID, Kind: "manual", Reason: reason}))
}

func (s *Service) Resume(meta Meta, lotID, reason string) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	lot, err := s.requireLot(lotID)
	if err != nil {
		return nil, err
	}
	if !lot.Halted {
		return nil, conflictf("批次 %s 未处于中止状态", lotID)
	}
	return s.dispatch(s.newEvent(meta, EvLotResumed, ResumePayload{LotID: lotID, Reason: reason}))
}

func (s *Service) ChangeOrder(meta Meta, p OrderChangePayload) (*CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if res, dup := s.preflight(meta); dup {
		return res, nil
	}
	if s.l.orders[p.OrderID] == nil {
		return nil, notFoundf("订单 %s 不存在", p.OrderID)
	}
	if p.Destination == "" {
		return nil, invalidf("新目的地不能为空")
	}
	return s.dispatch(s.newEvent(meta, EvOrderChanged, p))
}

func (s *Service) requireLot(id string) (*Lot, *Error) {
	lot := s.l.lots[id]
	if lot == nil {
		return nil, notFoundf("批次 %s 不存在", id)
	}
	return lot, nil
}

// ---- 查询 ----

func (s *Service) ScanBox(code string) (*ScanResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l.ScanBox(code, s.now())
}

func (s *Service) Lineage(code string) (*LineageView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l.Lineage(code)
}

func (s *Service) Explain(lotID, focus string) ([]DecisionEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l.Explain(lotID, focus)
}

func (s *Service) LotDetail(lotID string) (*LotView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l.LotDetail(lotID, s.now())
}

func (s *Service) Queue() []QueueItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.l.Queue(s.now())
}

// Flights 返回全部航班(按 ID 排序),供查询端点使用。
func (s *Service) Flights() []*Flight {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Flight
	for _, f := range s.l.flights {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
