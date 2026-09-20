package transit

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

// Error 是带 HTTP 状态码的业务错误。
type Error struct {
	Status  int    `json:"-"`
	Message string `json:"error"`
}

func (e *Error) Error() string { return e.Message }

func badRequest(format string, args ...any) *Error {
	return &Error{Status: 400, Message: fmt.Sprintf(format, args...)}
}

func notFound(format string, args ...any) *Error {
	return &Error{Status: 404, Message: fmt.Sprintf(format, args...)}
}

func conflict(format string, args ...any) *Error {
	return &Error{Status: 409, Message: fmt.Sprintf(format, args...)}
}

// CommandResult 是写命令的统一返回:是否命中幂等重复、产生的事件与最新视图。
type CommandResult struct {
	Duplicate bool            `json:"duplicate"`
	EventIDs  []string        `json:"event_ids,omitempty"`
	Lot       *LotView        `json:"lot,omitempty"`
	Batch     *CatchBatchView `json:"catch_batch,omitempty"`
	Order     *OrderView      `json:"order,omitempty"`
	Flight    *FlightView     `json:"flight,omitempty"`
}

// Service 在事件存储之上实现全部业务规则。
type Service struct {
	store *Store
}

// NewService 创建业务服务。
func NewService(st *Store) *Service { return &Service{store: st} }

// emit 落盘一条事件。
func (s *Service) emit(evType string, payload any, m Meta) (Event, bool, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, false, err
	}
	return s.store.Append(Event{
		Type:           evType,
		Actor:          m.Actor,
		Reason:         m.Reason,
		CausedBy:       m.CausedBy,
		IdempotencyKey: m.IdempotencyKey,
		OccurredAt:     m.OccurredAt,
		Payload:        raw,
	})
}

// lotView 读取批次当前视图。
func (s *Service) lotView(lotID string) (*LotView, error) {
	lot, ok := s.store.Lot(lotID)
	if !ok {
		return nil, notFound("批次 %s 不存在", lotID)
	}
	return lot, nil
}

// RegisterCatchBatch 登记原始捕捞批次。
func (s *Service) RegisterCatchBatch(p CatchBatchRegistered, m Meta) (*CommandResult, error) {
	if p.CatchBatchID == "" || p.Species == "" {
		return nil, badRequest("捕捞批次号与品种不能为空")
	}
	if p.TotalBoxes <= 0 || p.TotalWeightKg <= 0 || p.SurvivalHours <= 0 {
		return nil, badRequest("箱数、重量与可存活时长必须为正数")
	}
	if _, ok := s.store.Batch(p.CatchBatchID); ok {
		return nil, conflict("捕捞批次 %s 已存在", p.CatchBatchID)
	}
	ev, dup, err := s.emit(EventCatchBatchRegistered, p, m)
	if err != nil {
		return nil, err
	}
	batch, _ := s.store.Batch(p.CatchBatchID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Batch: batch}, nil
}

// CreateBondedLot 从捕捞批次拆分出保税暂养批次,箱数与重量不得超过批次余量。
func (s *Service) CreateBondedLot(p BondedLotCreated, m Meta) (*CommandResult, error) {
	if p.LotID == "" || p.CatchBatchID == "" || p.TankID == "" {
		return nil, badRequest("批次号、捕捞批次号与暂养池不能为空")
	}
	if p.Custodian == "" {
		p.Custodian = "保税暂养库"
	}
	if len(p.Boxes) == 0 {
		return nil, badRequest("箱码列表不能为空")
	}
	if _, ok := s.store.Lot(p.LotID); ok {
		return nil, conflict("批次 %s 已存在", p.LotID)
	}
	batch, ok := s.store.Batch(p.CatchBatchID)
	if !ok {
		return nil, notFound("捕捞批次 %s 不存在", p.CatchBatchID)
	}
	seen := map[string]bool{}
	var weight float64
	for _, b := range p.Boxes {
		if b.BoxCode == "" || b.WeightKg <= 0 {
			return nil, badRequest("箱码不能为空且单箱重量必须为正数")
		}
		if seen[b.BoxCode] {
			return nil, badRequest("箱码 %s 在本次登记中重复", b.BoxCode)
		}
		if s.store.HasBox(b.BoxCode) {
			return nil, conflict("箱码 %s 已登记在其他批次", b.BoxCode)
		}
		seen[b.BoxCode] = true
		weight += b.WeightKg
	}
	// 守恒:入池数量不得超过捕捞批次余量。
	if len(p.Boxes) > batch.RemainingBoxes || weight-batch.RemainingWeightKg > weightEpsilon {
		return nil, conflict("超出捕捞批次 %s 的余量(剩余 %d 箱 / %.3f kg)", p.CatchBatchID, batch.RemainingBoxes, batch.RemainingWeightKg)
	}
	ev, dup, err := s.emit(EventBondedLotCreated, p, m)
	if err != nil {
		return nil, err
	}
	lot, _ := s.store.Lot(p.LotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// SplitLot 把父批次的箱码精确划分给若干子批次,划分必须不重不漏(守恒)。
func (s *Service) SplitLot(parentID string, children []SplitChild, m Meta) (*CommandResult, error) {
	parent, err := s.lotView(parentID)
	if err != nil {
		return nil, err
	}
	if !parent.Status.Reallocatable() {
		return nil, conflict("批次 %s 当前状态 %s 不允许拆分(仅封运前可拆分)", parentID, parent.Status)
	}
	if len(children) == 0 {
		return nil, badRequest("子批次列表不能为空")
	}
	assigned := map[string]bool{}
	for _, ch := range children {
		if ch.LotID == "" {
			return nil, badRequest("子批次号不能为空")
		}
		if _, ok := s.store.Lot(ch.LotID); ok {
			return nil, conflict("批次 %s 已存在", ch.LotID)
		}
		if ch.TankID == "" {
			ch.TankID = parent.TankID
		}
		if len(ch.BoxCodes) == 0 {
			return nil, badRequest("子批次 %s 的箱码列表不能为空", ch.LotID)
		}
		for _, code := range ch.BoxCodes {
			if _, ok := parent.Boxes[code]; !ok {
				return nil, conflict("箱码 %s 不属于批次 %s", code, parentID)
			}
			if assigned[code] {
				return nil, conflict("箱码 %s 被重复划分", code)
			}
			assigned[code] = true
		}
	}
	if len(assigned) != parent.BoxCount {
		return nil, conflict("拆分不守恒:父批次 %d 箱,子批次合计 %d 箱", parent.BoxCount, len(assigned))
	}
	ev, dup, err := s.emit(EventLotSplit, LotSplit{ParentLotID: parentID, Children: children}, m)
	if err != nil {
		return nil, err
	}
	lot, _ := s.store.Lot(parentID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// MergeLots 把多个暂养中的批次合并为一个,箱数与重量为各父批次之和(守恒)。
func (s *Service) MergeLots(parentIDs []string, childID, tankID, custodian string, m Meta) (*CommandResult, error) {
	if len(parentIDs) < 2 {
		return nil, badRequest("合并至少需要两个父批次")
	}
	if childID == "" {
		return nil, badRequest("合并后的批次号不能为空")
	}
	if _, ok := s.store.Lot(childID); ok {
		return nil, conflict("批次 %s 已存在", childID)
	}
	seen := map[string]bool{}
	for _, pid := range parentIDs {
		if seen[pid] {
			return nil, badRequest("父批次 %s 重复", pid)
		}
		seen[pid] = true
		parent, err := s.lotView(pid)
		if err != nil {
			return nil, err
		}
		if parent.Status != StatusHolding {
			return nil, conflict("批次 %s 状态为 %s,仅暂养中且未分配的批次可以合并", pid, parent.Status)
		}
	}
	if custodian == "" {
		parent, _ := s.store.Lot(parentIDs[0])
		custodian = parent.Custodian
	}
	ev, dup, err := s.emit(EventLotMerged, LotMerged{ParentLotIDs: parentIDs, ChildLotID: childID, TankID: tankID, Custodian: custodian}, m)
	if err != nil {
		return nil, err
	}
	lot, _ := s.store.Lot(childID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// RegisterOrder 登记客户订单。
func (s *Service) RegisterOrder(p OrderRegistered, m Meta) (*CommandResult, error) {
	if p.OrderID == "" || p.Destination == "" {
		return nil, badRequest("订单号与目的地不能为空")
	}
	if _, ok := s.store.Order(p.OrderID); ok {
		return nil, conflict("订单 %s 已存在", p.OrderID)
	}
	ev, dup, err := s.emit(EventOrderRegistered, p, m)
	if err != nil {
		return nil, err
	}
	order, _ := s.store.Order(p.OrderID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Order: order}, nil
}

// ChangeOrder 记录订单变化,受影响批次被标记为待改配。
func (s *Service) ChangeOrder(orderID, change string, m Meta) (*CommandResult, error) {
	if _, ok := s.store.Order(orderID); !ok {
		return nil, notFound("订单 %s 不存在", orderID)
	}
	ev, dup, err := s.emit(EventOrderChanged, OrderChanged{OrderID: orderID, Change: change}, m)
	if err != nil {
		return nil, err
	}
	order, _ := s.store.Order(orderID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Order: order}, nil
}

// Allocate 把批次分配到订单、车辆与航班。
func (s *Service) Allocate(lotID, orderID, vehicleID, flightID string, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if !lot.Status.Reallocatable() {
		return nil, conflict("批次 %s 当前状态 %s 不允许分配", lotID, lot.Status)
	}
	if lot.OrderID != "" {
		return nil, conflict("批次 %s 已分配到订单 %s,请使用改配", lotID, lot.OrderID)
	}
	return s.allocate(lotID, orderID, vehicleID, flightID, false, m)
}

// Reallocate 改配批次。仅尚未封运的货物可以改配;已封运批次须先开封。
func (s *Service) Reallocate(lotID, orderID, vehicleID, flightID string, trigger ReallocTrigger, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if !lot.Status.Reallocatable() {
		return nil, conflict("批次 %s 已封运或离场(状态 %s),不能重新分配;如需改配请先开封回到检查环节", lotID, lot.Status)
	}
	switch trigger {
	case TriggerFlightDelay, TriggerOrderChange, TriggerMetricBreach, TriggerManual:
	default:
		return nil, badRequest("未知改配触发原因 %q", trigger)
	}
	return s.allocate(lotID, orderID, vehicleID, flightID, true, m, trigger)
}

func (s *Service) allocate(lotID, orderID, vehicleID, flightID string, isRealloc bool, m Meta, trigger ...ReallocTrigger) (*CommandResult, error) {
	order, ok := s.store.Order(orderID)
	if !ok {
		return nil, notFound("订单 %s 不存在", orderID)
	}
	evType := EventLotAllocated
	payload := any(LotAllocated{LotID: lotID, OrderID: orderID, VehicleID: vehicleID, FlightID: flightID, Destination: order.Destination})
	if isRealloc {
		evType = EventLotReallocated
		payload = LotReallocated{LotID: lotID, OrderID: orderID, VehicleID: vehicleID, FlightID: flightID, Destination: order.Destination, Trigger: trigger[0]}
	}
	ev, dup, err := s.emit(evType, payload, m)
	if err != nil {
		return nil, err
	}
	lot, _ := s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// AppendTelemetry 追加一条环境观测。按设备来源与设备序号幂等,离线补传
// 不会重复入账;死亡抽检会同步调整台账,指标越界会自动提升处置优先级。
func (s *Service) AppendTelemetry(p TelemetryAppended, m Meta) (*CommandResult, error) {
	if p.DeviceID == "" {
		return nil, badRequest("设备编号不能为空")
	}
	switch p.Kind {
	case TelemetryTemperature, TelemetryDissolvedOxygen, TelemetryMortality:
	default:
		return nil, badRequest("未知观测类型 %q", p.Kind)
	}
	// 允许只给箱码:按箱码定位当前批次。
	if p.LotID == "" && p.BoxCode != "" {
		if lot, ok := s.store.LotByBox(p.BoxCode); ok {
			p.LotID = lot.LotID
		}
	}
	lot, err := s.lotView(p.LotID)
	if err != nil {
		return nil, err
	}
	if lot.Status.Terminal() {
		return nil, conflict("批次 %s 已 %s,不再接受观测", p.LotID, lot.Status)
	}
	if m.IdempotencyKey == "" {
		m.IdempotencyKey = fmt.Sprintf("telemetry:%s:%d", p.DeviceID, p.DeviceSeq)
	}
	ev, dup, err := s.emit(EventTelemetryAppended, p, m)
	if err != nil {
		return nil, err
	}
	if dup {
		lot, _ := s.store.Lot(p.LotID)
		return &CommandResult{Duplicate: true, EventIDs: []string{ev.ID}, Lot: lot}, nil
	}
	eventIDs := []string{ev.ID}
	th := s.store.Thresholds()
	// 死亡抽检:调整台账期望重量,后续交接按调整后值守恒。
	if p.Kind == TelemetryMortality && p.DeadWeightKg > 0 {
		adj, _, err := s.emit(EventQuantityAdjusted, QuantityAdjusted{
			LotID: p.LotID, DeadCount: p.DeadCount, DeadWeightKg: p.DeadWeightKg, SourceEventID: ev.ID,
		}, Meta{Actor: "system", CausedBy: ev.ID, Reason: "死亡抽检调整台账"})
		if err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, adj.ID)
	}
	// 指标越界:记录越界事件并提升处置优先级。
	var breach *MetricBreached
	switch p.Kind {
	case TelemetryTemperature:
		if p.Value < th.TempMinC || p.Value > th.TempMaxC {
			breach = &MetricBreached{LotID: p.LotID, Kind: p.Kind, Value: p.Value, Limit: fmt.Sprintf("%.1f~%.1f°C", th.TempMinC, th.TempMaxC)}
		}
	case TelemetryDissolvedOxygen:
		if p.Value < th.DissolvedOxygenMinMgL {
			breach = &MetricBreached{LotID: p.LotID, Kind: p.Kind, Value: p.Value, Limit: fmt.Sprintf("≥%.1f mg/L", th.DissolvedOxygenMinMgL)}
		}
	case TelemetryMortality:
		if p.DeadCount > 0 {
			breach = &MetricBreached{LotID: p.LotID, Kind: p.Kind, Value: float64(p.DeadCount), Limit: "死亡个体应为 0"}
		}
	}
	if breach != nil {
		bev, _, err := s.emit(EventMetricBreached, *breach, Meta{Actor: "system", CausedBy: ev.ID, Reason: "指标越界"})
		if err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, bev.ID)
		pev, _, err := s.emit(EventPriorityRaised, PriorityRaised{LotID: p.LotID, Level: PriorityHigh},
			Meta{Actor: "system", CausedBy: bev.ID, Reason: fmt.Sprintf("%s 越界,自动提升处置优先级", p.Kind)})
		if err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, pev.ID)
	}
	lot, _ = s.store.Lot(p.LotID)
	return &CommandResult{EventIDs: eventIDs, Lot: lot}, nil
}

// RecordInspection 记录一个部门的检查结果,按机构与回调号幂等;
// 检查不通过会使批次在相应环节中止。
func (s *Service) RecordInspection(p InspectionRecorded, m Meta) (*CommandResult, error) {
	switch p.Kind {
	case InspectionSecurity, InspectionCustoms, InspectionJoint:
	default:
		return nil, badRequest("未知检查环节 %q", p.Kind)
	}
	if p.Result != InspectionPass && p.Result != InspectionFail {
		return nil, badRequest("检查结论必须为 pass 或 fail")
	}
	if p.Authority == "" || p.CallbackID == "" {
		return nil, badRequest("检查机构与回调号不能为空")
	}
	lot, err := s.lotView(p.LotID)
	if err != nil {
		return nil, err
	}
	if lot.Status == StatusHalted {
		return nil, conflict("批次 %s 已中止,请先恢复再记录检查", p.LotID)
	}
	if !lot.Status.Reallocatable() {
		return nil, conflict("批次 %s 当前状态 %s 不接受新的检查结果", p.LotID, lot.Status)
	}
	if m.IdempotencyKey == "" {
		m.IdempotencyKey = fmt.Sprintf("inspection:%s:%s", p.Authority, p.CallbackID)
	}
	ev, dup, err := s.emit(EventInspectionRecorded, p, m)
	if err != nil {
		return nil, err
	}
	eventIDs := []string{ev.ID}
	if !dup && p.Result == InspectionFail {
		hev, _, err := s.emit(EventLotHalted, LotHalted{LotID: p.LotID, Step: string(p.Kind)},
			Meta{Actor: "system", CausedBy: ev.ID, Reason: fmt.Sprintf("%s 检查未通过(%s)", p.Kind, p.Authority)})
		if err != nil {
			return nil, err
		}
		eventIDs = append(eventIDs, hev.ID)
	}
	lot, _ = s.store.Lot(p.LotID)
	return &CommandResult{Duplicate: dup, EventIDs: eventIDs, Lot: lot}, nil
}

// Seal 施加封识。前提是安检、海关、联合核验全部通过。
func (s *Service) Seal(lotID, sealID string, m Meta) (*CommandResult, error) {
	if sealID == "" {
		return nil, badRequest("封识号不能为空")
	}
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if !lot.Status.Reallocatable() {
		return nil, conflict("批次 %s 当前状态 %s 不能施加封识", lotID, lot.Status)
	}
	var missing []string
	for _, kind := range []InspectionKind{InspectionSecurity, InspectionCustoms, InspectionJoint} {
		if lot.Inspections[string(kind)] != string(InspectionPass) {
			missing = append(missing, string(kind))
		}
	}
	if len(missing) > 0 {
		return nil, conflict("批次 %s 尚缺检查通过结论: %v", lotID, missing)
	}
	ev, dup, err := s.emit(EventLotSealed, LotSealed{LotID: lotID, SealID: sealID}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// Unseal 开封。已完成联合核验的批次一旦开封,即回到相应检查环节:
// 联合核验结论作废,须重新核验并重新封识后才能再次放行。
func (s *Service) Unseal(lotID string, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusSealed && lot.Status != StatusReleased {
		return nil, conflict("批次 %s 当前状态 %s 未封运,无需开封", lotID, lot.Status)
	}
	ev, dup, err := s.emit(EventSealBroken, SealBroken{LotID: lotID, SealID: lot.SealID, Step: string(InspectionJoint)}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// Release 海关放行。按放行号幂等:同一放行号的重复回调返回原结果,
// 已放行批次上的不同放行号会被拒绝,杜绝二次放行。
func (s *Service) Release(lotID, releaseID, authority string, m Meta) (*CommandResult, error) {
	if releaseID == "" || authority == "" {
		return nil, badRequest("放行号与放行机构不能为空")
	}
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status == StatusReleased {
		if lot.ReleaseID == releaseID {
			return &CommandResult{Duplicate: true, Lot: lot}, nil
		}
		return nil, conflict("批次 %s 已凭放行号 %s 放行,拒绝二次放行", lotID, lot.ReleaseID)
	}
	if lot.Status != StatusSealed {
		return nil, conflict("批次 %s 当前状态 %s 未封运,不能放行", lotID, lot.Status)
	}
	if m.IdempotencyKey == "" {
		m.IdempotencyKey = "release:" + releaseID
	}
	ev, dup, err := s.emit(EventLotReleased, LotReleased{LotID: lotID, ReleaseID: releaseID, Authority: authority, Destination: lot.Destination}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// Handover 记录一次交接。箱数与活体重量必须与台账一致(守恒);
// 不守恒时记录差异事件并使批次在交接环节中止。
func (s *Service) Handover(p HandoverRecorded, m Meta) (*CommandResult, error) {
	if p.HandoverID == "" || p.FromParty == "" || p.ToParty == "" {
		return nil, badRequest("交接号与交接双方不能为空")
	}
	lot, err := s.lotView(p.LotID)
	if err != nil {
		return nil, err
	}
	if lot.Status == StatusHalted {
		return nil, conflict("批次 %s 已中止,请先处置恢复", p.LotID)
	}
	if lot.Status.Terminal() {
		return nil, conflict("批次 %s 已 %s,不再交接", p.LotID, lot.Status)
	}
	if p.FromParty != lot.Custodian {
		return nil, conflict("交出方 %q 与当前责任方 %q 不一致", p.FromParty, lot.Custodian)
	}
	if m.IdempotencyKey == "" {
		m.IdempotencyKey = "handover:" + p.HandoverID
	}
	// 守恒校验:箱数与活体重量都必须与台账一致。
	if p.BoxCount != lot.BoxCount || math.Abs(p.LiveWeightKg-lot.LiveWeightKg) > weightEpsilon {
		dev, dup, err := s.emit(EventHandoverDiscrepancy, HandoverDiscrepancy{
			HandoverID: p.HandoverID, LotID: p.LotID, FromParty: p.FromParty, ToParty: p.ToParty,
			ExpectedBoxes: lot.BoxCount, ActualBoxes: p.BoxCount,
			ExpectedWeightKg: lot.LiveWeightKg, ActualWeightKg: p.LiveWeightKg,
		}, m)
		if err != nil {
			return nil, err
		}
		eventIDs := []string{dev.ID}
		if !dup {
			hev, _, err := s.emit(EventLotHalted, LotHalted{LotID: p.LotID, Step: fmt.Sprintf("handover:%s→%s", p.FromParty, p.ToParty)},
				Meta{Actor: "system", CausedBy: dev.ID, Reason: fmt.Sprintf("交接不守恒: 台账 %d 箱/%.3f kg,实交 %d 箱/%.3f kg", lot.BoxCount, lot.LiveWeightKg, p.BoxCount, p.LiveWeightKg)})
			if err != nil {
				return nil, err
			}
			eventIDs = append(eventIDs, hev.ID)
		}
		lot, _ := s.store.Lot(p.LotID)
		return &CommandResult{Duplicate: dup, EventIDs: eventIDs, Lot: lot}, nil
	}
	ev, dup, err := s.emit(EventHandoverRecorded, p, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(p.LotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// RegisterFlight 登记货运航班。
func (s *Service) RegisterFlight(p FlightRegistered, m Meta) (*CommandResult, error) {
	if p.FlightID == "" {
		return nil, badRequest("航班号不能为空")
	}
	if _, ok := s.store.Flight(p.FlightID); ok {
		return nil, conflict("航班 %s 已存在", p.FlightID)
	}
	ev, dup, err := s.emit(EventFlightRegistered, p, m)
	if err != nil {
		return nil, err
	}
	flight, _ := s.store.Flight(p.FlightID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Flight: flight}, nil
}

// DelayFlight 记录航班延误,相关批次被标记为待改配。
func (s *Service) DelayFlight(flightID string, newDeparture time.Time, m Meta) (*CommandResult, error) {
	if _, ok := s.store.Flight(flightID); !ok {
		return nil, notFound("航班 %s 不存在", flightID)
	}
	ev, dup, err := s.emit(EventFlightDelayed, FlightDelayed{FlightID: flightID, NewDepartureAt: newDeparture}, m)
	if err != nil {
		return nil, err
	}
	flight, _ := s.store.Flight(flightID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Flight: flight}, nil
}

// Halt 人工中止批次,记录发生环节与原因。
func (s *Service) Halt(lotID, step string, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status == StatusHalted {
		return nil, conflict("批次 %s 已处于中止状态", lotID)
	}
	if lot.Status.Terminal() {
		return nil, conflict("批次 %s 已 %s,不能中止", lotID, lot.Status)
	}
	ev, dup, err := s.emit(EventLotHalted, LotHalted{LotID: lotID, Step: step}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// Resume 恢复被中止的批次到中止前状态。
func (s *Service) Resume(lotID string, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusHalted {
		return nil, conflict("批次 %s 当前状态 %s 不是中止", lotID, lot.Status)
	}
	ev, dup, err := s.emit(EventLotResumed, LotResumed{LotID: lotID, RestoredStatus: lot.PreHaltStatus}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// Depart 记录起运离港,要求已放行。
func (s *Service) Depart(lotID string, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusReleased {
		return nil, conflict("批次 %s 当前状态 %s 未放行,不能起运", lotID, lot.Status)
	}
	ev, dup, err := s.emit(EventLotDeparted, LotDeparted{LotID: lotID, FlightID: lot.FlightID}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// Deliver 记录目的地交付。
func (s *Service) Deliver(lotID string, m Meta) (*CommandResult, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	if lot.Status != StatusInTransit {
		return nil, conflict("批次 %s 当前状态 %s 不在途,不能交付", lotID, lot.Status)
	}
	ev, dup, err := s.emit(EventLotDelivered, LotDelivered{LotID: lotID}, m)
	if err != nil {
		return nil, err
	}
	lot, _ = s.store.Lot(lotID)
	return &CommandResult{Duplicate: dup, EventIDs: []string{ev.ID}, Lot: lot}, nil
}

// --- 查询 ---

// ScanView 是扫码返回的视图:当前责任方、剩余时限、监管结论与允许目的地。
type ScanView struct {
	BoxCode               string    `json:"box_code"`
	LotID                 string    `json:"lot_id"`
	Status                LotStatus `json:"status"`
	ResponsibleParty      string    `json:"responsible_party"`
	RemainingSeconds      int64     `json:"remaining_seconds"`
	SurvivalDeadline      time.Time `json:"survival_deadline"`
	RegulatoryConclusion  string    `json:"regulatory_conclusion"`
	ConclusionLabel       string    `json:"conclusion_label"`
	AllowedDestinations   []string  `json:"allowed_destinations"`
	Priority              Priority  `json:"priority"`
	CanReallocate         bool      `json:"can_reallocate"`
	ReallocationSuggested bool      `json:"reallocation_suggested"`
	HaltStep              string    `json:"halt_step,omitempty"`
	HaltReason            string    `json:"halt_reason,omitempty"`
}

// ScanBox 扫描箱码,返回当前责任方、剩余时限、监管结论与允许目的地。
func (s *Service) ScanBox(boxCode string) (*ScanView, error) {
	lot, ok := s.store.LotByBox(boxCode)
	if !ok {
		return nil, notFound("箱码 %s 未登记", boxCode)
	}
	now := s.store.Now()
	th := s.store.Thresholds()
	remaining := lot.SurvivalDeadline.Sub(now)
	// 处置优先级取已记录等级与按剩余时限推算等级的较高者。
	priority := lot.Priority
	if remaining < th.CriticalPriorityRemaining {
		if priorityRank(priority) < priorityRank(PriorityCritical) {
			priority = PriorityCritical
		}
	} else if remaining < th.HighPriorityRemaining && priorityRank(priority) < priorityRank(PriorityHigh) {
		priority = PriorityHigh
	}
	conclusion, label := regulatoryConclusion(lot)
	return &ScanView{
		BoxCode:               boxCode,
		LotID:                 lot.LotID,
		Status:                lot.Status,
		ResponsibleParty:      lot.Custodian,
		RemainingSeconds:      int64(remaining.Seconds()),
		SurvivalDeadline:      lot.SurvivalDeadline,
		RegulatoryConclusion:  conclusion,
		ConclusionLabel:       label,
		AllowedDestinations:   s.allowedDestinations(lot),
		Priority:              priority,
		CanReallocate:         lot.Status.Reallocatable(),
		ReallocationSuggested: lot.ReallocSuggested,
		HaltStep:              lot.HaltStep,
		HaltReason:            lot.HaltReason,
	}, nil
}

// regulatoryConclusion 汇总监管结论。
func regulatoryConclusion(lot *LotView) (code, label string) {
	switch lot.Status {
	case StatusHolding, StatusAllocated:
		return "pending_inspection", "待检查"
	case StatusInspecting:
		return "inspecting", "检查中"
	case StatusSealed:
		return "joint_verified_sealed", "联合核验通过,已封运"
	case StatusReleased, StatusInTransit:
		return "released", "已放行"
	case StatusDelivered:
		return "delivered", "已交付"
	case StatusHalted:
		return "halted", "已中止"
	default:
		return "closed", "已拆分/合并"
	}
}

// allowedDestinations 计算批次当前允许前往的目的地。
// 已分配/已封运/已放行的批次锁定在订单目的地;未分配的在场批次
// 可前往任一未变更订单的目的地。
func (s *Service) allowedDestinations(lot *LotView) []string {
	if lot.Destination != "" {
		return []string{lot.Destination}
	}
	if !lot.Status.Reallocatable() {
		return []string{}
	}
	set := map[string]bool{}
	for _, o := range s.store.Orders() {
		if !o.Changed {
			set[o.Destination] = true
		}
	}
	out := make([]string, 0, len(set))
	for d := range set {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// ReallocExplain 解释一次改配。
type ReallocExplain struct {
	EventID         string         `json:"event_id"`
	Trigger         ReallocTrigger `json:"trigger"`
	Reason          string         `json:"reason,omitempty"`
	CausedBy        string         `json:"caused_by,omitempty"`
	CausedBySummary string         `json:"caused_by_summary,omitempty"`
	OccurredAt      time.Time      `json:"occurred_at"`
}

// ReleaseExplain 解释一次放行及其依据。
type ReleaseExplain struct {
	EventID            string    `json:"event_id"`
	ReleaseID          string    `json:"release_id"`
	Authority          string    `json:"authority"`
	Basis              string    `json:"basis"`
	InspectionEventIDs []string  `json:"inspection_event_ids"`
	SealEventID        string    `json:"seal_event_id"`
	OccurredAt         time.Time `json:"occurred_at"`
}

// HaltExplain 解释批次在哪一步被中止。
type HaltExplain struct {
	EventID         string    `json:"event_id"`
	Step            string    `json:"step"`
	Reason          string    `json:"reason,omitempty"`
	CausedBy        string    `json:"caused_by,omitempty"`
	CausedBySummary string    `json:"caused_by_summary,omitempty"`
	OccurredAt      time.Time `json:"occurred_at"`
}

// SealBreakExplain 解释一次开封。
type SealBreakExplain struct {
	EventID    string    `json:"event_id"`
	SealID     string    `json:"seal_id"`
	ReturnStep string    `json:"return_step"`
	Reason     string    `json:"reason,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}

// Explanation 回答"为何一次放行、为何改配、在哪一步被中止"。
type Explanation struct {
	Release       *ReleaseExplain    `json:"release,omitempty"`
	Reallocations []ReallocExplain   `json:"reallocations,omitempty"`
	Halt          *HaltExplain       `json:"halt,omitempty"`
	SealBreaks    []SealBreakExplain `json:"seal_breaks,omitempty"`
}

// LotDetail 是批次详情:当前视图加谱系解释。
type LotDetail struct {
	Lot         *LotView     `json:"lot"`
	Explanation *Explanation `json:"explanation"`
}

// GetLot 返回批次详情与事后解释。
func (s *Service) GetLot(lotID string) (*LotDetail, error) {
	lot, err := s.lotView(lotID)
	if err != nil {
		return nil, err
	}
	return &LotDetail{Lot: lot, Explanation: s.explain(lotID)}, nil
}

// explain 扫描批次谱系链上的事件,归纳放行依据、改配原因与中止环节。
func (s *Service) explain(lotID string) *Explanation {
	ex := &Explanation{}
	events := s.store.LotEvents(lotID)
	summaryOf := func(id string) string {
		ev, ok := s.store.EventByID(id)
		if !ok {
			return ""
		}
		return fmt.Sprintf("%s(%s)", ev.Type, ev.ID)
	}
	var inspectionIDs []string
	var sealID string
	for _, ev := range events {
		switch ev.Type {
		case EventInspectionRecorded:
			var p InspectionRecorded
			if ev.Decode(&p) == nil && p.Result == InspectionPass {
				inspectionIDs = append(inspectionIDs, ev.ID)
			}
		case EventLotSealed:
			sealID = ev.ID
		case EventLotReleased:
			var p LotReleased
			if ev.Decode(&p) == nil {
				ex.Release = &ReleaseExplain{
					EventID:            ev.ID,
					ReleaseID:          p.ReleaseID,
					Authority:          p.Authority,
					Basis:              "安检、海关、联合核验均通过并施加封识,凭放行号一次性放行",
					InspectionEventIDs: inspectionIDs,
					SealEventID:        sealID,
					OccurredAt:         ev.OccurredAt,
				}
			}
		case EventLotReallocated:
			var p LotReallocated
			if ev.Decode(&p) == nil {
				ex.Reallocations = append(ex.Reallocations, ReallocExplain{
					EventID:         ev.ID,
					Trigger:         p.Trigger,
					Reason:          ev.Reason,
					CausedBy:        ev.CausedBy,
					CausedBySummary: summaryOf(ev.CausedBy),
					OccurredAt:      ev.OccurredAt,
				})
			}
		case EventLotHalted:
			var p LotHalted
			if ev.Decode(&p) == nil {
				ex.Halt = &HaltExplain{
					EventID:         ev.ID,
					Step:            p.Step,
					Reason:          ev.Reason,
					CausedBy:        ev.CausedBy,
					CausedBySummary: summaryOf(ev.CausedBy),
					OccurredAt:      ev.OccurredAt,
				}
			}
		case EventSealBroken:
			var p SealBroken
			if ev.Decode(&p) == nil {
				ex.SealBreaks = append(ex.SealBreaks, SealBreakExplain{
					EventID:    ev.ID,
					SealID:     p.SealID,
					ReturnStep: p.Step,
					Reason:     ev.Reason,
					OccurredAt: ev.OccurredAt,
				})
			}
		}
	}
	return ex
}

// LotHistory 返回批次谱系链上的全部事件(含拆分合并来源)。
func (s *Service) LotHistory(lotID string) ([]Event, error) {
	if _, err := s.lotView(lotID); err != nil {
		return nil, err
	}
	return s.store.LotEvents(lotID), nil
}

// ListLots 返回全部批次,按批次号排序。
func (s *Service) ListLots() []*LotView {
	lots := s.store.Lots()
	sort.Slice(lots, func(i, j int) bool { return lots[i].LotID < lots[j].LotID })
	return lots
}
