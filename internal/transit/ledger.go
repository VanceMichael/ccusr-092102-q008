package transit

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// 每次环境指标越界从存活预算中扣减的时间。
const violationPenalty = 20 * time.Minute

// 决策日志条目类别。
const (
	LogRegister         = "register"
	LogPick             = "pick"
	LogInspection       = "inspection"
	LogJointCheck       = "joint_check"
	LogSeal             = "seal"
	LogRelease          = "release"
	LogReleaseDuplicate = "release_duplicate"
	LogReleaseConflict  = "release_conflict"
	LogOpen             = "open"
	LogSplit            = "split"
	LogMerge            = "merge"
	LogReassign         = "reassign"
	LogHandover         = "handover"
	LogDiscrepancy      = "discrepancy"
	LogHalt             = "halt"
	LogResume           = "resume"
	LogPriority         = "priority"
	LogViolation        = "violation"
	LogFlightDelay      = "flight_delay"
	LogOrderChange      = "order_change"
	LogLoad             = "load"
	LogDepart           = "depart"
	LogArrive           = "arrive"
	LogDeliver          = "deliver"
)

// Error 是带类别的业务错误,HTTP 层据此映射状态码。
type Error struct {
	Kind string `json:"kind"` // not_found / invalid / conflict
	Msg  string `json:"msg"`
}

func (e *Error) Error() string { return e.Msg }

func notFoundf(format string, args ...any) *Error {
	return &Error{Kind: "not_found", Msg: fmt.Sprintf(format, args...)}
}
func invalidf(format string, args ...any) *Error {
	return &Error{Kind: "invalid", Msg: fmt.Sprintf(format, args...)}
}
func conflictf(format string, args ...any) *Error {
	return &Error{Kind: "conflict", Msg: fmt.Sprintf(format, args...)}
}

// Ledger 是事件日志的内存投影:批次、箱、航班、订单。
type Ledger struct {
	lots    map[string]*Lot
	boxes   map[string]*Box
	flights map[string]*Flight
	orders  map[string]*Order
}

// NewLedger 创建空台账。
func NewLedger() *Ledger {
	return &Ledger{
		lots:    make(map[string]*Lot),
		boxes:   make(map[string]*Box),
		flights: make(map[string]*Flight),
		orders:  make(map[string]*Order),
	}
}

func (l *Ledger) log(lot *Lot, e Event, kind, summary, detail string) {
	lot.DecisionLog = append(lot.DecisionLog, DecisionEntry{
		Seq: e.Seq, Time: e.OccurredAt, Kind: kind, Summary: summary, Detail: detail,
	})
}

// weightTolerance 是交接/交付活重校验的容差。
func weightTolerance(expected float64) float64 {
	if t := expected * 0.005; t > 0.5 {
		return t
	}
	return 0.5
}

// Apply 把一条事件应用到投影。回放与在线共用同一路径,保证一致。
func (l *Ledger) Apply(e Event) {
	switch e.Type {
	case EvLotRegistered:
		l.applyLotRegistered(e)
	case EvLotPicked:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			t := e.OccurredAt
			lot.Stage = StagePicked
			lot.PickedAt = &t
			l.log(lot, e, LogPick, fmt.Sprintf("出库,存活时钟启动(预算 %.1f 小时)", lot.SurvivalBudget.Hours()), "")
		}
	case EvObservation:
		l.applyObservation(e)
	case EvHandover:
		l.applyHandover(e)
	case EvJointCheckPassed:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[JointCheckPassedPayload](e)
			t := e.OccurredAt
			lot.Joint.PassedAt = &t
			l.log(lot, e, LogJointCheck, "联合核验通过:死亡抽检、封识、安检均合格", p.Basis)
		}
	case EvLotSealed:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[SealPayload](e)
			t := e.OccurredAt
			lot.Sealed = true
			lot.SealCode = p.SealCode
			lot.SealedAt = &t
			lot.Stage = StageSealed
			l.log(lot, e, LogSeal, fmt.Sprintf("已封运(封识号 %s),改配窗口关闭", p.SealCode), "")
		}
	case EvLotReleased:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[ReleasePayload](e)
			t := e.OccurredAt
			lot.Released = true
			lot.ReleaseDecisionID = p.DecisionID
			lot.ReleaseSourceKey = p.SourceKey
			lot.ReleasedAt = &t
			lot.Stage = StageReleased
			l.log(lot, e, LogRelease, fmt.Sprintf("海关一次放行(决定号 %s)", p.DecisionID), p.Basis)
		}
	case EvReleaseDuplicate:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[ReleaseDupPayload](e)
			lot.ReleaseDuplicates++
			l.log(lot, e, LogReleaseDuplicate,
				fmt.Sprintf("重复放行回调已忽略(决定号 %s),保持一次放行", p.DecisionID), "")
		}
	case EvReleaseConflict:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[ReleaseConflictPayload](e)
			lot.ReleaseConflicts++
			l.log(lot, e, LogReleaseConflict,
				fmt.Sprintf("收到另一放行决定 %s,与已生效的 %s 冲突,保持原放行", p.RejectedDecisionID, p.KeptDecisionID), "")
		}
	case EvLotOpened:
		l.applyLotOpened(e)
	case EvLotSplit:
		l.applyLotSplit(e)
	case EvLotsMerged:
		l.applyLotsMerged(e)
	case EvLotRebound:
		l.applyLotRebound(e)
	case EvLotLoaded:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			lot.Stage = StageLoaded
			l.log(lot, e, LogLoad, fmt.Sprintf("已装机(航班 %s)", lot.FlightID), "")
		}
	case EvLotDelivered:
		l.applyLotDelivered(e)
	case EvLotHalted:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[HaltPayload](e)
			lot.Halted = true
			lot.HaltKind = p.Kind
			lot.HaltReason = p.Reason
			lot.HaltedAtStage = lot.Stage
			l.log(lot, e, LogHalt, fmt.Sprintf("批次中止于%s:%s", StageLabel(lot.Stage), p.Reason), "")
		}
	case EvLotResumed:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[ResumePayload](e)
			lot.Halted = false
			lot.HaltKind = ""
			lot.HaltReason = ""
			lot.HaltedAtStage = ""
			l.log(lot, e, LogResume, fmt.Sprintf("中止解除:%s", p.Reason), "")
		}
	case EvFlightRegistered:
		p, _ := decodePayload[RegisterFlightPayload](e)
		l.flights[p.FlightID] = &Flight{
			ID: p.FlightID, Destination: p.Destination,
			LoadingCutoff: p.LoadingCutoff, DepartAt: p.DepartAt, Status: "SCHEDULED",
		}
	case EvFlightDelayed:
		l.applyFlightDelayed(e)
	case EvFlightDeparted:
		l.applyFlightMoved(e, "DEPARTED", StageLoaded, StageDeparted, LogDepart, "航班 %s 已起飞")
	case EvFlightArrived:
		l.applyFlightMoved(e, "ARRIVED", StageDeparted, StageArrived, LogArrive, "航班 %s 已到港")
	case EvOrderChanged:
		l.applyOrderChanged(e)
	case EvMetricViolated:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[MetricViolatedPayload](e)
			lot.Violations++
			l.log(lot, e, LogViolation,
				fmt.Sprintf("指标越界:%s 读数 %.2f(%s)", p.Kind, p.Value, p.Threshold), "")
		}
	case EvPriorityRaised:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[PriorityRaisedPayload](e)
			lot.Priority = p.To
			l.log(lot, e, LogPriority,
				fmt.Sprintf("处置优先级提升:%s → %s(%s)", p.From, p.To, p.Reason), "")
		}
	case EvHandoverDiscrepancy:
		if lot := l.lots[payloadLotID(e)]; lot != nil {
			p, _ := decodePayload[DiscrepancyPayload](e)
			lot.Halted = true
			lot.HaltKind = "discrepancy"
			lot.HaltReason = fmt.Sprintf("%s守恒校验失败:申报 %d 箱/%.1fkg,账面 %d 箱/%.1fkg",
				p.Context, p.DeclaredCount, p.DeclaredWeight, p.ExpectedCount, p.ExpectedWeight)
			lot.HaltedAtStage = lot.Stage
			l.log(lot, e, LogDiscrepancy, lot.HaltReason+",批次中止待核查", "")
		}
	}
}

func payloadLotID(e Event) string {
	var p struct {
		LotID string `json:"lot_id"`
	}
	_ = json.Unmarshal(e.Payload, &p)
	return p.LotID
}

func (l *Ledger) applyLotRegistered(e Event) {
	p, _ := decodePayload[RegisterLotPayload](e)
	lot := &Lot{
		ID:             p.LotID,
		CatchBatch:     p.CatchBatch,
		Tank:           p.Tank,
		Boxes:          make(map[string]*Box, len(p.Boxes)),
		OrderID:        p.OrderID,
		Destination:    p.Destination,
		FlightID:       p.FlightID,
		Stage:          StageHolding,
		Custodian:      p.Custodian,
		Priority:       PriorityNormal,
		SurvivalBudget: time.Duration(p.SurvivalBudgetHours * float64(time.Hour)),
		TempMin:        p.TempMin,
		TempMax:        p.TempMax,
		DOMin:          p.DOMin,
		CreatedAt:      e.OccurredAt,
	}
	for _, bs := range p.Boxes {
		box := &Box{
			Code: bs.Code, LotID: lot.ID, CatchBatch: p.CatchBatch,
			InitialWeightKg: bs.WeightKg, LiveWeightKg: bs.WeightKg,
		}
		lot.Boxes[bs.Code] = box
		l.boxes[bs.Code] = box
		lot.InitialWeightKg += bs.WeightKg
	}
	l.lots[lot.ID] = lot
	if p.OrderID != "" {
		l.orders[p.OrderID] = &Order{ID: p.OrderID, Destination: p.Destination}
	}
	if p.FlightID != "" && l.flights[p.FlightID] == nil {
		l.flights[p.FlightID] = &Flight{ID: p.FlightID, Destination: p.Destination, Status: "SCHEDULED"}
	}
	l.log(lot, e, LogRegister,
		fmt.Sprintf("批次注册:%d 箱 %.1fkg,捕捞批次 %s,暂养池 %s,订单 %s,目的地 %s",
			len(p.Boxes), lot.InitialWeightKg, p.CatchBatch, p.Tank, p.OrderID, p.Destination), "")
}

func (l *Ledger) applyObservation(e Event) {
	p, _ := decodePayload[ObservationPayload](e)
	lot := l.lots[p.LotID]
	if lot == nil {
		return
	}
	t := e.OccurredAt
	switch p.Kind {
	case ObsMortality:
		l.applyMortality(lot, p.DeadWeightKg, p.BoxCode)
		lot.DeadCount += p.DeadCount
		if p.Conclusion == "pass" {
			lot.Joint.MortalityPass = true
			lot.Joint.MortalityAt = &t
		}
		l.log(lot, e, LogInspection, fmt.Sprintf(
			"死亡抽检:死亡 %d 只/%.1fkg,累计死亡率 %.1f%%,结论 %s",
			p.DeadCount, p.DeadWeightKg, lot.MortalityRate()*100, p.Conclusion), p.Note)
	case ObsSeal:
		if p.Conclusion == "pass" {
			lot.Joint.SealOK = true
			lot.Joint.SealAt = &t
		}
	case ObsSecurity:
		if p.Conclusion == "pass" {
			lot.Joint.SecurityPass = true
			lot.Joint.SecurityAt = &t
		}
	case ObsCustoms:
		lot.Customs = CustomsStatus{Conclusion: p.Conclusion, DecisionID: p.DecisionID, At: &t}
	}
	// 收到联合核验类观测后,环节推进到联合核验中
	if lot.Stage == StagePicked &&
		(p.Kind == ObsMortality || p.Kind == ObsSeal || p.Kind == ObsSecurity) {
		lot.Stage = StageJointCheck
	}
}

// applyMortality 把死亡重量从指定箱(或全批按比例)的活重中扣减。
func (l *Ledger) applyMortality(lot *Lot, deadKg float64, boxCode string) {
	if deadKg <= 0 {
		return
	}
	var targets []*Box
	if boxCode != "" {
		if b := lot.Boxes[boxCode]; b != nil {
			targets = append(targets, b)
		}
	} else {
		codes := make([]string, 0, len(lot.Boxes))
		for c := range lot.Boxes {
			codes = append(codes, c)
		}
		sort.Strings(codes)
		for _, c := range codes {
			targets = append(targets, lot.Boxes[c])
		}
	}
	var total float64
	for _, b := range targets {
		total += b.LiveWeightKg
	}
	if total <= 0 {
		return
	}
	remaining := deadKg
	for i, b := range targets {
		share := remaining // 最后一箱承担尾差,保证总量精确
		if i < len(targets)-1 {
			share = deadKg * b.LiveWeightKg / total
		}
		if share > b.LiveWeightKg {
			share = b.LiveWeightKg
		}
		b.LiveWeightKg -= share
		remaining -= share
	}
	lot.DeadWeightKg += deadKg
}

func (l *Ledger) applyHandover(e Event) {
	p, _ := decodePayload[HandoverPayload](e)
	lot := l.lots[p.LotID]
	if lot == nil {
		return
	}
	if !l.conservationOK(lot, p.BoxCount, p.LiveWeightKg) {
		return // 守恒失败:不更新责任方,由派生事件记录差异并中止
	}
	lot.Custodian = p.ToParty
	if p.Vehicle != "" {
		lot.Vehicle = p.Vehicle
	}
	l.log(lot, e, LogHandover, fmt.Sprintf("交接 %s → %s:%d 箱 %.1fkg,箱数与活重守恒",
		p.FromParty, p.ToParty, p.BoxCount, p.LiveWeightKg), p.Note)
}

func (l *Ledger) conservationOK(lot *Lot, count int, weight float64) bool {
	expected := lot.LiveWeight()
	if count != lot.BoxCount() {
		return false
	}
	diff := weight - expected
	if diff < 0 {
		diff = -diff
	}
	return diff <= weightTolerance(expected)
}

func (l *Ledger) applyLotOpened(e Event) {
	p, _ := decodePayload[OpenPayload](e)
	lot := l.lots[p.LotID]
	if lot == nil {
		return
	}
	lot.Sealed = false
	lot.SealCode = ""
	lot.SealedAt = nil
	lot.OpenCount++
	lot.Stage = StageJointCheck
	lot.Joint = JointCheck{}
	voided := ""
	if lot.Released {
		voided = lot.ReleaseDecisionID
		lot.VoidedDecisionID = voided
		lot.Released = false
		lot.ReleasedAt = nil
		lot.ReleaseDecisionID = ""
	}
	lot.Customs = CustomsStatus{}
	detail := ""
	if voided != "" {
		detail = fmt.Sprintf("原放行决定 %s 作废,须重新联合核验并放行", voided)
	}
	l.log(lot, e, LogOpen, fmt.Sprintf("开封(%s),回到联合核验环节", p.Reason), detail)
}

func (l *Ledger) applyLotSplit(e Event) {
	p, _ := decodePayload[SplitPayload](e)
	parent := l.lots[p.LotID]
	if parent == nil {
		return
	}
	child := &Lot{
		ID:             p.NewLotID,
		CatchBatch:     parent.CatchBatch,
		Tank:           parent.Tank,
		ParentLotID:    parent.ID,
		Boxes:          make(map[string]*Box, len(p.BoxCodes)),
		OrderID:        parent.OrderID,
		Destination:    parent.Destination,
		FlightID:       parent.FlightID,
		Vehicle:        parent.Vehicle,
		Stage:          parent.Stage,
		Custodian:      parent.Custodian,
		Joint:          parent.Joint,
		Customs:        parent.Customs,
		Priority:       parent.Priority,
		Violations:     parent.Violations,
		PickedAt:       parent.PickedAt,
		SurvivalBudget: parent.SurvivalBudget,
		TempMin:        parent.TempMin,
		TempMax:        parent.TempMax,
		DOMin:          parent.DOMin,
		CreatedAt:      e.OccurredAt,
	}
	if p.OrderID != "" {
		child.OrderID = p.OrderID
	}
	if p.Destination != "" {
		child.Destination = p.Destination
	}
	if p.FlightID != "" {
		child.FlightID = p.FlightID
	}
	for _, code := range p.BoxCodes {
		box := parent.Boxes[code]
		if box == nil {
			continue
		}
		delete(parent.Boxes, code)
		box.LotID = child.ID
		child.Boxes[code] = box
		child.InitialWeightKg += box.InitialWeightKg
		parent.InitialWeightKg -= box.InitialWeightKg
	}
	// 死亡重量按初始重量比例随拆分转移,保持两侧守恒
	if parent.InitialWeightKg+child.InitialWeightKg > 0 {
		share := parent.DeadWeightKg * child.InitialWeightKg / (parent.InitialWeightKg + child.InitialWeightKg)
		child.DeadWeightKg = share
		parent.DeadWeightKg -= share
	}
	l.lots[child.ID] = child
	if child.OrderID != "" {
		l.orders[child.OrderID] = &Order{ID: child.OrderID, Destination: child.Destination}
	}
	parent.ChildLotIDs = append(parent.ChildLotIDs, child.ID)
	if len(parent.Boxes) == 0 {
		parent.Closed = true
	}
	summary := fmt.Sprintf("拆分出子批次 %s:%d 箱 %.1fkg(%s)", child.ID, child.BoxCount(), child.LiveWeight(), p.Reason)
	l.log(parent, e, LogSplit, summary, "")
	l.log(child, e, LogSplit, fmt.Sprintf("由批次 %s 拆分而来,去向 %s", parent.ID, child.Destination), p.Reason)
}

func (l *Ledger) applyLotsMerged(e Event) {
	p, _ := decodePayload[MergePayload](e)
	if len(p.LotIDs) == 0 {
		return
	}
	first := l.lots[p.LotIDs[0]]
	if first == nil {
		return
	}
	merged := &Lot{
		ID:               p.NewLotID,
		CatchBatch:       first.CatchBatch,
		Tank:             first.Tank,
		MergedFromLotIDs: append([]string(nil), p.LotIDs...),
		Boxes:            make(map[string]*Box),
		OrderID:          first.OrderID,
		Destination:      first.Destination,
		FlightID:         first.FlightID,
		Vehicle:          first.Vehicle,
		Stage:            first.Stage,
		Custodian:        first.Custodian,
		Priority:         first.Priority,
		PickedAt:         first.PickedAt,
		SurvivalBudget:   first.SurvivalBudget,
		TempMin:          first.TempMin,
		TempMax:          first.TempMax,
		DOMin:            first.DOMin,
		CreatedAt:        e.OccurredAt,
	}
	merged.Joint = first.Joint
	for _, id := range p.LotIDs {
		src := l.lots[id]
		if src == nil || id == p.NewLotID {
			continue
		}
		for code, box := range src.Boxes {
			box.LotID = merged.ID
			merged.Boxes[code] = box
		}
		merged.InitialWeightKg += src.InitialWeightKg
		merged.DeadWeightKg += src.DeadWeightKg
		merged.DeadCount += src.DeadCount
		merged.Violations += src.Violations
		if priorityRank(src.Priority) > priorityRank(merged.Priority) {
			merged.Priority = src.Priority
		}
		// 合并后的联合核验取各方结论的交集
		merged.Joint.MortalityPass = merged.Joint.MortalityPass && src.Joint.MortalityPass
		merged.Joint.SealOK = merged.Joint.SealOK && src.Joint.SealOK
		merged.Joint.SecurityPass = merged.Joint.SecurityPass && src.Joint.SecurityPass
		if src.Joint.PassedAt == nil {
			merged.Joint.PassedAt = nil
		}
		if src.PickedAt != nil && (merged.PickedAt == nil || src.PickedAt.Before(*merged.PickedAt)) {
			merged.PickedAt = src.PickedAt
		}
		src.Boxes = map[string]*Box{}
		src.Closed = true
		l.log(src, e, LogMerge, fmt.Sprintf("并入批次 %s(%s)", merged.ID, p.Reason), "")
	}
	l.lots[merged.ID] = merged
	l.log(merged, e, LogMerge, fmt.Sprintf("由 %d 个批次合并而成:%d 箱 %.1fkg",
		len(p.LotIDs), merged.BoxCount(), merged.LiveWeight()), p.Reason)
}

func (l *Ledger) applyLotRebound(e Event) {
	p, _ := decodePayload[RebindPayload](e)
	lot := l.lots[p.LotID]
	if lot == nil {
		return
	}
	var changes []string
	if p.OrderID != "" && p.OrderID != lot.OrderID {
		changes = append(changes, fmt.Sprintf("订单 %s→%s", lot.OrderID, p.OrderID))
		lot.OrderID = p.OrderID
		l.orders[p.OrderID] = &Order{ID: p.OrderID, Destination: lot.Destination}
	}
	if p.Destination != "" && p.Destination != lot.Destination {
		changes = append(changes, fmt.Sprintf("目的地 %s→%s", lot.Destination, p.Destination))
		lot.Destination = p.Destination
	}
	if p.FlightID != "" && p.FlightID != lot.FlightID {
		changes = append(changes, fmt.Sprintf("航班 %s→%s", lot.FlightID, p.FlightID))
		lot.FlightID = p.FlightID
		if l.flights[p.FlightID] == nil {
			l.flights[p.FlightID] = &Flight{ID: p.FlightID, Destination: lot.Destination, Status: "SCHEDULED"}
		}
	}
	if len(changes) == 0 {
		changes = append(changes, "绑定信息不变")
	}
	l.log(lot, e, LogReassign, fmt.Sprintf("改配:%s(%s)", strings.Join(changes, ","), p.Reason), "")
}

func (l *Ledger) applyLotDelivered(e Event) {
	p, _ := decodePayload[DeliverPayload](e)
	lot := l.lots[p.LotID]
	if lot == nil {
		return
	}
	if !l.conservationOK(lot, p.BoxCount, p.LiveWeightKg) {
		return // 由派生事件记录差异并中止
	}
	lot.Stage = StageDelivered
	lot.Custodian = p.ToParty
	l.log(lot, e, LogDeliver, fmt.Sprintf("目的地交付 %s:%d 箱 %.1fkg,守恒确认",
		p.ToParty, p.BoxCount, p.LiveWeightKg), "")
}

func (l *Ledger) applyFlightDelayed(e Event) {
	p, _ := decodePayload[DelayFlightPayload](e)
	f := l.flights[p.FlightID]
	if f == nil {
		return
	}
	f.LoadingCutoff = p.NewCutoff
	f.DepartAt = p.NewDepartAt
	f.DelayReason = p.Reason
	f.DelayCount++
	for _, lot := range l.boundLots(p.FlightID) {
		var summary string
		if preSeal(lot.Stage) {
			summary = fmt.Sprintf("航班延误:新截载 %s(%s);批次尚未封运,可在新截载前改配",
				p.NewCutoff.Format("15:04"), p.Reason)
		} else {
			summary = fmt.Sprintf("航班延误:新截载 %s(%s);批次已封运,保持原绑定",
				p.NewCutoff.Format("15:04"), p.Reason)
		}
		l.log(lot, e, LogFlightDelay, summary, "")
		if d, ok := lot.survivalDeadline(); ok && d.Before(p.NewCutoff) && preSeal(lot.Stage) {
			l.log(lot, e, LogFlightDelay, "存活时限不足以等待延误航班,建议改配更早航班", "")
		}
	}
}

func (l *Ledger) applyFlightMoved(e Event, flightStatus string, from, to Stage, kind, format string) {
	p, _ := decodePayload[FlightRefPayload](e)
	f := l.flights[p.FlightID]
	if f == nil {
		return
	}
	f.Status = flightStatus
	for _, lot := range l.boundLots(p.FlightID) {
		if lot.Stage == from {
			lot.Stage = to
			l.log(lot, e, kind, fmt.Sprintf(format, p.FlightID), "")
		}
	}
}

func (l *Ledger) applyOrderChanged(e Event) {
	p, _ := decodePayload[OrderChangePayload](e)
	order := l.orders[p.OrderID]
	if order == nil {
		return
	}
	order.Destination = p.Destination
	for _, lot := range l.lots {
		if lot.OrderID != p.OrderID || lot.Closed || lot.Stage == StageDelivered {
			continue
		}
		if preSeal(lot.Stage) {
			old := lot.Destination
			lot.Destination = p.Destination
			l.log(lot, e, LogOrderChange, fmt.Sprintf(
				"订单变化:目的地 %s→%s;批次尚未封运,跟随改配(%s)", old, p.Destination, p.Reason), "")
		} else {
			l.log(lot, e, LogOrderChange, fmt.Sprintf(
				"订单变化:目的地改为 %s;批次已封运,保持原目的地 %s", p.Destination, lot.Destination), "")
		}
	}
}

// boundLots 返回绑定到某航班的未关闭批次,按 ID 排序保证确定性。
func (l *Ledger) boundLots(flightID string) []*Lot {
	var ids []string
	for id, lot := range l.lots {
		if lot.FlightID == flightID && !lot.Closed && lot.Stage != StageDelivered {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	out := make([]*Lot, 0, len(ids))
	for _, id := range ids {
		out = append(out, l.lots[id])
	}
	return out
}

// survivalDeadline 返回存活时限(出库时间 + 预算 − 越界扣减)。
func (l *Lot) survivalDeadline() (time.Time, bool) {
	if l.PickedAt == nil || l.SurvivalBudget <= 0 {
		return time.Time{}, false
	}
	return l.PickedAt.Add(l.SurvivalBudget).Add(-violationPenalty * time.Duration(l.Violations)), true
}

// timeConstraints 计算批次当前的全部时限候选。
func (l *Ledger) timeConstraints(lot *Lot, now time.Time) []TimeConstraint {
	var out []TimeConstraint
	add := func(basis string, deadline time.Time) {
		rem := int64(deadline.Sub(now).Seconds())
		out = append(out, TimeConstraint{
			Basis: basis, Deadline: deadline, RemainingSeconds: rem, Exceeded: rem < 0,
		})
	}
	if d, ok := lot.survivalDeadline(); ok && stageRank[lot.Stage] < stageRank[StageDelivered] {
		add("存活时限", d)
	}
	if f := l.flights[lot.FlightID]; f != nil && !f.LoadingCutoff.IsZero() &&
		stageRank[lot.Stage] < stageRank[StageLoaded] {
		add("航班截载", f.LoadingCutoff)
	}
	return out
}

// computePriority 根据当前时限与指标计算应有的优先级。
func (l *Ledger) computePriority(lot *Lot, now time.Time) (Priority, string) {
	if lot.Closed || lot.Stage == StageDelivered {
		return PriorityNormal, ""
	}
	p := PriorityNormal
	reason := ""
	raise := func(np Priority, r string) {
		if priorityRank(np) > priorityRank(p) {
			p, reason = np, r
		}
	}
	var minRem *int64
	for _, c := range l.timeConstraints(lot, now) {
		r := c.RemainingSeconds
		if minRem == nil || r < *minRem {
			m := r
			minRem = &m
		}
	}
	if minRem != nil {
		switch {
		case *minRem < int64((45 * time.Minute).Seconds()):
			raise(PriorityCritical, "剩余时限不足 45 分钟,接近生存阈值")
		case *minRem < int64((2 * time.Hour).Seconds()):
			raise(PriorityHigh, "剩余时限不足 2 小时")
		}
	}
	if lot.Violations > 0 {
		raise(PriorityHigh, "存在环境指标越界")
	}
	if lot.MortalityRate() > 0.05 {
		raise(PriorityCritical, "死亡率超过 5%")
	}
	if lot.Halted {
		raise(PriorityHigh, "批次中止待处置")
	}
	return p, reason
}

func (l *Ledger) effectivePriority(lot *Lot, now time.Time) Priority {
	computed, _ := l.computePriority(lot, now)
	if priorityRank(computed) > priorityRank(lot.Priority) {
		return computed
	}
	return lot.Priority
}

// regulatorySummary 汇总当前监管结论。
func regulatorySummary(lot *Lot) string {
	if lot.Halted {
		return fmt.Sprintf("中止于%s:%s", StageLabel(lot.HaltedAtStage), lot.HaltReason)
	}
	var jc string
	switch {
	case lot.Joint.PassedAt != nil:
		jc = "联合核验通过"
	case lot.Joint.MortalityPass || lot.Joint.SealOK || lot.Joint.SecurityPass:
		var missing []string
		if !lot.Joint.MortalityPass {
			missing = append(missing, "死亡抽检")
		}
		if !lot.Joint.SealOK {
			missing = append(missing, "封识")
		}
		if !lot.Joint.SecurityPass {
			missing = append(missing, "安检")
		}
		jc = "联合核验进行中,待" + strings.Join(missing, "、")
	default:
		jc = "联合核验未开始"
	}
	seal := "未封运"
	if lot.Sealed {
		seal = fmt.Sprintf("已封运(封识号 %s)", lot.SealCode)
	}
	customs := "海关未申报"
	switch {
	case lot.Released:
		customs = fmt.Sprintf("海关已放行(决定号 %s,一次放行)", lot.ReleaseDecisionID)
	case lot.VoidedDecisionID != "":
		customs = "原放行已作废,待重新核验放行"
	case lot.Customs.Conclusion == "fail":
		customs = "海关查验不通过"
	case lot.Customs.Conclusion == "pass":
		customs = "海关结论合格,待封运后放行"
	}
	return jc + ";" + seal + ";" + customs
}

// ScanBox 是扫码应答:当前责任方、剩余时限、监管结论、允许目的地。
func (l *Ledger) ScanBox(code string, now time.Time) (*ScanResult, error) {
	box, ok := l.boxes[code]
	if !ok {
		return nil, notFoundf("箱码 %s 不存在", code)
	}
	lot := l.lots[box.LotID]
	if lot == nil {
		return nil, notFoundf("箱码 %s 所属批次丢失", code)
	}
	res := &ScanResult{
		BoxCode:      code,
		LotID:        lot.ID,
		CatchBatch:   lot.CatchBatch,
		Stage:        lot.Stage,
		StageLabel:   StageLabel(lot.Stage),
		Custodian:    lot.Custodian,
		Regulatory:   regulatorySummary(lot),
		Priority:     l.effectivePriority(lot, now),
		Halted:       lot.Halted,
		HaltReason:   lot.HaltReason,
		Constraints:  l.timeConstraints(lot, now),
		LiveWeightKg: box.LiveWeightKg,
	}
	res.AllowedDestination = lot.Destination
	res.DestinationLocked = lot.Sealed || lot.Released
	if len(res.Constraints) > 0 {
		min := res.Constraints[0]
		for _, c := range res.Constraints[1:] {
			if c.RemainingSeconds < min.RemainingSeconds {
				min = c
			}
		}
		rem := min.RemainingSeconds
		res.RemainingSeconds = &rem
		res.RemainingBasis = min.Basis
	}
	return res, nil
}

// Lineage 返回箱码的完整谱系。
func (l *Ledger) Lineage(code string) (*LineageView, error) {
	box, ok := l.boxes[code]
	if !ok {
		return nil, notFoundf("箱码 %s 不存在", code)
	}
	lot := l.lots[box.LotID]
	if lot == nil {
		return nil, notFoundf("箱码 %s 所属批次丢失", code)
	}
	// 沿父批次走到根,再反转为根→当前的链
	var chain []LotNode
	seen := map[string]bool{}
	for cur := lot; cur != nil && !seen[cur.ID]; cur = l.lots[cur.ParentLotID] {
		seen[cur.ID] = true
		chain = append([]LotNode{{
			LotID: cur.ID, ParentLotID: cur.ParentLotID,
			ChildLotIDs: cur.ChildLotIDs, MergedFrom: cur.MergedFromLotIDs,
			Stage: cur.Stage, StageLabel: StageLabel(cur.Stage),
			Closed: cur.Closed, BoxCount: cur.BoxCount(),
		}}, chain...)
	}
	return &LineageView{
		BoxCode:     code,
		CatchBatch:  lot.CatchBatch,
		Tank:        lot.Tank,
		LotChain:    chain,
		OrderID:     lot.OrderID,
		Vehicle:     lot.Vehicle,
		FlightID:    lot.FlightID,
		Destination: lot.Destination,
	}, nil
}

// Explain 返回批次的决策日志,可按关注点过滤:
// release(为何一次放行)/ reassign(为何改配)/ halt(在哪一步被中止)。
func (l *Ledger) Explain(lotID, focus string) ([]DecisionEntry, error) {
	lot := l.lots[lotID]
	if lot == nil {
		return nil, notFoundf("批次 %s 不存在", lotID)
	}
	var kinds map[string]bool
	switch focus {
	case "release":
		kinds = map[string]bool{LogRelease: true, LogReleaseDuplicate: true, LogReleaseConflict: true, LogJointCheck: true, LogSeal: true, LogOpen: true}
	case "reassign":
		kinds = map[string]bool{LogReassign: true, LogSplit: true, LogMerge: true, LogOrderChange: true, LogFlightDelay: true}
	case "halt":
		kinds = map[string]bool{LogHalt: true, LogDiscrepancy: true, LogResume: true, LogViolation: true, LogInspection: true}
	}
	out := make([]DecisionEntry, 0, len(lot.DecisionLog))
	for _, entry := range lot.DecisionLog {
		if kinds == nil || kinds[entry.Kind] {
			out = append(out, entry)
		}
	}
	return out, nil
}

// LotView 是批次详情视图。
type LotView struct {
	Lot               *Lot             `json:"lot"`
	Boxes             []*Box           `json:"boxes"`
	EffectivePriority Priority         `json:"effective_priority"`
	Regulatory        string           `json:"regulatory"`
	Constraints       []TimeConstraint `json:"time_constraints,omitempty"`
}

// LotDetail 返回批次详情。
func (l *Ledger) LotDetail(lotID string, now time.Time) (*LotView, error) {
	lot := l.lots[lotID]
	if lot == nil {
		return nil, notFoundf("批次 %s 不存在", lotID)
	}
	boxes := make([]*Box, 0, len(lot.Boxes))
	for _, b := range lot.Boxes {
		boxes = append(boxes, b)
	}
	sort.Slice(boxes, func(i, j int) bool { return boxes[i].Code < boxes[j].Code })
	return &LotView{
		Lot:               lot,
		Boxes:             boxes,
		EffectivePriority: l.effectivePriority(lot, now),
		Regulatory:        regulatorySummary(lot),
		Constraints:       l.timeConstraints(lot, now),
	}, nil
}

// Queue 返回在途处置队列:优先级高者优先,同级按剩余时限升序。
func (l *Ledger) Queue(now time.Time) []QueueItem {
	var items []QueueItem
	for _, lot := range l.lots {
		if lot.Closed || lot.Stage == StageDelivered {
			continue
		}
		item := QueueItem{
			LotID:        lot.ID,
			Stage:        lot.Stage,
			StageLabel:   StageLabel(lot.Stage),
			Priority:     l.effectivePriority(lot, now),
			Halted:       lot.Halted,
			HaltReason:   lot.HaltReason,
			Custodian:    lot.Custodian,
			Destination:  lot.Destination,
			BoxCount:     lot.BoxCount(),
			LiveWeightKg: lot.LiveWeight(),
		}
		cs := l.timeConstraints(lot, now)
		if len(cs) > 0 {
			min := cs[0].RemainingSeconds
			for _, c := range cs[1:] {
				if c.RemainingSeconds < min {
					min = c.RemainingSeconds
				}
			}
			item.RemainingSeconds = &min
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if priorityRank(items[i].Priority) != priorityRank(items[j].Priority) {
			return priorityRank(items[i].Priority) > priorityRank(items[j].Priority)
		}
		ri, rj := items[i].RemainingSeconds, items[j].RemainingSeconds
		switch {
		case ri == nil && rj == nil:
			return items[i].LotID < items[j].LotID
		case ri == nil:
			return false
		case rj == nil:
			return true
		case *ri != *rj:
			return *ri < *rj
		}
		return items[i].LotID < items[j].LotID
	})
	return items
}
