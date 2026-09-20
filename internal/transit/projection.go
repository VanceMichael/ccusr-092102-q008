package transit

import (
	"fmt"
	"maps"
	"slices"
	"time"
)

// LotView 是批次的当前状态投影,由事件重放得到。
type LotView struct {
	LotID            string             `json:"lot_id"`
	Status           LotStatus          `json:"status"`
	PreHaltStatus    LotStatus          `json:"pre_halt_status,omitempty"`
	CatchBatchIDs    []string           `json:"catch_batch_ids"`
	TankID           string             `json:"tank_id"`
	Custodian        string             `json:"custodian"`
	Boxes            map[string]float64 `json:"boxes"`
	BoxCount         int                `json:"box_count"`
	LiveWeightKg     float64            `json:"live_weight_kg"`
	OrderID          string             `json:"order_id,omitempty"`
	VehicleID        string             `json:"vehicle_id,omitempty"`
	FlightID         string             `json:"flight_id,omitempty"`
	Destination      string             `json:"destination,omitempty"`
	SealID           string             `json:"seal_id,omitempty"`
	ReleaseID        string             `json:"release_id,omitempty"`
	Inspections      map[string]string  `json:"inspections"`
	ParentLotIDs     []string           `json:"parent_lot_ids,omitempty"`
	ChildLotIDs      []string           `json:"child_lot_ids,omitempty"`
	Priority         Priority           `json:"priority"`
	PriorityReason   string             `json:"priority_reason,omitempty"`
	ReallocSuggested bool               `json:"reallocation_suggested"`
	ReallocReason    string             `json:"reallocation_reason,omitempty"`
	HaltStep         string             `json:"halt_step,omitempty"`
	HaltReason       string             `json:"halt_reason,omitempty"`
	HarvestedAt      time.Time          `json:"harvested_at"`
	SurvivalHours    float64            `json:"survival_hours"`
	SurvivalDeadline time.Time          `json:"survival_deadline"`
	ClosedBy         string             `json:"closed_by,omitempty"`
	Telemetry        []TelemetryRecord  `json:"telemetry,omitempty"`
}

func (v *LotView) clone() *LotView {
	c := *v
	c.CatchBatchIDs = slices.Clone(v.CatchBatchIDs)
	c.Boxes = maps.Clone(v.Boxes)
	c.Inspections = maps.Clone(v.Inspections)
	c.ParentLotIDs = slices.Clone(v.ParentLotIDs)
	c.ChildLotIDs = slices.Clone(v.ChildLotIDs)
	c.Telemetry = slices.Clone(v.Telemetry)
	return &c
}

// TelemetryRecord 是批次上追加的一条环境观测。
type TelemetryRecord struct {
	EventID      string        `json:"event_id"`
	DeviceID     string        `json:"device_id"`
	Kind         TelemetryKind `json:"kind"`
	Value        float64       `json:"value"`
	DeadCount    int           `json:"dead_count,omitempty"`
	DeadWeightKg float64       `json:"dead_weight_kg,omitempty"`
	OccurredAt   time.Time     `json:"occurred_at"`
}

// CatchBatchView 是捕捞批次投影,跟踪尚未入池的箱数与重量。
type CatchBatchView struct {
	CatchBatchID      string    `json:"catch_batch_id"`
	Species           string    `json:"species"`
	Origin            string    `json:"origin"`
	HarvestedAt       time.Time `json:"harvested_at"`
	SurvivalHours     float64   `json:"survival_hours"`
	TotalBoxes        int       `json:"total_boxes"`
	TotalWeightKg     float64   `json:"total_weight_kg"`
	RemainingBoxes    int       `json:"remaining_boxes"`
	RemainingWeightKg float64   `json:"remaining_weight_kg"`
}

// OrderView 是客户订单投影。
type OrderView struct {
	OrderID          string    `json:"order_id"`
	Customer         string    `json:"customer"`
	Destination      string    `json:"destination"`
	RequiredBoxes    int       `json:"required_boxes"`
	RequiredWeightKg float64   `json:"required_weight_kg"`
	LatestArrival    time.Time `json:"latest_arrival"`
	Changed          bool      `json:"changed"`
	ChangeNote       string    `json:"change_note,omitempty"`
}

// FlightView 是航班投影。
type FlightView struct {
	FlightID    string    `json:"flight_id"`
	Destination string    `json:"destination"`
	DepartureAt time.Time `json:"departure_at"`
	DelayCount  int       `json:"delay_count"`
}

// projection 是全部内存视图,只能由事件应用推进。
type projection struct {
	lots      map[string]*LotView
	boxes     map[string]string // box_code -> 当前所属 lot_id
	batches   map[string]*CatchBatchView
	orders    map[string]*OrderView
	flights   map[string]*FlightView
	lotEvents map[string][]string // lot_id -> 相关事件 ID(谱系与解释的数据源)
}

func newProjection() *projection {
	return &projection{
		lots:      map[string]*LotView{},
		boxes:     map[string]string{},
		batches:   map[string]*CatchBatchView{},
		orders:    map[string]*OrderView{},
		flights:   map[string]*FlightView{},
		lotEvents: map[string][]string{},
	}
}

// link 把事件挂到批次的谱系链上。
func (p *projection) link(lotID, eventID string) {
	p.lotEvents[lotID] = append(p.lotEvents[lotID], eventID)
}

// apply 把一条事件应用到投影。必须是确定性的:重放与实时应用结果一致。
func (p *projection) apply(ev Event) error {
	switch ev.Type {
	case EventCatchBatchRegistered:
		var pl CatchBatchRegistered
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		p.batches[pl.CatchBatchID] = &CatchBatchView{
			CatchBatchID:      pl.CatchBatchID,
			Species:           pl.Species,
			Origin:            pl.Origin,
			HarvestedAt:       pl.HarvestedAt,
			SurvivalHours:     pl.SurvivalHours,
			TotalBoxes:        pl.TotalBoxes,
			TotalWeightKg:     pl.TotalWeightKg,
			RemainingBoxes:    pl.TotalBoxes,
			RemainingWeightKg: pl.TotalWeightKg,
		}

	case EventBondedLotCreated:
		var pl BondedLotCreated
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		batch, ok := p.batches[pl.CatchBatchID]
		if !ok {
			return fmt.Errorf("捕捞批次 %s 不存在", pl.CatchBatchID)
		}
		lot := &LotView{
			LotID:            pl.LotID,
			Status:           StatusHolding,
			CatchBatchIDs:    []string{pl.CatchBatchID},
			TankID:           pl.TankID,
			Custodian:        pl.Custodian,
			Boxes:            map[string]float64{},
			Inspections:      map[string]string{},
			Priority:         PriorityNormal,
			HarvestedAt:      batch.HarvestedAt,
			SurvivalHours:    batch.SurvivalHours,
			SurvivalDeadline: batch.HarvestedAt.Add(time.Duration(batch.SurvivalHours * float64(time.Hour))),
		}
		for _, b := range pl.Boxes {
			lot.Boxes[b.BoxCode] = b.WeightKg
			lot.LiveWeightKg += b.WeightKg
			p.boxes[b.BoxCode] = pl.LotID
		}
		lot.BoxCount = len(lot.Boxes)
		batch.RemainingBoxes -= lot.BoxCount
		batch.RemainingWeightKg -= lot.LiveWeightKg
		p.lots[pl.LotID] = lot
		p.link(pl.LotID, ev.ID)

	case EventLotSplit:
		var pl LotSplit
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		parent := p.lots[pl.ParentLotID]
		// 先保存父批次状态,子批次原样继承(拆分只允许在封运前进行)。
		parentStatus := parent.Status
		parent.Status = StatusClosed
		parent.ClosedBy = "split"
		p.link(pl.ParentLotID, ev.ID)
		for _, ch := range pl.Children {
			child := &LotView{
				LotID:            ch.LotID,
				Status:           parentStatus,
				CatchBatchIDs:    slices.Clone(parent.CatchBatchIDs),
				TankID:           ch.TankID,
				Custodian:        parent.Custodian,
				Boxes:            map[string]float64{},
				OrderID:          parent.OrderID,
				VehicleID:        parent.VehicleID,
				FlightID:         parent.FlightID,
				Destination:      parent.Destination,
				Inspections:      maps.Clone(parent.Inspections),
				ParentLotIDs:     []string{pl.ParentLotID},
				Priority:         parent.Priority,
				PriorityReason:   parent.PriorityReason,
				HarvestedAt:      parent.HarvestedAt,
				SurvivalHours:    parent.SurvivalHours,
				SurvivalDeadline: parent.SurvivalDeadline,
			}
			for _, code := range ch.BoxCodes {
				w := parent.Boxes[code]
				child.Boxes[code] = w
				child.LiveWeightKg += w
				p.boxes[code] = ch.LotID
			}
			child.BoxCount = len(child.Boxes)
			parent.ChildLotIDs = append(parent.ChildLotIDs, ch.LotID)
			p.lots[ch.LotID] = child
			p.link(ch.LotID, ev.ID)
		}

	case EventLotMerged:
		var pl LotMerged
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		child := &LotView{
			LotID:        pl.ChildLotID,
			Status:       StatusHolding,
			TankID:       pl.TankID,
			Custodian:    pl.Custodian,
			Boxes:        map[string]float64{},
			Inspections:  map[string]string{},
			ParentLotIDs: slices.Clone(pl.ParentLotIDs),
			Priority:     PriorityNormal,
		}
		seen := map[string]bool{}
		for _, pid := range pl.ParentLotIDs {
			parent := p.lots[pid]
			parent.Status = StatusClosed
			parent.ClosedBy = "merged"
			parent.ChildLotIDs = append(parent.ChildLotIDs, pl.ChildLotID)
			p.link(pid, ev.ID)
			for _, bid := range parent.CatchBatchIDs {
				if !seen[bid] {
					seen[bid] = true
					child.CatchBatchIDs = append(child.CatchBatchIDs, bid)
				}
			}
			for code, w := range parent.Boxes {
				child.Boxes[code] = w
				child.LiveWeightKg += w
				p.boxes[code] = pl.ChildLotID
			}
			// 合并后的生存时限取各父批次中最早的,保持保守。
			if child.SurvivalDeadline.IsZero() || parent.SurvivalDeadline.Before(child.SurvivalDeadline) {
				child.SurvivalDeadline = parent.SurvivalDeadline
				child.HarvestedAt = parent.HarvestedAt
				child.SurvivalHours = parent.SurvivalHours
			}
		}
		child.BoxCount = len(child.Boxes)
		p.lots[pl.ChildLotID] = child
		p.link(pl.ChildLotID, ev.ID)

	case EventOrderRegistered:
		var pl OrderRegistered
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		p.orders[pl.OrderID] = &OrderView{
			OrderID:          pl.OrderID,
			Customer:         pl.Customer,
			Destination:      pl.Destination,
			RequiredBoxes:    pl.RequiredBoxes,
			RequiredWeightKg: pl.RequiredWeightKg,
			LatestArrival:    pl.LatestArrival,
		}

	case EventOrderChanged:
		var pl OrderChanged
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		if o, ok := p.orders[pl.OrderID]; ok {
			o.Changed = true
			o.ChangeNote = pl.Change
		}
		p.flagForRealloc(func(l *LotView) bool { return l.OrderID == pl.OrderID }, "订单变化: "+pl.Change)

	case EventLotAllocated, EventLotReallocated:
		var pl LotAllocated
		if ev.Type == EventLotReallocated {
			var rp LotReallocated
			if err := ev.Decode(&rp); err != nil {
				return err
			}
			pl = LotAllocated{LotID: rp.LotID, OrderID: rp.OrderID, VehicleID: rp.VehicleID, FlightID: rp.FlightID, Destination: rp.Destination}
		} else if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.OrderID = pl.OrderID
		lot.VehicleID = pl.VehicleID
		lot.FlightID = pl.FlightID
		lot.Destination = pl.Destination
		lot.ReallocSuggested = false
		lot.ReallocReason = ""
		if lot.Status == StatusHolding {
			lot.Status = StatusAllocated
		}
		p.link(pl.LotID, ev.ID)

	case EventTelemetryAppended:
		var pl TelemetryAppended
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		if lot, ok := p.lots[pl.LotID]; ok {
			lot.Telemetry = append(lot.Telemetry, TelemetryRecord{
				EventID:      ev.ID,
				DeviceID:     pl.DeviceID,
				Kind:         pl.Kind,
				Value:        pl.Value,
				DeadCount:    pl.DeadCount,
				DeadWeightKg: pl.DeadWeightKg,
				OccurredAt:   ev.OccurredAt,
			})
			p.link(pl.LotID, ev.ID)
		}

	case EventQuantityAdjusted:
		var pl QuantityAdjusted
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		if lot, ok := p.lots[pl.LotID]; ok {
			lot.LiveWeightKg -= pl.DeadWeightKg
			p.link(pl.LotID, ev.ID)
		}

	case EventMetricBreached:
		var pl MetricBreached
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		p.link(pl.LotID, ev.ID)

	case EventInspectionRecorded:
		var pl InspectionRecorded
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Inspections[string(pl.Kind)] = string(pl.Result)
		if lot.Status == StatusHolding || lot.Status == StatusAllocated {
			lot.Status = StatusInspecting
		}
		p.link(pl.LotID, ev.ID)

	case EventLotSealed:
		var pl LotSealed
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Status = StatusSealed
		lot.SealID = pl.SealID
		p.link(pl.LotID, ev.ID)

	case EventSealBroken:
		var pl SealBroken
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		// 开封后回到相应检查环节:联合核验结论作废,需重新核验后再封运。
		lot.Status = StatusInspecting
		lot.SealID = ""
		lot.ReleaseID = ""
		delete(lot.Inspections, string(InspectionJoint))
		p.link(pl.LotID, ev.ID)

	case EventLotReleased:
		var pl LotReleased
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Status = StatusReleased
		lot.ReleaseID = pl.ReleaseID
		p.link(pl.LotID, ev.ID)

	case EventHandoverRecorded:
		var pl HandoverRecorded
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Custodian = pl.ToParty
		p.link(pl.LotID, ev.ID)

	case EventHandoverDiscrepancy:
		var pl HandoverDiscrepancy
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		// 货物实际已移动,责任方随之转移;批次由随后的 halt 事件中止。
		if lot, ok := p.lots[pl.LotID]; ok {
			lot.Custodian = pl.ToParty
			p.link(pl.LotID, ev.ID)
		}

	case EventFlightRegistered:
		var pl FlightRegistered
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		p.flights[pl.FlightID] = &FlightView{FlightID: pl.FlightID, Destination: pl.Destination, DepartureAt: pl.DepartureAt}

	case EventFlightDelayed:
		var pl FlightDelayed
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		if f, ok := p.flights[pl.FlightID]; ok {
			f.DepartureAt = pl.NewDepartureAt
			f.DelayCount++
		}
		p.flagForRealloc(func(l *LotView) bool { return l.FlightID == pl.FlightID }, "航班延误: "+ev.Reason)

	case EventPriorityRaised:
		var pl PriorityRaised
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		if lot, ok := p.lots[pl.LotID]; ok {
			if priorityRank(pl.Level) > priorityRank(lot.Priority) {
				lot.Priority = pl.Level
				lot.PriorityReason = ev.Reason
			}
			p.link(pl.LotID, ev.ID)
		}

	case EventLotHalted:
		var pl LotHalted
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.PreHaltStatus = lot.Status
		lot.Status = StatusHalted
		lot.HaltStep = pl.Step
		lot.HaltReason = ev.Reason
		p.link(pl.LotID, ev.ID)

	case EventLotResumed:
		var pl LotResumed
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Status = pl.RestoredStatus
		lot.PreHaltStatus = ""
		lot.HaltStep = ""
		lot.HaltReason = ""
		p.link(pl.LotID, ev.ID)

	case EventLotDeparted:
		var pl LotDeparted
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Status = StatusInTransit
		p.link(pl.LotID, ev.ID)

	case EventLotDelivered:
		var pl LotDelivered
		if err := ev.Decode(&pl); err != nil {
			return err
		}
		lot := p.lots[pl.LotID]
		lot.Status = StatusDelivered
		p.link(pl.LotID, ev.ID)
	}
	return nil
}

// flagForRealloc 标记受航班延误/订单变化影响的在场批次为待改配。
// 已封运批次同样标记,提示运营方须先开封回到检查环节才能改配。
func (p *projection) flagForRealloc(match func(*LotView) bool, reason string) {
	for _, lot := range p.lots {
		if lot.Status.Terminal() || lot.Status == StatusHalted || lot.Status == StatusInTransit {
			continue
		}
		if match(lot) {
			lot.ReallocSuggested = true
			lot.ReallocReason = reason
		}
	}
}
