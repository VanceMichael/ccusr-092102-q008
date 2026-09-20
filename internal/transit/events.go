package transit

import (
	"encoding/json"
	"time"
)

// 事件类型。所有事件只增不改,按 seq 顺序重放即可重建任意时刻的在途状态。
const (
	EventCatchBatchRegistered = "catch_batch_registered" // 捕捞批次登记
	EventBondedLotCreated     = "bonded_lot_created"     // 保税暂养批次建立(从捕捞批次拆分)
	EventLotSplit             = "lot_split"              // 批次拆分
	EventLotMerged            = "lot_merged"             // 批次合并
	EventOrderRegistered      = "order_registered"       // 客户订单登记
	EventOrderChanged         = "order_changed"          // 订单变化
	EventLotAllocated         = "lot_allocated"          // 分配到订单/车辆/航班
	EventTelemetryAppended    = "telemetry_appended"     // 温度/溶氧/死亡抽检追加
	EventQuantityAdjusted     = "quantity_adjusted"      // 死亡抽检引起的台账调整
	EventMetricBreached       = "metric_breached"        // 指标越界
	EventInspectionRecorded   = "inspection_recorded"    // 安检/海关/联合核验结果
	EventLotSealed            = "lot_sealed"             // 联合核验完成,施加封识
	EventSealBroken           = "seal_broken"            // 开封,回到相应检查环节
	EventLotReleased          = "lot_released"           // 海关放行
	EventHandoverRecorded     = "handover_recorded"      // 交接守恒确认
	EventHandoverDiscrepancy  = "handover_discrepancy"   // 交接箱数/重量不守恒
	EventLotReallocated       = "lot_reallocated"        // 改配
	EventFlightRegistered     = "flight_registered"      // 航班登记
	EventFlightDelayed        = "flight_delayed"         // 航班延误
	EventPriorityRaised       = "priority_raised"        // 处置优先级提升
	EventLotHalted            = "lot_halted"             // 中止
	EventLotResumed           = "lot_resumed"            // 中止后恢复
	EventLotDeparted          = "lot_departed"           // 起运离港
	EventLotDelivered         = "lot_delivered"          // 目的地交付
)

// Event 是事件信封。OccurredAt 是事情真实发生的时间(设备/口岸本地时间,
// 离线补传时可能远早于 RecordedAt);RecordedAt 是服务落盘时间。
// CausedBy 指向触发本事件的源事件(如延误事件触发改配),用于事后解释。
type Event struct {
	ID             string          `json:"id"`
	Seq            int64           `json:"seq"`
	Type           string          `json:"type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	RecordedAt     time.Time       `json:"recorded_at"`
	Actor          string          `json:"actor,omitempty"`
	Reason         string          `json:"reason,omitempty"`
	CausedBy       string          `json:"caused_by,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Payload        json.RawMessage `json:"payload"`
}

// Decode 解析事件载荷。
func (e Event) Decode(v any) error { return json.Unmarshal(e.Payload, v) }

// Meta 是写命令附带的通用元数据。
type Meta struct {
	Actor          string
	Reason         string
	CausedBy       string
	IdempotencyKey string
	OccurredAt     time.Time
}

// --- 载荷定义 ---

// CatchBatchRegistered 登记原始捕捞批次,是谱系的根。
type CatchBatchRegistered struct {
	CatchBatchID  string    `json:"catch_batch_id"`
	Species       string    `json:"species"`
	Origin        string    `json:"origin"`
	HarvestedAt   time.Time `json:"harvested_at"`
	SurvivalHours float64   `json:"survival_hours"`
	TotalBoxes    int       `json:"total_boxes"`
	TotalWeightKg float64   `json:"total_weight_kg"`
}

// BoxSpec 是单箱的初始登记。
type BoxSpec struct {
	BoxCode  string  `json:"box_code"`
	WeightKg float64 `json:"weight_kg"`
}

// BondedLotCreated 从捕捞批次拆分出保税暂养批次并入池。
type BondedLotCreated struct {
	LotID        string    `json:"lot_id"`
	CatchBatchID string    `json:"catch_batch_id"`
	TankID       string    `json:"tank_id"`
	Custodian    string    `json:"custodian"`
	Boxes        []BoxSpec `json:"boxes"`
}

// SplitChild 是拆分出的一个子批次。
type SplitChild struct {
	LotID    string   `json:"lot_id"`
	TankID   string   `json:"tank_id"`
	BoxCodes []string `json:"box_codes"`
}

// LotSplit 把父批次的箱码精确划分给若干子批次(箱数与重量守恒)。
type LotSplit struct {
	ParentLotID string       `json:"parent_lot_id"`
	Children    []SplitChild `json:"children"`
}

// LotMerged 把多个暂养批次合并为一个批次(箱数与重量守恒)。
type LotMerged struct {
	ParentLotIDs []string `json:"parent_lot_ids"`
	ChildLotID   string   `json:"child_lot_id"`
	TankID       string   `json:"tank_id"`
	Custodian    string   `json:"custodian"`
}

// OrderRegistered 登记客户订单。
type OrderRegistered struct {
	OrderID          string    `json:"order_id"`
	Customer         string    `json:"customer"`
	Destination      string    `json:"destination"`
	RequiredBoxes    int       `json:"required_boxes"`
	RequiredWeightKg float64   `json:"required_weight_kg"`
	LatestArrival    time.Time `json:"latest_arrival"`
}

// OrderChanged 记录订单变化,受影响的未封运批次会被标记为待改配。
type OrderChanged struct {
	OrderID string `json:"order_id"`
	Change  string `json:"change"`
}

// LotAllocated 把批次分配到订单、车辆与航班,目的地取自订单。
type LotAllocated struct {
	LotID       string `json:"lot_id"`
	OrderID     string `json:"order_id"`
	VehicleID   string `json:"vehicle_id"`
	FlightID    string `json:"flight_id"`
	Destination string `json:"destination"`
}

// LotReallocated 是改配记录,Trigger 说明触发原因。
type LotReallocated struct {
	LotID       string         `json:"lot_id"`
	OrderID     string         `json:"order_id"`
	VehicleID   string         `json:"vehicle_id"`
	FlightID    string         `json:"flight_id"`
	Destination string         `json:"destination"`
	Trigger     ReallocTrigger `json:"trigger"`
}

// TelemetryAppended 追加一条环境观测,按设备来源与发生时间排序保存。
// Kind 为 mortality 时,DeadCount/DeadWeightKg 会驱动台账调整。
type TelemetryAppended struct {
	DeviceID     string        `json:"device_id"`
	DeviceSeq    int64         `json:"device_seq"`
	LotID        string        `json:"lot_id"`
	BoxCode      string        `json:"box_code,omitempty"`
	Kind         TelemetryKind `json:"kind"`
	Value        float64       `json:"value"`
	DeadCount    int           `json:"dead_count,omitempty"`
	DeadWeightKg float64       `json:"dead_weight_kg,omitempty"`
}

// QuantityAdjusted 是死亡抽检引起的台账调整,后续交接按调整后的期望值守恒。
type QuantityAdjusted struct {
	LotID         string  `json:"lot_id"`
	DeadCount     int     `json:"dead_count"`
	DeadWeightKg  float64 `json:"dead_weight_kg"`
	SourceEventID string  `json:"source_event_id"`
}

// MetricBreached 记录一次指标越界。
type MetricBreached struct {
	LotID string        `json:"lot_id"`
	Kind  TelemetryKind `json:"kind"`
	Value float64       `json:"value"`
	Limit string        `json:"limit"`
}

// InspectionRecorded 记录一个部门的检查结果,按机构与回调号幂等。
type InspectionRecorded struct {
	LotID      string           `json:"lot_id"`
	Kind       InspectionKind   `json:"kind"`
	Result     InspectionResult `json:"result"`
	Authority  string           `json:"authority"`
	CallbackID string           `json:"callback_id"`
}

// LotSealed 施加封识,标志联合核验完成。
type LotSealed struct {
	LotID  string `json:"lot_id"`
	SealID string `json:"seal_id"`
}

// SealBroken 开封,批次回到 Step 指定的检查环节。
type SealBroken struct {
	LotID  string `json:"lot_id"`
	SealID string `json:"seal_id"`
	Step   string `json:"step"`
}

// LotReleased 是海关放行记录,按放行号幂等,杜绝二次放行。
type LotReleased struct {
	LotID       string `json:"lot_id"`
	ReleaseID   string `json:"release_id"`
	Authority   string `json:"authority"`
	Destination string `json:"destination"`
}

// HandoverRecorded 是守恒确认的交接。
type HandoverRecorded struct {
	HandoverID   string  `json:"handover_id"`
	LotID        string  `json:"lot_id"`
	FromParty    string  `json:"from_party"`
	ToParty      string  `json:"to_party"`
	BoxCount     int     `json:"box_count"`
	LiveWeightKg float64 `json:"live_weight_kg"`
}

// HandoverDiscrepancy 记录交接不守恒,批次随即中止。
type HandoverDiscrepancy struct {
	HandoverID       string  `json:"handover_id"`
	LotID            string  `json:"lot_id"`
	FromParty        string  `json:"from_party"`
	ToParty          string  `json:"to_party"`
	ExpectedBoxes    int     `json:"expected_boxes"`
	ActualBoxes      int     `json:"actual_boxes"`
	ExpectedWeightKg float64 `json:"expected_weight_kg"`
	ActualWeightKg   float64 `json:"actual_weight_kg"`
}

// FlightRegistered 登记货运航班。
type FlightRegistered struct {
	FlightID    string    `json:"flight_id"`
	Destination string    `json:"destination"`
	DepartureAt time.Time `json:"departure_at"`
}

// FlightDelayed 记录航班延误,相关批次会被标记为待改配。
type FlightDelayed struct {
	FlightID       string    `json:"flight_id"`
	NewDepartureAt time.Time `json:"new_departure_at"`
}

// PriorityRaised 记录一次处置优先级提升。
type PriorityRaised struct {
	LotID string   `json:"lot_id"`
	Level Priority `json:"level"`
}

// LotHalted 记录中止及其发生的环节。
type LotHalted struct {
	LotID string `json:"lot_id"`
	Step  string `json:"step"`
}

// LotResumed 记录中止后恢复到的状态。
type LotResumed struct {
	LotID          string    `json:"lot_id"`
	RestoredStatus LotStatus `json:"restored_status"`
}

// LotDeparted 记录起运离港。
type LotDeparted struct {
	LotID    string `json:"lot_id"`
	FlightID string `json:"flight_id"`
}

// LotDelivered 记录目的地交付。
type LotDelivered struct {
	LotID string `json:"lot_id"`
}
