package transit

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventType 是事件类型。外部事件由设备/部门上报或操作员下达;
// 派生事件(Derived=true)由规则引擎在应用外部事件后产生,
// 同样写入日志,保证重启回放后历史完全一致。
type EventType string

const (
	EvLotRegistered    EventType = "lot.registered"
	EvLotPicked        EventType = "lot.picked"
	EvObservation      EventType = "observation.recorded"
	EvHandover         EventType = "handover.recorded"
	EvLotSealed        EventType = "lot.sealed"
	EvLotOpened        EventType = "lot.opened"
	EvLotSplit         EventType = "lot.split"
	EvLotsMerged       EventType = "lots.merged"
	EvLotRebound       EventType = "lot.rebound"
	EvLotLoaded        EventType = "lot.loaded"
	EvLotDelivered     EventType = "lot.delivered"
	EvLotHalted        EventType = "lot.halted"
	EvLotResumed       EventType = "lot.resumed"
	EvFlightRegistered EventType = "flight.registered"
	EvFlightDelayed    EventType = "flight.delayed"
	EvFlightDeparted   EventType = "flight.departed"
	EvFlightArrived    EventType = "flight.arrived"
	EvOrderChanged     EventType = "order.changed"

	// 派生事件
	EvJointCheckPassed    EventType = "jointcheck.passed"
	EvLotReleased         EventType = "lot.released"
	EvReleaseDuplicate    EventType = "release.duplicate"
	EvReleaseConflict     EventType = "release.conflict"
	EvMetricViolated      EventType = "metric.violated"
	EvPriorityRaised      EventType = "priority.raised"
	EvHandoverDiscrepancy EventType = "handover.discrepancy"
)

// Event 是追加到日志的一条事件。幂等键 = Source + "|" + ID,
// 离线补传与重复回调携带相同键时只会被应用一次。
type Event struct {
	ID             string          `json:"id"`
	Seq            uint64          `json:"seq"`
	Type           EventType       `json:"type"`
	Source         string          `json:"source"` // 设备编号或部门
	OccurredAt     time.Time       `json:"occurred_at"`
	RecordedAt     time.Time       `json:"recorded_at"`
	Derived        bool            `json:"derived,omitempty"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
}

// Meta 是命令附带的来源信息。
type Meta struct {
	EventID    string
	Source     string
	OccurredAt time.Time
}

func decodePayload[T any](e Event) (T, error) {
	var p T
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return p, fmt.Errorf("解析事件负载失败(%s): %w", e.Type, err)
	}
	return p, nil
}

func marshalPayload(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err) // 负载均为内部结构体,序列化不应失败
	}
	return data
}

// ---- 事件负载 ----

type BoxSpec struct {
	Code     string  `json:"code"`
	WeightKg float64 `json:"weight_kg"`
}

type RegisterLotPayload struct {
	LotID               string    `json:"lot_id"`
	CatchBatch          string    `json:"catch_batch"`
	Tank                string    `json:"tank"`
	Boxes               []BoxSpec `json:"boxes"`
	OrderID             string    `json:"order_id"`
	Destination         string    `json:"destination"`
	FlightID            string    `json:"flight_id,omitempty"`
	Custodian           string    `json:"custodian"`
	SurvivalBudgetHours float64   `json:"survival_budget_hours"`
	TempMin             float64   `json:"temp_min"`
	TempMax             float64   `json:"temp_max"`
	DOMin               float64   `json:"do_min"`
}

type PickPayload struct {
	LotID string `json:"lot_id"`
}

// 观测类别
const (
	ObsTemperature     = "temperature"      // 温度 ℃
	ObsDissolvedOxygen = "dissolved_oxygen" // 溶氧 mg/L
	ObsMortality       = "mortality"        // 死亡抽检
	ObsSeal            = "seal"             // 封识状态
	ObsSecurity        = "security"         // 安检结论
	ObsCustoms         = "customs"          // 海关结论
)

type ObservationPayload struct {
	LotID        string  `json:"lot_id"`
	BoxCode      string  `json:"box_code,omitempty"`
	Kind         string  `json:"kind"`            // 见 Obs* 常量
	Value        float64 `json:"value,omitempty"` // 温度/溶氧读数
	Unit         string  `json:"unit,omitempty"`
	Conclusion   string  `json:"conclusion,omitempty"` // pass / fail / warn / info
	DeadWeightKg float64 `json:"dead_weight_kg,omitempty"`
	DeadCount    int     `json:"dead_count,omitempty"`
	DecisionID   string  `json:"decision_id,omitempty"` // 海关决定号
	Note         string  `json:"note,omitempty"`
}

type HandoverPayload struct {
	LotID        string  `json:"lot_id"`
	FromParty    string  `json:"from_party"`
	ToParty      string  `json:"to_party"`
	BoxCount     int     `json:"box_count"`
	LiveWeightKg float64 `json:"live_weight_kg"`
	Vehicle      string  `json:"vehicle,omitempty"`
	Note         string  `json:"note,omitempty"`
}

type SealPayload struct {
	LotID    string `json:"lot_id"`
	SealCode string `json:"seal_code"`
}

type OpenPayload struct {
	LotID  string `json:"lot_id"`
	Reason string `json:"reason"`
}

type SplitPayload struct {
	LotID       string   `json:"lot_id"`
	NewLotID    string   `json:"new_lot_id"`
	BoxCodes    []string `json:"box_codes"`
	OrderID     string   `json:"order_id,omitempty"`
	Destination string   `json:"destination,omitempty"`
	FlightID    string   `json:"flight_id,omitempty"`
	Reason      string   `json:"reason"`
}

type MergePayload struct {
	LotIDs   []string `json:"lot_ids"`
	NewLotID string   `json:"new_lot_id"`
	Reason   string   `json:"reason"`
}

type RebindPayload struct {
	LotID       string `json:"lot_id"`
	OrderID     string `json:"order_id,omitempty"`
	Destination string `json:"destination,omitempty"`
	FlightID    string `json:"flight_id,omitempty"`
	Reason      string `json:"reason"`
}

type AssignFlightPayload struct {
	LotID    string `json:"lot_id"`
	FlightID string `json:"flight_id"`
}

type RegisterFlightPayload struct {
	FlightID      string    `json:"flight_id"`
	Destination   string    `json:"destination"`
	LoadingCutoff time.Time `json:"loading_cutoff"`
	DepartAt      time.Time `json:"depart_at"`
}

type DelayFlightPayload struct {
	FlightID    string    `json:"flight_id"`
	NewCutoff   time.Time `json:"new_cutoff"`
	NewDepartAt time.Time `json:"new_depart_at"`
	Reason      string    `json:"reason"`
}

type FlightRefPayload struct {
	FlightID string `json:"flight_id"`
}

type LoadPayload struct {
	LotID    string `json:"lot_id"`
	FlightID string `json:"flight_id"`
}

type DeliverPayload struct {
	LotID        string  `json:"lot_id"`
	ToParty      string  `json:"to_party"`
	BoxCount     int     `json:"box_count"`
	LiveWeightKg float64 `json:"live_weight_kg"`
}

type HaltPayload struct {
	LotID  string `json:"lot_id"`
	Kind   string `json:"kind,omitempty"` // manual / inspection / discrepancy
	Reason string `json:"reason"`
}

type ResumePayload struct {
	LotID  string `json:"lot_id"`
	Auto   bool   `json:"auto,omitempty"`
	Reason string `json:"reason"`
}

type OrderChangePayload struct {
	OrderID     string `json:"order_id"`
	Destination string `json:"destination"`
	Reason      string `json:"reason"`
}

// ---- 派生事件负载 ----

type JointCheckPassedPayload struct {
	LotID string `json:"lot_id"`
	Basis string `json:"basis"` // 三项结论的来源与时间
}

type ReleasePayload struct {
	LotID      string `json:"lot_id"`
	DecisionID string `json:"decision_id"`
	Basis      string `json:"basis"`
	SourceKey  string `json:"source_key,omitempty"` // 触发放行的观测事件幂等键
}

type ReleaseDupPayload struct {
	LotID      string `json:"lot_id"`
	DecisionID string `json:"decision_id"`
}

type ReleaseConflictPayload struct {
	LotID              string `json:"lot_id"`
	KeptDecisionID     string `json:"kept_decision_id"`
	RejectedDecisionID string `json:"rejected_decision_id"`
}

type MetricViolatedPayload struct {
	LotID     string  `json:"lot_id"`
	Kind      string  `json:"kind"`
	Value     float64 `json:"value"`
	Threshold string  `json:"threshold"`
}

type PriorityRaisedPayload struct {
	LotID  string   `json:"lot_id"`
	From   Priority `json:"from"`
	To     Priority `json:"to"`
	Reason string   `json:"reason"`
}

type DiscrepancyPayload struct {
	LotID          string  `json:"lot_id"`
	Context        string  `json:"context"` // 交接 / 交付
	DeclaredCount  int     `json:"declared_count"`
	ExpectedCount  int     `json:"expected_count"`
	DeclaredWeight float64 `json:"declared_weight_kg"`
	ExpectedWeight float64 `json:"expected_weight_kg"`
}
