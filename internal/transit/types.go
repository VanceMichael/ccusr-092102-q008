// Package transit 实现鲜活水产保税联运的核心领域模型。
//
// 设计要点:
//   - 事件溯源:所有状态变化先追加到 JSONL 事件日志(带幂等键),
//     再应用到内存投影;服务重启后回放日志即可恢复在途状态。
//   - 谱系:捕捞批次 → 保税批次(可拆分/合并)→ 箱码 → 订单/车辆/航班/目的地,
//     全部通过事件保留父子关系。
//   - 守恒:每次交接与交付都校验箱数与活体重量,失败即中止待核查。
//   - 一次放行:海关放行按决定号幂等,重复回调与离线补传不会造成二次放行。
package transit

import (
	"sort"
	"time"
)

// Stage 表示批次所处的联运环节。
type Stage string

const (
	StageHolding    Stage = "HOLDING"     // 保税暂养中
	StagePicked     Stage = "PICKED"      // 已出库,待联合核验
	StageJointCheck Stage = "JOINT_CHECK" // 联合核验中(死亡抽检/封识/安检)
	StageSealed     Stage = "SEALED"      // 已封运,改配窗口关闭
	StageReleased   Stage = "RELEASED"    // 海关已放行
	StageLoaded     Stage = "LOADED"      // 已装机
	StageDeparted   Stage = "DEPARTED"    // 已起飞
	StageArrived    Stage = "ARRIVED"     // 已到港
	StageDelivered  Stage = "DELIVERED"   // 目的地已交付
)

var stageRank = map[Stage]int{
	StageHolding:    0,
	StagePicked:     1,
	StageJointCheck: 2,
	StageSealed:     3,
	StageReleased:   4,
	StageLoaded:     5,
	StageDeparted:   6,
	StageArrived:    7,
	StageDelivered:  8,
}

// preSeal 报告该环节是否仍允许改配(仅尚未封运的货物可以重新分配)。
func preSeal(s Stage) bool { return stageRank[s] < stageRank[StageSealed] }

// StageLabel 返回环节的中文名称。
func StageLabel(s Stage) string {
	switch s {
	case StageHolding:
		return "保税暂养中"
	case StagePicked:
		return "已出库待核验"
	case StageJointCheck:
		return "联合核验中"
	case StageSealed:
		return "已封运"
	case StageReleased:
		return "海关已放行"
	case StageLoaded:
		return "已装机"
	case StageDeparted:
		return "已起飞"
	case StageArrived:
		return "已到港"
	case StageDelivered:
		return "已交付"
	}
	return string(s)
}

// Priority 是处置优先级,接近生存阈值会自动提升。
type Priority string

const (
	PriorityNormal   Priority = "NORMAL"
	PriorityHigh     Priority = "HIGH"
	PriorityCritical Priority = "CRITICAL"
)

func priorityRank(p Priority) int {
	switch p {
	case PriorityCritical:
		return 2
	case PriorityHigh:
		return 1
	}
	return 0
}

// Box 是一箱货物的台账记录。箱始终属于某一个批次。
type Box struct {
	Code            string  `json:"code"`
	LotID           string  `json:"lot_id"`
	CatchBatch      string  `json:"catch_batch"`
	InitialWeightKg float64 `json:"initial_weight_kg"`
	LiveWeightKg    float64 `json:"live_weight_kg"` // 扣除已登记死亡后的活体重量
}

// JointCheck 记录联合核验三项结论的收集情况。
type JointCheck struct {
	MortalityPass bool       `json:"mortality_pass"`
	MortalityAt   *time.Time `json:"mortality_at,omitempty"`
	SealOK        bool       `json:"seal_ok"`
	SealAt        *time.Time `json:"seal_at,omitempty"`
	SecurityPass  bool       `json:"security_pass"`
	SecurityAt    *time.Time `json:"security_at,omitempty"`
	PassedAt      *time.Time `json:"passed_at,omitempty"`
}

// CustomsStatus 记录最近一次海关结论。
type CustomsStatus struct {
	Conclusion string     `json:"conclusion,omitempty"` // pass / fail
	DecisionID string     `json:"decision_id,omitempty"`
	At         *time.Time `json:"at,omitempty"`
}

// DecisionEntry 是决策日志的一条记录,用于事后解释
// 为何一次放行、为何改配、在哪一步被中止。
type DecisionEntry struct {
	Seq     uint64    `json:"seq"`
	Time    time.Time `json:"time"`
	Kind    string    `json:"kind"`
	Summary string    `json:"summary"`
	Detail  string    `json:"detail,omitempty"`
}

// Lot 是保税批次的内存投影。批次内所有箱处于同一环节;
// 拆分/合并会产生子批次并保留谱系。
type Lot struct {
	ID               string   `json:"id"`
	CatchBatch       string   `json:"catch_batch"` // 原始捕捞批次
	Tank             string   `json:"tank"`        // 暂养池
	ParentLotID      string   `json:"parent_lot_id,omitempty"`
	ChildLotIDs      []string `json:"child_lot_ids,omitempty"`
	MergedFromLotIDs []string `json:"merged_from_lot_ids,omitempty"`
	Closed           bool     `json:"closed,omitempty"` // 拆分/合并后清空

	Boxes       map[string]*Box `json:"-"`
	OrderID     string          `json:"order_id"`
	Destination string          `json:"destination"`
	FlightID    string          `json:"flight_id,omitempty"`
	Vehicle     string          `json:"vehicle,omitempty"` // 当前承运车辆

	Stage     Stage  `json:"stage"`
	Custodian string `json:"custodian"` // 当前责任方

	Joint   JointCheck    `json:"joint_check"`
	Customs CustomsStatus `json:"customs"`

	Sealed    bool       `json:"sealed"`
	SealCode  string     `json:"seal_code,omitempty"`
	SealedAt  *time.Time `json:"sealed_at,omitempty"`
	OpenCount int        `json:"open_count,omitempty"`

	Released          bool       `json:"released"`
	ReleaseDecisionID string     `json:"release_decision_id,omitempty"`
	ReleasedAt        *time.Time `json:"released_at,omitempty"`
	ReleaseSourceKey  string     `json:"release_source_key,omitempty"` // 触发放行的观测事件幂等键
	VoidedDecisionID  string     `json:"voided_decision_id,omitempty"` // 开封后作废的放行决定
	ReleaseDuplicates int        `json:"release_duplicates,omitempty"` // 已忽略的重复放行回调次数
	ReleaseConflicts  int        `json:"release_conflicts,omitempty"`

	Halted        bool   `json:"halted,omitempty"`
	HaltKind      string `json:"halt_kind,omitempty"` // manual / inspection / discrepancy
	HaltReason    string `json:"halt_reason,omitempty"`
	HaltedAtStage Stage  `json:"halted_at_stage,omitempty"`

	Priority   Priority `json:"priority"` // 历史上达到过的最高优先级
	Violations int      `json:"violations,omitempty"`

	PickedAt       *time.Time    `json:"picked_at,omitempty"`
	SurvivalBudget time.Duration `json:"survival_budget"` // 出库后的存活预算
	TempMin        float64       `json:"temp_min"`
	TempMax        float64       `json:"temp_max"`
	DOMin          float64       `json:"do_min"`

	InitialWeightKg float64 `json:"initial_weight_kg"`
	DeadWeightKg    float64 `json:"dead_weight_kg"`
	DeadCount       int     `json:"dead_count"`

	DecisionLog []DecisionEntry `json:"decision_log,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// LiveWeight 返回批次当前活体重量(各箱之和)。
// 按箱码排序求和,保证回放与在线计算的浮点结果逐位一致。
func (l *Lot) LiveWeight() float64 {
	codes := make([]string, 0, len(l.Boxes))
	for c := range l.Boxes {
		codes = append(codes, c)
	}
	sort.Strings(codes)
	var w float64
	for _, c := range codes {
		w += l.Boxes[c].LiveWeightKg
	}
	return w
}

// BoxCount 返回批次当前箱数。
func (l *Lot) BoxCount() int { return len(l.Boxes) }

// MortalityRate 返回累计死亡重量占初始重量的比例。
func (l *Lot) MortalityRate() float64 {
	if l.InitialWeightKg <= 0 {
		return 0
	}
	return l.DeadWeightKg / l.InitialWeightKg
}

// Flight 是货运航班/包机计划。
type Flight struct {
	ID            string    `json:"id"`
	Destination   string    `json:"destination"`
	LoadingCutoff time.Time `json:"loading_cutoff"` // 随到随装截载时间
	DepartAt      time.Time `json:"depart_at"`
	Status        string    `json:"status"` // SCHEDULED / DEPARTED / ARRIVED
	DelayReason   string    `json:"delay_reason,omitempty"`
	DelayCount    int       `json:"delay_count,omitempty"`
}

// Order 是客户订单。
type Order struct {
	ID          string `json:"id"`
	Destination string `json:"destination"`
}

// TimeConstraint 是一条剩余时限候选(存活时限/航班截载)。
type TimeConstraint struct {
	Basis            string    `json:"basis"`
	Deadline         time.Time `json:"deadline"`
	RemainingSeconds int64     `json:"remaining_seconds"`
	Exceeded         bool      `json:"exceeded"`
}

// ScanResult 是扫描箱码得到的即时应答:
// 当前责任方、剩余时限、监管结论、允许目的地。
type ScanResult struct {
	BoxCode            string           `json:"box_code"`
	LotID              string           `json:"lot_id"`
	CatchBatch         string           `json:"catch_batch"`
	Stage              Stage            `json:"stage"`
	StageLabel         string           `json:"stage_label"`
	Custodian          string           `json:"custodian"`
	Regulatory         string           `json:"regulatory"`
	AllowedDestination string           `json:"allowed_destination"`
	DestinationLocked  bool             `json:"destination_locked"`
	Priority           Priority         `json:"priority"`
	Halted             bool             `json:"halted"`
	HaltReason         string           `json:"halt_reason,omitempty"`
	Constraints        []TimeConstraint `json:"time_constraints,omitempty"`
	RemainingSeconds   *int64           `json:"remaining_seconds,omitempty"`
	RemainingBasis     string           `json:"remaining_basis,omitempty"`
	LiveWeightKg       float64          `json:"live_weight_kg"`
}

// LotNode 是谱系中的一个批次节点。
type LotNode struct {
	LotID       string   `json:"lot_id"`
	ParentLotID string   `json:"parent_lot_id,omitempty"`
	ChildLotIDs []string `json:"child_lot_ids,omitempty"`
	MergedFrom  []string `json:"merged_from,omitempty"`
	Stage       Stage    `json:"stage"`
	StageLabel  string   `json:"stage_label"`
	Closed      bool     `json:"closed,omitempty"`
	BoxCount    int      `json:"box_count"`
}

// LineageView 是箱码的完整谱系:捕捞批次→暂养池→批次链→订单/车辆/航班→目的地。
type LineageView struct {
	BoxCode     string    `json:"box_code"`
	CatchBatch  string    `json:"catch_batch"`
	Tank        string    `json:"tank"`
	LotChain    []LotNode `json:"lot_chain"`
	OrderID     string    `json:"order_id,omitempty"`
	Vehicle     string    `json:"vehicle,omitempty"`
	FlightID    string    `json:"flight_id,omitempty"`
	Destination string    `json:"destination"`
}

// QueueItem 是处置队列中的一项,按优先级与剩余时限排序。
type QueueItem struct {
	LotID            string   `json:"lot_id"`
	Stage            Stage    `json:"stage"`
	StageLabel       string   `json:"stage_label"`
	Priority         Priority `json:"priority"`
	Halted           bool     `json:"halted"`
	HaltReason       string   `json:"halt_reason,omitempty"`
	Custodian        string   `json:"custodian"`
	Destination      string   `json:"destination"`
	BoxCount         int      `json:"box_count"`
	LiveWeightKg     float64  `json:"live_weight_kg"`
	RemainingSeconds *int64   `json:"remaining_seconds,omitempty"`
}
