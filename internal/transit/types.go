// Package transit 实现鲜活水产保税联运的核心领域逻辑。
//
// 设计要点:全部状态变化都先落盘为只增不改的事件(append-only event log),
// 内存中的批次/箱码视图只是事件的重放结果,因此服务重启后可以从日志完整恢复
// 在途状态;每条命令携带幂等键,离线补传与多部门重复回调只会命中已存事件,
// 不会造成二次放行。
package transit

import "time"

// LotStatus 描述保税批次在联运链路中的状态机节点。
type LotStatus string

const (
	StatusHolding    LotStatus = "HOLDING"    // 暂养中
	StatusAllocated  LotStatus = "ALLOCATED"  // 已分配订单/航班,待检查
	StatusInspecting LotStatus = "INSPECTING" // 安检/海关/联合核验进行中
	StatusSealed     LotStatus = "SEALED"     // 联合核验完成,已封运
	StatusReleased   LotStatus = "RELEASED"   // 海关已放行
	StatusInTransit  LotStatus = "IN_TRANSIT" // 已装机在途
	StatusDelivered  LotStatus = "DELIVERED"  // 目的地已交付
	StatusHalted     LotStatus = "HALTED"     // 已中止,等待处置
	StatusClosed     LotStatus = "CLOSED"     // 已拆分或已并入其他批次,不再独立存在
)

// Reallocatable 报告该状态下是否允许重新分配。
// 只有尚未封运的货物可以改配;已封运批次必须先开封回到检查环节。
func (s LotStatus) Reallocatable() bool {
	switch s {
	case StatusHolding, StatusAllocated, StatusInspecting:
		return true
	default:
		return false
	}
}

// Terminal 报告批次是否已终结(不再接受任何命令)。
func (s LotStatus) Terminal() bool {
	switch s {
	case StatusDelivered, StatusClosed:
		return true
	default:
		return false
	}
}

// Priority 是处置优先级,接近生存阈值或指标越界时自动提升。
type Priority string

const (
	PriorityNormal   Priority = "normal"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

func priorityRank(p Priority) int {
	switch p {
	case PriorityCritical:
		return 2
	case PriorityHigh:
		return 1
	default:
		return 0
	}
}

// TelemetryKind 是环境观测的类型,按设备来源与发生时间追加。
type TelemetryKind string

const (
	TelemetryTemperature     TelemetryKind = "temperature"      // 温度,摄氏度
	TelemetryDissolvedOxygen TelemetryKind = "dissolved_oxygen" // 溶氧,mg/L
	TelemetryMortality       TelemetryKind = "mortality"        // 死亡抽检
)

// InspectionKind 是监管检查的环节。
type InspectionKind string

const (
	InspectionSecurity InspectionKind = "security" // 安检
	InspectionCustoms  InspectionKind = "customs"  // 海关查验
	InspectionJoint    InspectionKind = "joint"    // 联合核验
)

// InspectionResult 是检查结论。
type InspectionResult string

const (
	InspectionPass InspectionResult = "pass"
	InspectionFail InspectionResult = "fail"
)

// ReallocTrigger 记录改配的触发原因,用于事后解释。
type ReallocTrigger string

const (
	TriggerFlightDelay  ReallocTrigger = "flight_delay"  // 航班延误
	TriggerOrderChange  ReallocTrigger = "order_change"  // 订单变化
	TriggerMetricBreach ReallocTrigger = "metric_breach" // 指标越界
	TriggerManual       ReallocTrigger = "manual"        // 人工调整
)

// Thresholds 是生存与监管阈值,越界会自动提升处置优先级。
type Thresholds struct {
	TempMinC                  float64       // 暂养/运输温度下限
	TempMaxC                  float64       // 温度上限
	DissolvedOxygenMinMgL     float64       // 溶氧下限
	HighPriorityRemaining     time.Duration // 剩余时限低于该值升为 high
	CriticalPriorityRemaining time.Duration // 剩余时限低于该值升为 critical
}

// DefaultThresholds 给出帝王蟹联运的常用阈值。
func DefaultThresholds() Thresholds {
	return Thresholds{
		TempMinC:                  -1,
		TempMaxC:                  5,
		DissolvedOxygenMinMgL:     4,
		HighPriorityRemaining:     90 * time.Minute,
		CriticalPriorityRemaining: 45 * time.Minute,
	}
}

// weightEpsilon 是活体重量守恒校验允许的浮点误差(千克)。
const weightEpsilon = 1e-6
