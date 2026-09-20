package transit

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var base = time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func (c *fakeClock) meta(id string) Meta {
	return Meta{EventID: id, Source: "test", OccurredAt: c.now()}
}

func newTestService(t *testing.T, clock *fakeClock) (*Service, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.jsonl")
	store, events, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return NewService(store, events, clock.now), path
}

// do 包装命令调用:do(t)(svc.Pick(...)) 形式断言成功且非幂等命中。
func do(t *testing.T) func(*CommandResult, error) {
	return func(res *CommandResult, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("命令失败: %v", err)
		}
		if res != nil && res.Duplicate {
			t.Fatalf("命令被误判为重复")
		}
	}
}

func mustErr(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("应失败但未失败,期望错误包含 %q", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Fatalf("错误 %q 不包含 %q", err.Error(), substr)
	}
}

var boxWeights = []float64{11.8, 12.2, 11.5} // 合计 35.5kg

// registerFlight 登记测试航班(截载 base+6h,起飞 base+7h)。
func registerFlight(t *testing.T, svc *Service, clock *fakeClock, flightID string) {
	t.Helper()
	do(t)(svc.RegisterFlight(clock.meta("flight-"+flightID), RegisterFlightPayload{
		FlightID: flightID, Destination: "香港",
		LoadingCutoff: base.Add(6 * time.Hour), DepartAt: base.Add(7 * time.Hour),
	}))
}

// registerLot 注册标准批次(3 箱共 35.5kg)。
func registerLot(t *testing.T, svc *Service, clock *fakeClock, lotID, flightID string) {
	t.Helper()
	registerLotBoxes(t, svc, clock, lotID, flightID, []string{"KC-000001", "KC-000002", "KC-000003"})
}

func registerLotBoxes(t *testing.T, svc *Service, clock *fakeClock, lotID, flightID string, codes []string) {
	t.Helper()
	boxes := make([]BoxSpec, len(codes))
	for i, c := range codes {
		boxes[i] = BoxSpec{Code: c, WeightKg: boxWeights[i%len(boxWeights)]}
	}
	do(t)(svc.RegisterLot(clock.meta("reg-"+lotID), RegisterLotPayload{
		LotID: lotID, CatchBatch: "RU-KC-20260918-A", Tank: "tank-03",
		Boxes:   boxes,
		OrderID: "ORD-HK-01", Destination: "香港", FlightID: flightID,
		Custodian: "珲春保税暂养场", SurvivalBudgetHours: 8,
		TempMin: 2, TempMax: 8, DOMin: 6,
	}))
}

func observe(t *testing.T, svc *Service, clock *fakeClock, eventID string, p ObservationPayload) {
	t.Helper()
	res := svc.RecordObservations([]ObservationInput{{Meta: clock.meta(eventID), Payload: p}})
	if len(res) != 1 || res[0].Error != "" {
		t.Fatalf("观测上报失败: %+v", res)
	}
	if res[0].Duplicate {
		t.Fatalf("观测 %s 被误判为重复", eventID)
	}
}

// driveToSealed 把批次推进到已封运(出库→死亡抽检/封识/安检→封运),死亡 0.2kg。
func driveToSealed(t *testing.T, svc *Service, clock *fakeClock, lotID, idPrefix string) {
	t.Helper()
	do(t)(svc.Pick(clock.meta(idPrefix+"-pick"), lotID))
	observe(t, svc, clock, idPrefix+"-mort", ObservationPayload{
		LotID: lotID, Kind: ObsMortality, Conclusion: "pass", DeadWeightKg: 0.2, DeadCount: 1,
	})
	observe(t, svc, clock, idPrefix+"-sealobs", ObservationPayload{
		LotID: lotID, Kind: ObsSeal, Conclusion: "pass",
	})
	observe(t, svc, clock, idPrefix+"-sec", ObservationPayload{
		LotID: lotID, Kind: ObsSecurity, Conclusion: "pass",
	})
	do(t)(svc.Seal(clock.meta(idPrefix+"-sealcmd"), lotID, "SEAL-"+lotID))
}

// driveJointCheckAndSeal 在已出库批次上完成联合核验并封运(开封后重新封运也走这里)。
func driveJointCheckAndSeal(t *testing.T, svc *Service, clock *fakeClock, lotID, idPrefix string) {
	t.Helper()
	observe(t, svc, clock, idPrefix+"-mort", ObservationPayload{
		LotID: lotID, Kind: ObsMortality, Conclusion: "pass", DeadWeightKg: 0.2, DeadCount: 1,
	})
	observe(t, svc, clock, idPrefix+"-sealobs", ObservationPayload{
		LotID: lotID, Kind: ObsSeal, Conclusion: "pass",
	})
	observe(t, svc, clock, idPrefix+"-sec", ObservationPayload{
		LotID: lotID, Kind: ObsSecurity, Conclusion: "pass",
	})
	do(t)(svc.Seal(clock.meta(idPrefix+"-sealcmd"), lotID, "SEAL-"+lotID))
}

// driveToReleased 把批次推进到海关放行(决定号 HG-D001)。
func driveToReleased(t *testing.T, svc *Service, clock *fakeClock, lotID, idPrefix string) {
	t.Helper()
	driveToSealed(t, svc, clock, lotID, idPrefix)
	observe(t, svc, clock, idPrefix+"-customs", ObservationPayload{
		LotID: lotID, Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D001",
	})
}

func lotOf(t *testing.T, svc *Service, lotID string) *Lot {
	t.Helper()
	view, err := svc.LotDetail(lotID)
	if err != nil {
		t.Fatalf("查询批次失败: %v", err)
	}
	return view.Lot
}

func explainHas(t *testing.T, svc *Service, lotID, focus, substr string) bool {
	t.Helper()
	entries, err := svc.Explain(lotID, focus)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.Contains(e.Summary, substr) || strings.Contains(e.Detail, substr) {
			return true
		}
	}
	return false
}

// 需求:多部门重复回调与离线补传不能造成二次放行。
func TestSingleReleaseIdempotent(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	driveToReleased(t, svc, clock, "LOT-1", "d1")

	lot := lotOf(t, svc, "LOT-1")
	if !lot.Released || lot.ReleaseDecisionID != "HG-D001" {
		t.Fatalf("应已放行且决定号为 HG-D001: %+v", lot)
	}

	// 1. 同一事件 ID 的离线补传 → 存储层幂等命中
	res := svc.RecordObservations([]ObservationInput{{
		Meta:    clock.meta("d1-customs"),
		Payload: ObservationPayload{LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D001"},
	}})
	if !res[0].Duplicate {
		t.Fatalf("相同事件 ID 的补传应被去重")
	}

	// 2. 不同事件 ID、同一决定号的重复回调 → release.duplicate,不放行第二次
	observe(t, svc, clock, "d1-customs-dept2", ObservationPayload{
		LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D001",
	})
	// 3. 另一部门的不同决定号 → 冲突记录,保持原放行
	observe(t, svc, clock, "d1-customs-dept3", ObservationPayload{
		LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D002",
	})

	lot = lotOf(t, svc, "LOT-1")
	if lot.ReleaseDecisionID != "HG-D001" {
		t.Fatalf("放行决定号被覆盖: %s", lot.ReleaseDecisionID)
	}
	if lot.ReleaseDuplicates != 1 || lot.ReleaseConflicts != 1 {
		t.Fatalf("重复/冲突计数错误: dup=%d conflict=%d", lot.ReleaseDuplicates, lot.ReleaseConflicts)
	}

	// 决策日志中"一次放行"只出现一次
	entries, err := svc.Explain("LOT-1", "release")
	if err != nil {
		t.Fatal(err)
	}
	var releases, dups, conflicts int
	for _, e := range entries {
		switch e.Kind {
		case LogRelease:
			releases++
		case LogReleaseDuplicate:
			dups++
		case LogReleaseConflict:
			conflicts++
		}
	}
	if releases != 1 || dups != 1 || conflicts != 1 {
		t.Fatalf("放行日志应为 1 放行/1 重复/1 冲突,实际 %d/%d/%d", releases, dups, conflicts)
	}

	// 扫码应答:监管结论含一次放行,目的地已锁定
	scan, err := svc.ScanBox("KC-000001")
	if err != nil {
		t.Fatal(err)
	}
	if scan.Stage != StageReleased || !scan.DestinationLocked {
		t.Fatalf("扫码状态错误: %+v", scan)
	}
	if !strings.Contains(scan.Regulatory, "一次放行") || !strings.Contains(scan.Regulatory, "HG-D001") {
		t.Fatalf("监管结论缺少放行信息: %s", scan.Regulatory)
	}
	if scan.AllowedDestination != "香港" {
		t.Fatalf("允许目的地应为香港: %s", scan.AllowedDestination)
	}
}

// 需求:离线补传按幂等键去重,死亡扣重只应用一次。
func TestOfflineBackfillNoDoubleApply(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerLot(t, svc, clock, "LOT-1", "")

	before := lotOf(t, svc, "LOT-1").LiveWeight()

	// 离线设备补传:发生时间在过去,事件 ID 唯一
	past := base.Add(-40 * time.Minute)
	res := svc.RecordObservations([]ObservationInput{{
		Meta:    Meta{EventID: "scale-0001", Source: "scale-01", OccurredAt: past},
		Payload: ObservationPayload{LotID: "LOT-1", Kind: ObsMortality, DeadWeightKg: 0.4, DeadCount: 2},
	}})
	if res[0].Duplicate || res[0].Error != "" {
		t.Fatalf("首次补传应被应用: %+v", res[0])
	}
	after1 := lotOf(t, svc, "LOT-1").LiveWeight()
	if diff := before - after1; diff < 0.39 || diff > 0.41 {
		t.Fatalf("死亡扣重应为 0.4kg,实际 %.2f", diff)
	}

	// 同一事件再次补传(断网重发)→ 去重,不重复扣重
	res = svc.RecordObservations([]ObservationInput{{
		Meta:    Meta{EventID: "scale-0001", Source: "scale-01", OccurredAt: past},
		Payload: ObservationPayload{LotID: "LOT-1", Kind: ObsMortality, DeadWeightKg: 0.4, DeadCount: 2},
	}})
	if !res[0].Duplicate {
		t.Fatalf("重复补传应被去重")
	}
	if got := lotOf(t, svc, "LOT-1").LiveWeight(); got != after1 {
		t.Fatalf("重复补传导致重复扣重: %.2f → %.2f", after1, got)
	}

	// 批量补传:重复条目与新条目混合,各自独立处理
	res = svc.RecordObservations([]ObservationInput{
		{Meta: Meta{EventID: "scale-0001", Source: "scale-01", OccurredAt: past},
			Payload: ObservationPayload{LotID: "LOT-1", Kind: ObsMortality, DeadWeightKg: 0.4}},
		{Meta: Meta{EventID: "scale-0002", Source: "scale-01", OccurredAt: past},
			Payload: ObservationPayload{LotID: "LOT-1", Kind: ObsMortality, DeadWeightKg: 0.3}},
	})
	if !res[0].Duplicate || res[1].Duplicate || res[1].Error != "" {
		t.Fatalf("批量补传结果错误: %+v", res)
	}
	if got := lotOf(t, svc, "LOT-1").LiveWeight(); got < after1-0.31 || got > after1-0.29 {
		t.Fatalf("批量补传扣重错误: %.2f", got)
	}
}

// 需求:每次交接箱数与活体重量守恒,失败即中止,纠正后恢复。
func TestHandoverConservation(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerLot(t, svc, clock, "LOT-1", "")

	// 箱数不符 → 守恒校验失败,批次中止,责任方不变更
	do(t)(svc.Handover(clock.meta("ho-1"), HandoverPayload{
		LotID: "LOT-1", FromParty: "珲春保税暂养场", ToParty: "冷藏车队 A",
		BoxCount: 2, LiveWeightKg: 35.5, Vehicle: "吉H-12345",
	}))
	lot := lotOf(t, svc, "LOT-1")
	if !lot.Halted || lot.HaltKind != "discrepancy" {
		t.Fatalf("箱数不符应中止: %+v", lot)
	}
	if lot.Custodian != "珲春保税暂养场" {
		t.Fatalf("守恒失败不应变更责任方: %s", lot.Custodian)
	}

	// 申报纠正后守恒通过 → 自动解除中止,责任方转移
	do(t)(svc.Handover(clock.meta("ho-2"), HandoverPayload{
		LotID: "LOT-1", FromParty: "珲春保税暂养场", ToParty: "冷藏车队 A",
		BoxCount: 3, LiveWeightKg: 35.5, Vehicle: "吉H-12345",
	}))
	lot = lotOf(t, svc, "LOT-1")
	if lot.Halted || lot.Custodian != "冷藏车队 A" || lot.Vehicle != "吉H-12345" {
		t.Fatalf("守恒恢复失败: %+v", lot)
	}

	// 死亡 0.6kg 后按旧重量交接 → 再次守恒失败;按新重量 → 通过
	observe(t, svc, clock, "mort-1", ObservationPayload{
		LotID: "LOT-1", Kind: ObsMortality, DeadWeightKg: 0.6, DeadCount: 3,
	})
	do(t)(svc.Handover(clock.meta("ho-3"), HandoverPayload{
		LotID: "LOT-1", FromParty: "冷藏车队 A", ToParty: "延吉机场货站",
		BoxCount: 3, LiveWeightKg: 35.5,
	}))
	if !lotOf(t, svc, "LOT-1").Halted {
		t.Fatalf("活重不符应中止")
	}
	do(t)(svc.Handover(clock.meta("ho-4"), HandoverPayload{
		LotID: "LOT-1", FromParty: "冷藏车队 A", ToParty: "延吉机场货站",
		BoxCount: 3, LiveWeightKg: 34.9,
	}))
	lot = lotOf(t, svc, "LOT-1")
	if lot.Halted || lot.Custodian != "延吉机场货站" {
		t.Fatalf("按新活重交接应通过: %+v", lot)
	}
}

// 需求:仅尚未封运的货物可以重新分配;拆分保持箱数与活重守恒并保留谱系。
func TestReassignOnlyBeforeSeal(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	driveToSealed(t, svc, clock, "LOT-1", "s1")

	// 已封运:改配与拆分均被拒绝
	_, err := svc.Rebind(clock.meta("rb-1"), RebindPayload{LotID: "LOT-1", Destination: "澳门", Reason: "客户改单"})
	mustErr(t, err, "已封运")
	_, err = svc.Split(clock.meta("sp-1"), SplitPayload{LotID: "LOT-1", NewLotID: "LOT-1-A", BoxCodes: []string{"KC-000003"}})
	mustErr(t, err, "已封运")

	// 开封回到检查环节后恢复未封运状态,可以改配
	do(t)(svc.Open(clock.meta("op-1"), "LOT-1", "海关复查开箱"))
	do(t)(svc.Rebind(clock.meta("rb-2"), RebindPayload{LotID: "LOT-1", Destination: "澳门", Reason: "客户改单"}))
	if got := lotOf(t, svc, "LOT-1").Destination; got != "澳门" {
		t.Fatalf("改配未生效: %s", got)
	}

	// 拆分:子批次 1 箱,父批次剩 2 箱,活重总量守恒
	totalBefore := lotOf(t, svc, "LOT-1").LiveWeight()
	do(t)(svc.Split(clock.meta("sp-2"), SplitPayload{
		LotID: "LOT-1", NewLotID: "LOT-1-B", BoxCodes: []string{"KC-000003"},
		Destination: "香港", Reason: "部分货物改回香港订单",
	}))
	parent := lotOf(t, svc, "LOT-1")
	child := lotOf(t, svc, "LOT-1-B")
	if parent.BoxCount() != 2 || child.BoxCount() != 1 {
		t.Fatalf("拆分箱数错误: 父 %d 子 %d", parent.BoxCount(), child.BoxCount())
	}
	if got := parent.LiveWeight() + child.LiveWeight(); got != totalBefore {
		t.Fatalf("拆分前后活重不守恒: %.4f → %.4f", totalBefore, got)
	}
	if child.ParentLotID != "LOT-1" || child.Destination != "香港" {
		t.Fatalf("子批次谱系/去向错误: %+v", child)
	}

	// 谱系查询:箱码链路 父 → 子
	lin, err := svc.Lineage("KC-000003")
	if err != nil {
		t.Fatal(err)
	}
	if len(lin.LotChain) != 2 || lin.LotChain[0].LotID != "LOT-1" || lin.LotChain[1].LotID != "LOT-1-B" {
		t.Fatalf("谱系链错误: %+v", lin.LotChain)
	}
	if lin.CatchBatch != "RU-KC-20260918-A" || lin.Tank != "tank-03" {
		t.Fatalf("谱系缺少捕捞批次/暂养池: %+v", lin)
	}
}

// 需求:已完成联合核验的批次一旦开封就回到相应检查环节,原放行作废。
func TestOpenReturnsToJointCheck(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	driveToReleased(t, svc, clock, "LOT-1", "r1")

	do(t)(svc.Open(clock.meta("op-1"), "LOT-1", "安检复查开箱"))
	lot := lotOf(t, svc, "LOT-1")
	if lot.Stage != StageJointCheck || lot.Sealed || lot.Released {
		t.Fatalf("开封后应回到联合核验环节且放行作废: %+v", lot)
	}
	if lot.VoidedDecisionID != "HG-D001" {
		t.Fatalf("作废决定号未记录: %s", lot.VoidedDecisionID)
	}
	if lot.Joint.PassedAt != nil {
		t.Fatalf("开封后联合核验应重置")
	}

	// 重新核验 → 重新封运 → 新决定号放行(开封后的重新放行,非二次放行)
	driveJointCheckAndSeal(t, svc, clock, "LOT-1", "r2")
	observe(t, svc, clock, "r2-customs", ObservationPayload{
		LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D002",
	})
	lot = lotOf(t, svc, "LOT-1")
	if !lot.Released || lot.ReleaseDecisionID != "HG-D002" {
		t.Fatalf("重新放行失败: %+v", lot)
	}
	if !explainHas(t, svc, "LOT-1", "", "HG-D001") {
		t.Fatalf("解释中应保留作废旧放行的记录")
	}
}

// 需求:指标越界与接近生存阈值时自动提升处置优先级。
func TestPriorityBoost(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerLot(t, svc, clock, "LOT-1", "")
	registerLotBoxes(t, svc, clock, "LOT-2", "", []string{"KC-000101", "KC-000102", "KC-000103"})
	do(t)(svc.Pick(clock.meta("pk-1"), "LOT-1"))

	// 溶氧越界 → 指标越界事件 → 优先级 HIGH
	observe(t, svc, clock, "do-1", ObservationPayload{LotID: "LOT-1", Kind: ObsDissolvedOxygen, Value: 4.5, Unit: "mg/L"})
	lot := lotOf(t, svc, "LOT-1")
	if lot.Violations != 1 || lot.Priority != PriorityHigh {
		t.Fatalf("越界后应提升为 HIGH: %+v", lot)
	}

	// 死亡率超 5% → CRITICAL
	observe(t, svc, clock, "mort-1", ObservationPayload{LotID: "LOT-1", Kind: ObsMortality, DeadWeightKg: 2.0, DeadCount: 12})
	if got := lotOf(t, svc, "LOT-1").Priority; got != PriorityCritical {
		t.Fatalf("死亡率超阈值应为 CRITICAL: %s", got)
	}

	// 时间逼近生存阈值 → CRITICAL(LOT-2 无航班,仅存活时限)
	do(t)(svc.Pick(clock.meta("pk-2"), "LOT-2"))
	clock.advance(7*time.Hour + 30*time.Minute) // 剩余 30 分钟
	view, err := svc.LotDetail("LOT-2")
	if err != nil {
		t.Fatal(err)
	}
	if view.EffectivePriority != PriorityCritical {
		t.Fatalf("接近生存阈值应为 CRITICAL: %s", view.EffectivePriority)
	}

	// 处置队列:CRITICAL 在前
	queue := svc.Queue()
	if len(queue) != 2 || queue[0].Priority != PriorityCritical {
		t.Fatalf("队列排序错误: %+v", queue)
	}
}

// 需求:服务重启不能丢失在途状态。
func TestRestartRecovery(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, path := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	driveToReleased(t, svc, clock, "LOT-1", "r1")
	do(t)(svc.Handover(clock.meta("ho-1"), HandoverPayload{
		LotID: "LOT-1", FromParty: "珲春保税暂养场", ToParty: "冷藏车队 A",
		BoxCount: 3, LiveWeightKg: 35.3, Vehicle: "吉H-12345",
	}))
	observe(t, svc, clock, "dup-c", ObservationPayload{
		LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D001",
	})

	scanBefore, err := svc.ScanBox("KC-000001")
	if err != nil {
		t.Fatal(err)
	}
	logBefore := len(lotOf(t, svc, "LOT-1").DecisionLog)

	// 模拟重启:重新打开日志并回放
	store2, events, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	svc2 := NewService(store2, events, clock.now)

	scanAfter, err := svc2.ScanBox("KC-000001")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scanAfter, scanBefore) {
		t.Fatalf("重启后扫码结果不一致:\n前 %+v\n后 %+v", scanBefore, scanAfter)
	}
	lot := lotOf(t, svc2, "LOT-1")
	if !lot.Released || lot.ReleaseDecisionID != "HG-D001" || lot.ReleaseDuplicates != 1 {
		t.Fatalf("重启后放行状态丢失: %+v", lot)
	}
	if len(lot.DecisionLog) != logBefore {
		t.Fatalf("重启后决策日志条数不一致: %d → %d", logBefore, len(lot.DecisionLog))
	}

	// 重启后幂等仍然有效:原始海关事件重发 → 存储层去重
	res := svc2.RecordObservations([]ObservationInput{{
		Meta:    clock.meta("r1-customs"),
		Payload: ObservationPayload{LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D001"},
	}})
	if !res[0].Duplicate {
		t.Fatalf("重启后重复事件应被去重")
	}
	if got := lotOf(t, svc2, "LOT-1").ReleaseDuplicates; got != 1 {
		t.Fatalf("重启后重复回调被重复计数: %d", got)
	}
}

// 需求:航班延误时,未封运批次获得改配窗口,已封运批次保持绑定。
func TestFlightDelay(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerFlight(t, svc, clock, "KE999")
	registerLot(t, svc, clock, "LOT-A", "KE312")
	registerLotBoxes(t, svc, clock, "LOT-B", "KE312", []string{"KC-000101", "KC-000102", "KC-000103"})
	do(t)(svc.Pick(clock.meta("pk-a"), "LOT-A"))
	driveToSealed(t, svc, clock, "LOT-B", "sb")

	do(t)(svc.DelayFlight(clock.meta("dl-1"), DelayFlightPayload{
		FlightID: "KE312", NewCutoff: base.Add(9 * time.Hour), NewDepartAt: base.Add(10 * time.Hour),
		Reason: "目的港天气",
	}))

	if !explainHas(t, svc, "LOT-A", "", "尚未封运,可在新截载前改配") {
		t.Fatalf("未封运批次缺少改配窗口提示")
	}
	if !explainHas(t, svc, "LOT-B", "", "已封运,保持原绑定") {
		t.Fatalf("已封运批次缺少保持绑定提示")
	}

	// 未封运批次改配到更早航班
	do(t)(svc.Rebind(clock.meta("rb-a"), RebindPayload{LotID: "LOT-A", FlightID: "KE999", Reason: "原航班延误"}))
	if got := lotOf(t, svc, "LOT-A").FlightID; got != "KE999" {
		t.Fatalf("改配航班未生效: %s", got)
	}
}

// 需求:订单变化时,未封运批次跟随改配,已封运批次保持。
func TestOrderChange(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-A", "KE312")
	registerLotBoxes(t, svc, clock, "LOT-B", "KE312", []string{"KC-000101", "KC-000102", "KC-000103"})
	driveToSealed(t, svc, clock, "LOT-B", "sb")

	do(t)(svc.ChangeOrder(clock.meta("oc-1"), OrderChangePayload{
		OrderID: "ORD-HK-01", Destination: "澳门", Reason: "客户改港",
	}))
	if got := lotOf(t, svc, "LOT-A").Destination; got != "澳门" {
		t.Fatalf("未封运批次应跟随订单改配: %s", got)
	}
	if got := lotOf(t, svc, "LOT-B").Destination; got != "香港" {
		t.Fatalf("已封运批次应保持原目的地: %s", got)
	}

	scanA, _ := svc.ScanBox("KC-000001")
	if scanA.AllowedDestination != "澳门" || scanA.DestinationLocked {
		t.Fatalf("未封运批次目的地应可继续调整: %+v", scanA)
	}
	scanB, _ := svc.ScanBox("KC-000101")
	if scanB.AllowedDestination != "香港" || !scanB.DestinationLocked {
		t.Fatalf("已封运批次目的地应锁定: %+v", scanB)
	}
}

// 需求:完整链路走到交付,交付同样校验守恒。
func TestFullJourneyDeliver(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	driveToReleased(t, svc, clock, "LOT-1", "r1")

	// 装机 → 起飞 → 到港
	do(t)(svc.Load(clock.meta("ld-1"), LoadPayload{LotID: "LOT-1", FlightID: "KE312"}))
	do(t)(svc.DepartFlight(clock.meta("dp-1"), "KE312"))
	do(t)(svc.ArriveFlight(clock.meta("ar-1"), "KE312"))
	if got := lotOf(t, svc, "LOT-1").Stage; got != StageArrived {
		t.Fatalf("应已到港: %s", got)
	}

	// 交付箱数不符 → 守恒失败中止;人工解除后按正确数量交付
	do(t)(svc.Deliver(clock.meta("dv-1"), DeliverPayload{
		LotID: "LOT-1", ToParty: "香港分拨中心", BoxCount: 2, LiveWeightKg: 35.3,
	}))
	lot := lotOf(t, svc, "LOT-1")
	if !lot.Halted || lot.Stage != StageArrived {
		t.Fatalf("交付守恒失败应中止且停留在到港环节: %+v", lot)
	}
	do(t)(svc.Resume(clock.meta("rs-1"), "LOT-1", "现场复核确认为申报笔误"))
	do(t)(svc.Deliver(clock.meta("dv-2"), DeliverPayload{
		LotID: "LOT-1", ToParty: "香港分拨中心", BoxCount: 3, LiveWeightKg: 35.3,
	}))
	lot = lotOf(t, svc, "LOT-1")
	if lot.Stage != StageDelivered || lot.Custodian != "香港分拨中心" {
		t.Fatalf("交付失败: %+v", lot)
	}

	// 已交付批次不再出现在处置队列
	for _, item := range svc.Queue() {
		if item.LotID == "LOT-1" {
			t.Fatalf("已交付批次不应在处置队列中")
		}
	}
}

// 需求:事后查询能解释在哪一步被中止、为何中止。
func TestExplainHalt(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerLot(t, svc, clock, "LOT-1", "")
	do(t)(svc.Halt(clock.meta("ht-1"), "LOT-1", "现场查验发现标签异常"))

	entries, err := svc.Explain("LOT-1", "halt")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Kind != LogHalt {
		t.Fatalf("中止解释条目错误: %+v", entries)
	}
	if !strings.Contains(entries[0].Summary, "保税暂养中") || !strings.Contains(entries[0].Summary, "标签异常") {
		t.Fatalf("中止条目应包含环节与原因: %s", entries[0].Summary)
	}

	scan, _ := svc.ScanBox("KC-000001")
	if !scan.Halted || !strings.Contains(scan.Regulatory, "标签异常") {
		t.Fatalf("扫码监管结论应体现中止: %+v", scan)
	}
}

// 需求:扫码应答包含当前责任方、剩余时限、监管结论、允许目的地。
func TestScanAnswerFields(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	do(t)(svc.Pick(clock.meta("pk-1"), "LOT-1"))
	do(t)(svc.Handover(clock.meta("ho-1"), HandoverPayload{
		LotID: "LOT-1", FromParty: "珲春保税暂养场", ToParty: "冷藏车队 A",
		BoxCount: 3, LiveWeightKg: 35.5, Vehicle: "吉H-12345",
	}))

	clock.advance(time.Hour)
	scan, err := svc.ScanBox("KC-000002")
	if err != nil {
		t.Fatal(err)
	}
	if scan.Custodian != "冷藏车队 A" {
		t.Fatalf("责任方错误: %s", scan.Custodian)
	}
	if scan.RemainingSeconds == nil || scan.RemainingBasis == "" {
		t.Fatalf("缺少剩余时限: %+v", scan)
	}
	// 航班截载(base+6h)早于存活时限(base+8h),剩余约 5 小时
	if *scan.RemainingSeconds < 4*3600 || *scan.RemainingSeconds > 6*3600 {
		t.Fatalf("剩余时限不合理: %d 秒(基准 %s)", *scan.RemainingSeconds, scan.RemainingBasis)
	}
	if scan.Regulatory == "" || scan.AllowedDestination != "香港" {
		t.Fatalf("监管结论/允许目的地缺失: %+v", scan)
	}
	if len(scan.Constraints) != 2 {
		t.Fatalf("应同时给出存活时限与航班截载两个约束: %+v", scan.Constraints)
	}
}

// 需求:合并批次保持守恒并保留谱系。
func TestMergeLots(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	registerLot(t, svc, clock, "LOT-1", "")
	registerLotBoxes(t, svc, clock, "LOT-2", "", []string{"KC-000101"})

	do(t)(svc.Merge(clock.meta("mg-1"), MergePayload{
		LotIDs: []string{"LOT-1", "LOT-2"}, NewLotID: "LOT-M", Reason: "同一航班拼板",
	}))
	merged := lotOf(t, svc, "LOT-M")
	if merged.BoxCount() != 4 {
		t.Fatalf("合并后箱数错误: %d", merged.BoxCount())
	}
	if got := merged.LiveWeight(); got < 47.2 || got > 47.4 {
		t.Fatalf("合并后活重错误: %.2f", got)
	}
	if !lotOf(t, svc, "LOT-1").Closed || !lotOf(t, svc, "LOT-2").Closed {
		t.Fatalf("合并后来源批次应关闭")
	}
	scan, err := svc.ScanBox("KC-000101")
	if err != nil || scan.LotID != "LOT-M" {
		t.Fatalf("合并后箱码应归属新批次: %+v", scan)
	}
}

// 需求:崩溃窗口(主事件已落盘、派生事件未落盘)在重启后补齐,且不产生副作用。
func TestCrashWindowRecovery(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, path := newTestService(t, clock)
	registerFlight(t, svc, clock, "KE312")
	registerLot(t, svc, clock, "LOT-1", "KE312")
	driveToSealed(t, svc, clock, "LOT-1", "s1")

	// 模拟崩溃:海关合格事件直接写入日志,未经规则引擎(派生的放行事件"丢失")
	raw, _, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	crashEvent := &Event{
		ID: "cus-crash", Type: EvObservation, Source: "customs-sys",
		OccurredAt: base.Add(time.Hour),
		Payload: marshalPayload(ObservationPayload{
			LotID: "LOT-1", Kind: ObsCustoms, Conclusion: "pass", DecisionID: "HG-D009",
		}),
	}
	if _, dup, err := raw.Append(crashEvent); err != nil || dup {
		t.Fatalf("写入崩溃事件失败: dup=%v err=%v", dup, err)
	}
	raw.Close()

	// 重启:恢复流程应对最后一条外部事件重跑规则引擎,补齐放行
	store2, events2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	svc2 := NewService(store2, events2, clock.now)
	lot := lotOf(t, svc2, "LOT-1")
	if !lot.Released || lot.ReleaseDecisionID != "HG-D009" {
		t.Fatalf("崩溃窗口内的放行应被补齐: %+v", lot)
	}
	if lot.ReleaseDuplicates != 0 {
		t.Fatalf("恢复重跑不应产生虚假的重复放行记录: %d", lot.ReleaseDuplicates)
	}

	// 再次重启:补齐的派生事件已持久化,恢复重跑保持幂等
	store3, events3, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store3.Close()
	svc3 := NewService(store3, events3, clock.now)
	lot = lotOf(t, svc3, "LOT-1")
	if !lot.Released || lot.ReleaseDecisionID != "HG-D009" || lot.ReleaseDuplicates != 0 {
		t.Fatalf("二次重启后状态应保持一致: %+v", lot)
	}
}

// HTTP 冒烟:注册 → 幂等重放 → 扫码。
func TestHTTPSmoke(t *testing.T) {
	clock := &fakeClock{t: base}
	svc, _ := newTestService(t, clock)
	h := NewHandler(svc)

	post := func(path, body string) (int, map[string]any) {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var v map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &v)
		return rec.Code, v
	}

	body := `{
		"event_id": "http-reg-1", "source": "gateway",
		"lot_id": "LOT-1", "catch_batch": "RU-KC-20260918-A", "tank": "tank-03",
		"boxes": [{"code": "KC-000001", "weight_kg": 11.8}],
		"order_id": "ORD-HK-01", "destination": "香港", "custodian": "珲春保税暂养场",
		"survival_budget_hours": 8, "temp_min": 2, "temp_max": 8, "do_min": 6
	}`
	code, v := post("/v1/lots", body)
	if code != 200 || v["duplicate"] == true {
		t.Fatalf("注册批次失败: %d %+v", code, v)
	}
	// 同一 event_id 重放 → duplicate
	code, v = post("/v1/lots", body)
	if code != 200 || v["duplicate"] != true {
		t.Fatalf("幂等重放应返回 duplicate: %d %+v", code, v)
	}

	req := httptest.NewRequest("GET", "/v1/boxes/KC-000001", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("扫码失败: %d", rec.Code)
	}
	var scan map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &scan)
	if scan["custodian"] != "珲春保税暂养场" || scan["allowed_destination"] != "香港" {
		t.Fatalf("扫码应答字段错误: %+v", scan)
	}
}
