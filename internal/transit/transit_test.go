package transit_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"example.com/live-seafood-transit/internal/transit"
)

var testNow = time.Date(2026, 9, 20, 8, 0, 0, 0, time.UTC)

// newService 创建使用临时目录与固定时钟的服务。
func newService(t *testing.T) (*transit.Service, *transit.Store) {
	t.Helper()
	return newServiceAt(t, testNow)
}

func newServiceAt(t *testing.T, now time.Time) (*transit.Service, *transit.Store) {
	t.Helper()
	store, err := transit.Open(filepath.Join(t.TempDir(), "events.jsonl"), transit.DefaultThresholds(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return transit.NewService(store), store
}

func mustErr(t *testing.T, err error, status int) {
	t.Helper()
	var be *transit.Error
	if !errors.As(err, &be) {
		t.Fatalf("期望业务错误,得到 %v", err)
	}
	if be.Status != status {
		t.Fatalf("期望状态码 %d,得到 %d(%s)", status, be.Status, be.Message)
	}
}

// registerBatch 登记捕捞批次:20 小时前起捕,可存活 30 小时(剩余约 10 小时)。
func registerBatch(t *testing.T, svc *transit.Service, id string, boxes int, weight float64) {
	t.Helper()
	_, err := svc.RegisterCatchBatch(transit.CatchBatchRegistered{
		CatchBatchID:  id,
		Species:       "俄罗斯帝王蟹",
		Origin:        "Vladivostok",
		HarvestedAt:   testNow.Add(-20 * time.Hour),
		SurvivalHours: 30,
		TotalBoxes:    boxes,
		TotalWeightKg: weight,
	}, transit.Meta{Actor: "港口理货员"})
	if err != nil {
		t.Fatal(err)
	}
}

func createLot(t *testing.T, svc *transit.Service, lotID, batchID string, boxes ...transit.BoxSpec) {
	t.Helper()
	_, err := svc.CreateBondedLot(transit.BondedLotCreated{
		LotID:        lotID,
		CatchBatchID: batchID,
		TankID:       "tank-03",
		Custodian:    "珲春保税暂养库",
		Boxes:        boxes,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
}

func registerOrder(t *testing.T, svc *transit.Service, orderID, destination string) {
	t.Helper()
	_, err := svc.RegisterOrder(transit.OrderRegistered{
		OrderID:          orderID,
		Customer:         "香港海产行",
		Destination:      destination,
		RequiredBoxes:    2,
		RequiredWeightKg: 20,
		LatestArrival:    testNow.Add(14 * time.Hour),
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
}

func passInspections(t *testing.T, svc *transit.Service, lotID string) {
	t.Helper()
	for i, kind := range []transit.InspectionKind{transit.InspectionSecurity, transit.InspectionCustoms, transit.InspectionJoint} {
		_, err := svc.RecordInspection(transit.InspectionRecorded{
			LotID: lotID, Kind: kind, Result: transit.InspectionPass,
			Authority: "口岸联检", CallbackID: "cb-" + string(kind) + "-1",
		}, transit.Meta{Actor: "联检系统"})
		if err != nil {
			t.Fatalf("第 %d 项检查登记失败: %v", i, err)
		}
	}
}

// setupSealedLot 建立一条已封运批次:2 箱共 20 kg。
func setupSealedLot(t *testing.T, svc *transit.Service) string {
	t.Helper()
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	registerOrder(t, svc, "ORD-1", "Hong Kong")
	if _, err := svc.Allocate("LOT-1", "ORD-1", "truck-01", "flight-01", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	passInspections(t, svc, "LOT-1")
	if _, err := svc.Seal("LOT-1", "SEAL-1", transit.Meta{Actor: "海关关员"}); err != nil {
		t.Fatal(err)
	}
	return "LOT-1"
}

// 需求:扫描任一箱码得到当前责任方、剩余时限、监管结论和允许目的地。
func TestScanBoxReturnsCustodianDeadlineConclusionAndDestinations(t *testing.T) {
	svc, _ := newService(t)
	setupSealedLot(t, svc)
	if _, err := svc.Release("LOT-1", "REL-1", "珲春海关", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	scan, err := svc.ScanBox("KC-1")
	if err != nil {
		t.Fatal(err)
	}
	if scan.ResponsibleParty != "珲春保税暂养库" {
		t.Fatalf("责任方错误: %s", scan.ResponsibleParty)
	}
	// 起捕 20 小时、可存活 30 小时,剩余应约 10 小时。
	if scan.RemainingSeconds != 10*3600 {
		t.Fatalf("剩余时限错误: %d 秒", scan.RemainingSeconds)
	}
	if scan.RegulatoryConclusion != "released" {
		t.Fatalf("监管结论错误: %s", scan.RegulatoryConclusion)
	}
	if len(scan.AllowedDestinations) != 1 || scan.AllowedDestinations[0] != "Hong Kong" {
		t.Fatalf("允许目的地错误: %v", scan.AllowedDestinations)
	}
	if scan.CanReallocate {
		t.Fatal("已放行批次不应允许改配")
	}
}

// 需求:多部门重复回调不能造成二次放行。
func TestReleaseIsIdempotentAndRejectsSecondRelease(t *testing.T) {
	svc, store := newService(t)
	setupSealedLot(t, svc)
	res, err := svc.Release("LOT-1", "REL-1", "珲春海关", transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Duplicate {
		t.Fatal("首次放行不应标记为重复")
	}
	// 同一放行号重复回调:返回原结果,不产生新事件。
	before := len(store.LotEvents("LOT-1"))
	res, err = svc.Release("LOT-1", "REL-1", "珲春海关", transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatal("重复回调应标记为重复")
	}
	if got := len(store.LotEvents("LOT-1")); got != before {
		t.Fatalf("重复放行产生了新事件: %d -> %d", before, got)
	}
	// 不同放行号:拒绝二次放行。
	_, err = svc.Release("LOT-1", "REL-2", "珲春海关", transit.Meta{})
	mustErr(t, err, 409)
}

// 需求:重复的检查回调只入账一次。
func TestDuplicateInspectionCallbackRecordedOnce(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	insp := transit.InspectionRecorded{LotID: "LOT-1", Kind: transit.InspectionCustoms, Result: transit.InspectionPass, Authority: "珲春海关", CallbackID: "cb-1"}
	if _, err := svc.RecordInspection(insp, transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	res, err := svc.RecordInspection(insp, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatal("重复回调应标记为重复")
	}
	if got := len(res.Lot.Inspections); got != 1 {
		t.Fatalf("检查结论被重复入账: %d 条", got)
	}
}

// 需求:离线补传(相同设备序号)幂等,晚到的发生时间被保留。
func TestOfflineTelemetryReuploadIsIdempotent(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	past := testNow.Add(-3 * time.Hour) // 设备离线 3 小时后补传
	tel := transit.TelemetryAppended{DeviceID: "tank-sensor-03", DeviceSeq: 42, LotID: "LOT-1", Kind: transit.TelemetryTemperature, Value: 2.1}
	if _, err := svc.AppendTelemetry(tel, transit.Meta{OccurredAt: past}); err != nil {
		t.Fatal(err)
	}
	res, err := svc.AppendTelemetry(tel, transit.Meta{OccurredAt: past})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatal("离线补传应被识别为重复")
	}
	if got := len(res.Lot.Telemetry); got != 1 {
		t.Fatalf("补传被重复入账: %d 条", got)
	}
	if !res.Lot.Telemetry[0].OccurredAt.Equal(past) {
		t.Fatalf("发生时间未被保留: %v", res.Lot.Telemetry[0].OccurredAt)
	}
}

// 需求:每次交接后的箱数与活体重量都要守恒。
func TestHandoverConservation(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	// 守恒交接:暂养库 -> 冷藏车。
	res, err := svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-1", LotID: "LOT-1", FromParty: "珲春保税暂养库", ToParty: "冷藏车 truck-01",
		BoxCount: 2, LiveWeightKg: 20,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Custodian != "冷藏车 truck-01" {
		t.Fatalf("责任方未转移: %s", res.Lot.Custodian)
	}
	// 交出方与当前责任方不一致:拒绝。
	_, err = svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-2", LotID: "LOT-1", FromParty: "珲春保税暂养库", ToParty: "机场货站",
		BoxCount: 2, LiveWeightKg: 20,
	}, transit.Meta{})
	mustErr(t, err, 409)
	// 重量不守恒:记录差异并在交接环节中止。
	res, err = svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-3", LotID: "LOT-1", FromParty: "冷藏车 truck-01", ToParty: "机场货站",
		BoxCount: 2, LiveWeightKg: 19.5,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Status != transit.StatusHalted {
		t.Fatalf("不守恒交接后批次应中止,状态 %s", res.Lot.Status)
	}
	if !strings.Contains(res.Lot.HaltStep, "handover") {
		t.Fatalf("中止环节应记录为交接: %s", res.Lot.HaltStep)
	}
	// 中止期间拒绝新交接;处置后恢复。
	_, err = svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-4", LotID: "LOT-1", FromParty: "机场货站", ToParty: "航空公司",
		BoxCount: 2, LiveWeightKg: 20,
	}, transit.Meta{})
	mustErr(t, err, 409)
	res, err = svc.Resume("LOT-1", transit.Meta{Reason: "差异已查明并更正台账"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Status != transit.StatusHolding {
		t.Fatalf("恢复后状态错误: %s", res.Lot.Status)
	}
}

// 需求:死亡抽检调整台账,后续交接按调整后的活体重量守恒。
func TestMortalityAdjustsLedgerBeforeHandover(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	res, err := svc.AppendTelemetry(transit.TelemetryAppended{
		DeviceID: "inspector-01", DeviceSeq: 1, LotID: "LOT-1",
		Kind: transit.TelemetryMortality, DeadCount: 1, DeadWeightKg: 1.5,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Lot.LiveWeightKg; got != 18.5 {
		t.Fatalf("台账未按死亡抽检调整: %.3f", got)
	}
	// 按旧重量交接:不守恒,批次中止。
	res, err = svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-1", LotID: "LOT-1", FromParty: "珲春保税暂养库", ToParty: "冷藏车 truck-01",
		BoxCount: 2, LiveWeightKg: 20,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Status != transit.StatusHalted {
		t.Fatal("按调整前重量交接应被判定为不守恒")
	}
	if _, err := svc.Resume("LOT-1", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	// 按调整后重量交接:守恒通过。注意上一笔差异交接已把责任方移到冷藏车。
	res, err = svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-2", LotID: "LOT-1", FromParty: "冷藏车 truck-01", ToParty: "机场货站",
		BoxCount: 2, LiveWeightKg: 18.5,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Custodian != "机场货站" {
		t.Fatalf("守恒交接未转移责任方: %s", res.Lot.Custodian)
	}
}

// 需求:拆分合并谱系保留,且箱数与活体重量守恒。
func TestSplitAndMergeConserveAndKeepLineage(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 4, 40)
	createLot(t, svc, "LOT-1", "CB-1",
		transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10},
		transit.BoxSpec{BoxCode: "KC-3", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-4", WeightKg: 10})
	// 拆分漏箱:不守恒,拒绝。
	_, err := svc.SplitLot("LOT-1", []transit.SplitChild{
		{LotID: "LOT-1A", TankID: "tank-03", BoxCodes: []string{"KC-1", "KC-2"}},
		{LotID: "LOT-1B", TankID: "tank-04", BoxCodes: []string{"KC-3"}},
	}, transit.Meta{})
	mustErr(t, err, 409)
	// 正确拆分:不重不漏。
	if _, err := svc.SplitLot("LOT-1", []transit.SplitChild{
		{LotID: "LOT-1A", TankID: "tank-03", BoxCodes: []string{"KC-1", "KC-2"}},
		{LotID: "LOT-1B", TankID: "tank-04", BoxCodes: []string{"KC-3", "KC-4"}},
	}, transit.Meta{Reason: "分池降温"}); err != nil {
		t.Fatal(err)
	}
	parent, _ := svc.GetLot("LOT-1")
	if parent.Lot.Status != transit.StatusClosed || parent.Lot.ClosedBy != "split" {
		t.Fatalf("父批次应关闭: %s/%s", parent.Lot.Status, parent.Lot.ClosedBy)
	}
	child, _ := svc.GetLot("LOT-1A")
	if child.Lot.BoxCount != 2 || child.Lot.LiveWeightKg != 20 {
		t.Fatalf("子批次数量不守恒: %d 箱 %.3f kg", child.Lot.BoxCount, child.Lot.LiveWeightKg)
	}
	if len(child.Lot.ParentLotIDs) != 1 || child.Lot.ParentLotIDs[0] != "LOT-1" {
		t.Fatalf("拆分谱系缺失: %v", child.Lot.ParentLotIDs)
	}
	// 箱码索引指向新批次。
	scan, err := svc.ScanBox("KC-3")
	if err != nil {
		t.Fatal(err)
	}
	if scan.LotID != "LOT-1B" {
		t.Fatalf("箱码谱系未更新: %s", scan.LotID)
	}
	// 合并:重量与箱数为父批次之和。
	res, err := svc.MergeLots([]string{"LOT-1A", "LOT-1B"}, "LOT-2", "tank-05", "", transit.Meta{Reason: "合并装车"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.BoxCount != 4 || res.Lot.LiveWeightKg != 40 {
		t.Fatalf("合并不守恒: %d 箱 %.3f kg", res.Lot.BoxCount, res.Lot.LiveWeightKg)
	}
	if len(res.Lot.ParentLotIDs) != 2 {
		t.Fatalf("合并谱系缺失: %v", res.Lot.ParentLotIDs)
	}
	if len(res.Lot.CatchBatchIDs) != 1 || res.Lot.CatchBatchIDs[0] != "CB-1" {
		t.Fatalf("捕捞批次谱系缺失: %v", res.Lot.CatchBatchIDs)
	}
}

// 需求:仅尚未封运的货物可以重新分配;已封运批次开封后回到相应检查环节。
func TestSealedLotCannotBeReallocatedUntilUnsealed(t *testing.T) {
	svc, _ := newService(t)
	setupSealedLot(t, svc)
	// 已封运:禁止改配。
	_, err := svc.Reallocate("LOT-1", "ORD-1", "truck-02", "flight-02", transit.TriggerFlightDelay, transit.Meta{})
	mustErr(t, err, 409)
	// 开封:回到联合核验环节,联合核验结论作废。
	res, err := svc.Unseal("LOT-1", transit.Meta{Reason: "航班取消,需重新配载", Actor: "口岸调度"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Status != transit.StatusInspecting {
		t.Fatalf("开封后应回到检查环节: %s", res.Lot.Status)
	}
	if _, ok := res.Lot.Inspections[string(transit.InspectionJoint)]; ok {
		t.Fatal("联合核验结论应在开封后作废")
	}
	if !res.Lot.Status.Reallocatable() {
		t.Fatal("开封后应允许改配")
	}
	// 未封运状态可以改配。
	if _, err := svc.Reallocate("LOT-1", "ORD-1", "truck-02", "flight-02", transit.TriggerFlightDelay, transit.Meta{Reason: "原航班取消"}); err != nil {
		t.Fatal(err)
	}
	// 重新联合核验、重新封识后才能放行。
	if _, err := svc.Seal("LOT-1", "SEAL-2", transit.Meta{}); err == nil {
		t.Fatal("联合核验未重做前不应允许封识")
	}
	if _, err := svc.RecordInspection(transit.InspectionRecorded{
		LotID: "LOT-1", Kind: transit.InspectionJoint, Result: transit.InspectionPass,
		Authority: "口岸联检", CallbackID: "cb-joint-2",
	}, transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Seal("LOT-1", "SEAL-2", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Release("LOT-1", "REL-9", "珲春海关", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
}

// 需求:航班延误时标记受影响批次,改配记录触发原因供事后解释。
func TestFlightDelayTriggersReallocationWithExplanation(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	registerOrder(t, svc, "ORD-1", "Hong Kong")
	if _, err := svc.RegisterFlight(transit.FlightRegistered{FlightID: "flight-01", Destination: "Hong Kong", DepartureAt: testNow.Add(2 * time.Hour)}, transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Allocate("LOT-1", "ORD-1", "truck-01", "flight-01", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	delay, err := svc.DelayFlight("flight-01", testNow.Add(9*time.Hour), transit.Meta{Reason: "台风,包机延误"})
	if err != nil {
		t.Fatal(err)
	}
	lot, _ := svc.GetLot("LOT-1")
	if !lot.Lot.ReallocSuggested {
		t.Fatal("航班延误后应标记为待改配")
	}
	// 改配到备份航班,关联延误事件作为触发源。
	res, err := svc.Reallocate("LOT-1", "ORD-1", "truck-01", "flight-02", transit.TriggerFlightDelay,
		transit.Meta{Reason: "原航班延误 7 小时", CausedBy: delay.EventIDs[0]})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.FlightID != "flight-02" || res.Lot.ReallocSuggested {
		t.Fatal("改配未生效")
	}
	detail, _ := svc.GetLot("LOT-1")
	if len(detail.Explanation.Reallocations) != 1 {
		t.Fatalf("缺少改配解释: %+v", detail.Explanation)
	}
	re := detail.Explanation.Reallocations[0]
	if re.Trigger != transit.TriggerFlightDelay || !strings.Contains(re.CausedBySummary, "flight_delayed") {
		t.Fatalf("改配解释不完整: %+v", re)
	}
}

// 需求:指标越界与接近生存阈值都会自动提升处置优先级。
func TestPriorityRaisedOnBreachAndNearDeadline(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	// 温度越界:记录越界事件并提升优先级。
	res, err := svc.AppendTelemetry(transit.TelemetryAppended{
		DeviceID: "tank-sensor-03", DeviceSeq: 7, LotID: "LOT-1", Kind: transit.TelemetryTemperature, Value: 8.2,
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Priority != transit.PriorityHigh {
		t.Fatalf("越界后优先级应提升: %s", res.Lot.Priority)
	}
	// 接近生存阈值:扫码视图按剩余时限推算为 critical。
	nearDeadline := testNow.Add(9*time.Hour + 30*time.Minute) // 剩余 30 分钟
	svc2, _ := newServiceAt(t, nearDeadline)
	registerBatch(t, svc2, "CB-2", 1, 10)
	createLot(t, svc2, "LOT-2", "CB-2", transit.BoxSpec{BoxCode: "KC-9", WeightKg: 10})
	scan, err := svc2.ScanBox("KC-9")
	if err != nil {
		t.Fatal(err)
	}
	if scan.Priority != transit.PriorityCritical {
		t.Fatalf("临阈批次应提升为 critical: %s", scan.Priority)
	}
}

// 需求:事后查询能解释为何一次放行、为何改配、在哪一步被中止。
func TestExplanationCoversReleaseReallocationAndHalt(t *testing.T) {
	svc, _ := newService(t)
	setupSealedLot(t, svc)
	if _, err := svc.Release("LOT-1", "REL-1", "珲春海关", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	detail, err := svc.GetLot("LOT-1")
	if err != nil {
		t.Fatal(err)
	}
	rel := detail.Explanation.Release
	if rel == nil || rel.ReleaseID != "REL-1" || rel.Authority != "珲春海关" {
		t.Fatalf("放行解释缺失: %+v", detail.Explanation)
	}
	if len(rel.InspectionEventIDs) != 3 || rel.SealEventID == "" {
		t.Fatalf("放行依据不完整: %+v", rel)
	}
	// 中止解释:在海关查验环节中止。
	registerBatch(t, svc, "CB-2", 1, 10)
	createLot(t, svc, "LOT-2", "CB-2", transit.BoxSpec{BoxCode: "KC-8", WeightKg: 10})
	if _, err := svc.Halt("LOT-2", "customs", transit.Meta{Reason: "查验发现货证不符"}); err != nil {
		t.Fatal(err)
	}
	detail, _ = svc.GetLot("LOT-2")
	if detail.Explanation.Halt == nil || detail.Explanation.Halt.Step != "customs" {
		t.Fatalf("中止解释缺失: %+v", detail.Explanation)
	}
	// 历史链完整可追溯。
	history, err := svc.LotHistory("LOT-1")
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, ev := range history {
		types = append(types, ev.Type)
	}
	joined := strings.Join(types, ",")
	for _, want := range []string{"bonded_lot_created", "lot_allocated", "inspection_recorded", "lot_sealed", "lot_released"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("历史链缺少 %s: %s", want, joined)
		}
	}
}

// 需求:服务重启不能丢失在途状态。
func TestRestartRestoresInFlightState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	now := testNow
	open := func() (*transit.Service, *transit.Store) {
		store, err := transit.Open(path, transit.DefaultThresholds(), func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		return transit.NewService(store), store
	}
	svc, store := open()
	setupSealedLot(t, svc)
	if _, err := svc.Release("LOT-1", "REL-1", "珲春海关", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Handover(transit.HandoverRecorded{
		HandoverID: "HO-1", LotID: "LOT-1", FromParty: "珲春保税暂养库", ToParty: "冷藏车 truck-01",
		BoxCount: 2, LiveWeightKg: 20,
	}, transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	before, err := svc.ScanBox("KC-1")
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()
	// 模拟重启:重放事件日志。
	svc2, store2 := open()
	defer func() { _ = store2.Close() }()
	after, err := svc2.ScanBox("KC-1")
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := json.Marshal(before)
	b2, _ := json.Marshal(after)
	if !bytes.Equal(b1, b2) {
		t.Fatalf("重启后扫码视图不一致:\n%s\n%s", b1, b2)
	}
	// 重启后幂等键仍然有效:重复放行不会二次生效。
	res, err := svc2.Release("LOT-1", "REL-1", "珲春海关", transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Duplicate {
		t.Fatal("重启后重复放行应仍被识别")
	}
}

// 需求:订单变化标记受影响批次为待改配。
func TestOrderChangeFlagsLotForReallocation(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	registerOrder(t, svc, "ORD-1", "Hong Kong")
	registerOrder(t, svc, "ORD-2", "Singapore")
	if _, err := svc.Allocate("LOT-1", "ORD-1", "truck-01", "flight-01", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ChangeOrder("ORD-1", "客户取消一半货量", transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	lot, _ := svc.GetLot("LOT-1")
	if !lot.Lot.ReallocSuggested || !strings.Contains(lot.Lot.ReallocReason, "订单变化") {
		t.Fatalf("订单变化后应标记待改配: %+v", lot.Lot)
	}
	if _, err := svc.Reallocate("LOT-1", "ORD-2", "truck-01", "flight-03", transit.TriggerOrderChange, transit.Meta{}); err != nil {
		t.Fatal(err)
	}
	scan, _ := svc.ScanBox("KC-1")
	if len(scan.AllowedDestinations) != 1 || scan.AllowedDestinations[0] != "Singapore" {
		t.Fatalf("改配后允许目的地错误: %v", scan.AllowedDestinations)
	}
}

// 需求:检查不通过使批次在相应环节中止,恢复后可重新检查。
func TestFailedInspectionHaltsAtStep(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	res, err := svc.RecordInspection(transit.InspectionRecorded{
		LotID: "LOT-1", Kind: transit.InspectionSecurity, Result: transit.InspectionFail,
		Authority: "机场安检", CallbackID: "cb-sec-1",
	}, transit.Meta{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Lot.Status != transit.StatusHalted || res.Lot.HaltStep != "security" {
		t.Fatalf("应在安检环节中止: %s/%s", res.Lot.Status, res.Lot.HaltStep)
	}
	// 中止期间不接受新的检查结果。
	_, err = svc.RecordInspection(transit.InspectionRecorded{
		LotID: "LOT-1", Kind: transit.InspectionSecurity, Result: transit.InspectionPass,
		Authority: "机场安检", CallbackID: "cb-sec-2",
	}, transit.Meta{})
	mustErr(t, err, 409)
	if _, err := svc.Resume("LOT-1", transit.Meta{Reason: "复检合格,恢复流程"}); err != nil {
		t.Fatal(err)
	}
}

// 需求:入池数量不得超过捕捞批次余量(源头守恒)。
func TestLotCreationCannotExceedCatchBatch(t *testing.T) {
	svc, _ := newService(t)
	registerBatch(t, svc, "CB-1", 2, 20)
	createLot(t, svc, "LOT-1", "CB-1", transit.BoxSpec{BoxCode: "KC-1", WeightKg: 10}, transit.BoxSpec{BoxCode: "KC-2", WeightKg: 10})
	_, err := svc.CreateBondedLot(transit.BondedLotCreated{
		LotID: "LOT-2", CatchBatchID: "CB-1", TankID: "tank-03",
		Boxes: []transit.BoxSpec{{BoxCode: "KC-3", WeightKg: 10}},
	}, transit.Meta{})
	mustErr(t, err, 409)
}

// 需求:HTTP 层联通,扫码接口可用。
func TestHTTPAPIEndToEnd(t *testing.T) {
	svc, _ := newService(t)
	srv := httptest.NewServer(transit.NewHandler(svc))
	defer srv.Close()

	post := func(path string, body any) *http.Response {
		t.Helper()
		raw, _ := json.Marshal(body)
		resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp
	}
	if resp := post("/api/catch-batches", map[string]any{
		"catch_batch_id": "CB-1", "species": "俄罗斯帝王蟹", "origin": "Vladivostok",
		"harvested_at": testNow.Add(-20 * time.Hour), "survival_hours": 30,
		"total_boxes": 2, "total_weight_kg": 20,
	}); resp.StatusCode != 200 {
		t.Fatalf("登记捕捞批次失败: %d", resp.StatusCode)
	}
	if resp := post("/api/lots", map[string]any{
		"lot_id": "LOT-1", "catch_batch_id": "CB-1", "tank_id": "tank-03",
		"boxes": []map[string]any{{"box_code": "KC-1", "weight_kg": 10}, {"box_code": "KC-2", "weight_kg": 10}},
	}); resp.StatusCode != 200 {
		t.Fatalf("建立暂养批次失败: %d", resp.StatusCode)
	}
	resp, err := http.Get(srv.URL + "/api/boxes/KC-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("扫码失败: %d", resp.StatusCode)
	}
	var scan transit.ScanView
	if err := json.NewDecoder(resp.Body).Decode(&scan); err != nil {
		t.Fatal(err)
	}
	if scan.LotID != "LOT-1" || scan.ResponsibleParty == "" || scan.RegulatoryConclusion != "pending_inspection" {
		t.Fatalf("扫码视图不完整: %+v", scan)
	}
}
