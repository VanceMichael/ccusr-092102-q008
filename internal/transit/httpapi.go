package transit

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// NewHandler 构建 HTTP API。所有写命令都接受可选的 Idempotency-Key 请求头,
// 与请求体中的 idempotency_key 等价(请求体优先)。
func NewHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()
	h := &handlers{svc: svc}

	mux.HandleFunc("GET /health", h.health)

	mux.HandleFunc("POST /api/catch-batches", h.createCatchBatch)
	mux.HandleFunc("POST /api/lots", h.createLot)
	mux.HandleFunc("GET /api/lots", h.listLots)
	mux.HandleFunc("POST /api/lots/merge", h.mergeLots)
	mux.HandleFunc("GET /api/lots/{id}", h.getLot)
	mux.HandleFunc("GET /api/lots/{id}/history", h.lotHistory)
	mux.HandleFunc("POST /api/lots/{id}/split", h.splitLot)
	mux.HandleFunc("POST /api/lots/{id}/allocate", h.allocate)
	mux.HandleFunc("POST /api/lots/{id}/reallocate", h.reallocate)
	mux.HandleFunc("POST /api/lots/{id}/seal", h.seal)
	mux.HandleFunc("POST /api/lots/{id}/unseal", h.unseal)
	mux.HandleFunc("POST /api/lots/{id}/release", h.release)
	mux.HandleFunc("POST /api/lots/{id}/halt", h.halt)
	mux.HandleFunc("POST /api/lots/{id}/resume", h.resume)
	mux.HandleFunc("POST /api/lots/{id}/depart", h.depart)
	mux.HandleFunc("POST /api/lots/{id}/deliver", h.deliver)

	mux.HandleFunc("POST /api/orders", h.createOrder)
	mux.HandleFunc("GET /api/orders", h.listOrders)
	mux.HandleFunc("POST /api/orders/{id}/changes", h.changeOrder)

	mux.HandleFunc("POST /api/flights", h.createFlight)
	mux.HandleFunc("GET /api/flights", h.listFlights)
	mux.HandleFunc("POST /api/flights/{id}/delay", h.delayFlight)

	mux.HandleFunc("POST /api/telemetry", h.telemetry)
	mux.HandleFunc("POST /api/inspections", h.inspection)
	mux.HandleFunc("POST /api/handovers", h.handover)

	mux.HandleFunc("GET /api/boxes/{code}", h.scanBox)

	return mux
}

type handlers struct{ svc *Service }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeResult 把业务结果或错误写回客户端。
func writeResult(w http.ResponseWriter, v any, err error) {
	if err != nil {
		var be *Error
		if errors.As(err, &be) {
			writeJSON(w, be.Status, be)
			return
		}
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func decodeBody(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return badRequest("请求体解析失败: %v", err)
	}
	return nil
}

// metaOf 从请求头补充幂等键与操作人。
func (m Meta) withHeaders(r *http.Request) Meta {
	if m.IdempotencyKey == "" {
		m.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}
	if m.Actor == "" {
		m.Actor = r.Header.Get("X-Actor")
	}
	return m
}

func (h *handlers) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"状态": "服务已启动"})
}

// --- 请求体定义(内嵌载荷 + 通用元数据) ---

type metaFields struct {
	Actor          string    `json:"actor"`
	Reason         string    `json:"reason"`
	CausedBy       string    `json:"caused_by"`
	IdempotencyKey string    `json:"idempotency_key"`
	OccurredAt     time.Time `json:"occurred_at"`
}

func (f metaFields) meta(r *http.Request) Meta {
	return Meta{
		Actor:          f.Actor,
		Reason:         f.Reason,
		CausedBy:       f.CausedBy,
		IdempotencyKey: f.IdempotencyKey,
		OccurredAt:     f.OccurredAt,
	}.withHeaders(r)
}

func (h *handlers) createCatchBatch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CatchBatchRegistered
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.RegisterCatchBatch(req.CatchBatchRegistered, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) createLot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BondedLotCreated
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.CreateBondedLot(req.BondedLotCreated, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) listLots(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"lots": h.svc.ListLots()})
}

func (h *handlers) mergeLots(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ParentLotIDs []string `json:"parent_lot_ids"`
		ChildLotID   string   `json:"child_lot_id"`
		TankID       string   `json:"tank_id"`
		Custodian    string   `json:"custodian"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.MergeLots(req.ParentLotIDs, req.ChildLotID, req.TankID, req.Custodian, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) getLot(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.GetLot(r.PathValue("id"))
	writeResult(w, res, err)
}

func (h *handlers) lotHistory(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.LotHistory(r.PathValue("id"))
	writeResult(w, map[string]any{"events": res}, err)
}

func (h *handlers) splitLot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Children []SplitChild `json:"children"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.SplitLot(r.PathValue("id"), req.Children, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) allocate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrderID   string `json:"order_id"`
		VehicleID string `json:"vehicle_id"`
		FlightID  string `json:"flight_id"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Allocate(r.PathValue("id"), req.OrderID, req.VehicleID, req.FlightID, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) reallocate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrderID   string         `json:"order_id"`
		VehicleID string         `json:"vehicle_id"`
		FlightID  string         `json:"flight_id"`
		Trigger   ReallocTrigger `json:"trigger"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Reallocate(r.PathValue("id"), req.OrderID, req.VehicleID, req.FlightID, req.Trigger, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) seal(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SealID string `json:"seal_id"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Seal(r.PathValue("id"), req.SealID, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) unseal(w http.ResponseWriter, r *http.Request) {
	var req struct{ metaFields }
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Unseal(r.PathValue("id"), req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) release(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ReleaseID string `json:"release_id"`
		Authority string `json:"authority"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Release(r.PathValue("id"), req.ReleaseID, req.Authority, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) halt(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Step string `json:"step"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Halt(r.PathValue("id"), req.Step, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) resume(w http.ResponseWriter, r *http.Request) {
	var req struct{ metaFields }
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Resume(r.PathValue("id"), req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) depart(w http.ResponseWriter, r *http.Request) {
	var req struct{ metaFields }
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Depart(r.PathValue("id"), req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) deliver(w http.ResponseWriter, r *http.Request) {
	var req struct{ metaFields }
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Deliver(r.PathValue("id"), req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) createOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrderRegistered
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.RegisterOrder(req.OrderRegistered, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) listOrders(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"orders": h.svc.store.Orders()})
}

func (h *handlers) changeOrder(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Change string `json:"change"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.ChangeOrder(r.PathValue("id"), req.Change, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) createFlight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FlightRegistered
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.RegisterFlight(req.FlightRegistered, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) listFlights(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"flights": h.svc.store.Flights()})
}

func (h *handlers) delayFlight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		NewDepartureAt time.Time `json:"new_departure_at"`
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.DelayFlight(r.PathValue("id"), req.NewDepartureAt, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) telemetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TelemetryAppended
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.AppendTelemetry(req.TelemetryAppended, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) inspection(w http.ResponseWriter, r *http.Request) {
	var req struct {
		InspectionRecorded
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.RecordInspection(req.InspectionRecorded, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) handover(w http.ResponseWriter, r *http.Request) {
	var req struct {
		HandoverRecorded
		metaFields
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, nil, err)
		return
	}
	res, err := h.svc.Handover(req.HandoverRecorded, req.metaFields.meta(r))
	writeResult(w, res, err)
}

func (h *handlers) scanBox(w http.ResponseWriter, r *http.Request) {
	res, err := h.svc.ScanBox(r.PathValue("code"))
	writeResult(w, res, err)
}
