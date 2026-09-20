package transit

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

// NewHandler 构建全部 HTTP 端点。
func NewHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"状态": "服务已启动"})
	})

	// ---- 命令端点 ----
	mux.HandleFunc("POST /v1/lots", command(func(meta Meta, body []byte, _ *http.Request) (*CommandResult, error) {
		var p RegisterLotPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		return svc.RegisterLot(meta, p)
	}))
	mux.HandleFunc("POST /v1/observations", func(w http.ResponseWriter, r *http.Request) {
		body, ok := readBody(w, r)
		if !ok {
			return
		}
		// 支持单条对象或数组(离线补传批量上报)
		var raws []json.RawMessage
		trimmed := bytes.TrimSpace(body)
		if len(trimmed) > 0 && trimmed[0] == '[' {
			if err := json.Unmarshal(trimmed, &raws); err != nil {
				writeError(w, invalidf("请求体不是合法JSON数组: %v", err))
				return
			}
		} else {
			raws = []json.RawMessage{trimmed}
		}
		items := make([]ObservationInput, 0, len(raws))
		for _, raw := range raws {
			var p ObservationPayload
			if err := json.Unmarshal(raw, &p); err != nil {
				writeError(w, invalidf("观测条目不是合法JSON: %v", err))
				return
			}
			items = append(items, ObservationInput{Meta: parseMeta(raw), Payload: p})
		}
		writeJSON(w, http.StatusOK, map[string]any{"results": svc.RecordObservations(items)})
	})
	mux.HandleFunc("POST /v1/handovers", command(func(meta Meta, body []byte, _ *http.Request) (*CommandResult, error) {
		var p HandoverPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		return svc.Handover(meta, p)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/pick", command(func(meta Meta, _ []byte, r *http.Request) (*CommandResult, error) {
		return svc.Pick(meta, r.PathValue("lot"))
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/seal", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p SealPayload
		_ = json.Unmarshal(body, &p)
		return svc.Seal(meta, r.PathValue("lot"), p.SealCode)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/open", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p OpenPayload
		_ = json.Unmarshal(body, &p)
		return svc.Open(meta, r.PathValue("lot"), p.Reason)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/split", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p SplitPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.LotID = r.PathValue("lot")
		return svc.Split(meta, p)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/rebind", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p RebindPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.LotID = r.PathValue("lot")
		return svc.Rebind(meta, p)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/assign-flight", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p AssignFlightPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.LotID = r.PathValue("lot")
		return svc.AssignFlight(meta, p)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/load", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p LoadPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.LotID = r.PathValue("lot")
		return svc.Load(meta, p)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/deliver", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p DeliverPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.LotID = r.PathValue("lot")
		return svc.Deliver(meta, p)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/halt", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p HaltPayload
		_ = json.Unmarshal(body, &p)
		return svc.Halt(meta, r.PathValue("lot"), p.Reason)
	}))
	mux.HandleFunc("POST /v1/lots/{lot}/resume", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p ResumePayload
		_ = json.Unmarshal(body, &p)
		return svc.Resume(meta, r.PathValue("lot"), p.Reason)
	}))
	mux.HandleFunc("POST /v1/merges", command(func(meta Meta, body []byte, _ *http.Request) (*CommandResult, error) {
		var p MergePayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		return svc.Merge(meta, p)
	}))
	mux.HandleFunc("POST /v1/flights", command(func(meta Meta, body []byte, _ *http.Request) (*CommandResult, error) {
		var p RegisterFlightPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		return svc.RegisterFlight(meta, p)
	}))
	mux.HandleFunc("POST /v1/flights/{flight}/delay", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p DelayFlightPayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.FlightID = r.PathValue("flight")
		return svc.DelayFlight(meta, p)
	}))
	mux.HandleFunc("POST /v1/flights/{flight}/depart", command(func(meta Meta, _ []byte, r *http.Request) (*CommandResult, error) {
		return svc.DepartFlight(meta, r.PathValue("flight"))
	}))
	mux.HandleFunc("POST /v1/flights/{flight}/arrive", command(func(meta Meta, _ []byte, r *http.Request) (*CommandResult, error) {
		return svc.ArriveFlight(meta, r.PathValue("flight"))
	}))
	mux.HandleFunc("POST /v1/orders/{order}/change", command(func(meta Meta, body []byte, r *http.Request) (*CommandResult, error) {
		var p OrderChangePayload
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, invalidf("请求体不是合法JSON: %v", err)
		}
		p.OrderID = r.PathValue("order")
		return svc.ChangeOrder(meta, p)
	}))

	// ---- 查询端点 ----
	mux.HandleFunc("GET /v1/boxes/{code}", func(w http.ResponseWriter, r *http.Request) {
		res, err := svc.ScanBox(r.PathValue("code"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("GET /v1/boxes/{code}/lineage", func(w http.ResponseWriter, r *http.Request) {
		res, err := svc.Lineage(r.PathValue("code"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("GET /v1/lots/{lot}", func(w http.ResponseWriter, r *http.Request) {
		res, err := svc.LotDetail(r.PathValue("lot"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	mux.HandleFunc("GET /v1/lots/{lot}/explain", func(w http.ResponseWriter, r *http.Request) {
		entries, err := svc.Explain(r.PathValue("lot"), r.URL.Query().Get("focus"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"lot_id": r.PathValue("lot"), "entries": entries})
	})
	mux.HandleFunc("GET /v1/queue", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": svc.Queue()})
	})
	mux.HandleFunc("GET /v1/flights", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"flights": svc.Flights()})
	})

	return mux
}

// command 包装一个命令处理函数:读取请求体、解析来源信息、
// 调用业务方法并统一写出结果或错误。
func command(fn func(meta Meta, body []byte, r *http.Request) (*CommandResult, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, ok := readBody(w, r)
		if !ok {
			return
		}
		res, err := fn(parseMeta(body), body, r)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	}
}

// metaJSON 是命令请求体中的来源信息字段。
type metaJSON struct {
	EventID    string     `json:"event_id,omitempty"`
	Source     string     `json:"source,omitempty"`
	OccurredAt *time.Time `json:"occurred_at,omitempty"`
}

func parseMeta(body []byte) Meta {
	var m metaJSON
	_ = json.Unmarshal(body, &m)
	meta := Meta{EventID: m.EventID, Source: m.Source}
	if m.OccurredAt != nil {
		meta.OccurredAt = *m.OccurredAt
	}
	return meta
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeError(w, invalidf("读取请求体失败: %v", err))
		return nil, false
	}
	return body, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	if be, ok := err.(*Error); ok {
		code := http.StatusBadRequest
		switch be.Kind {
		case "not_found":
			code = http.StatusNotFound
		case "conflict":
			code = http.StatusConflict
		}
		writeJSON(w, code, map[string]string{"error": be.Msg, "kind": be.Kind})
		return
	}
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
}
