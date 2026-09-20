# 鲜活水产保税联运服务

记录鲜活水产(如俄罗斯帝王蟹)在暂养、冷藏车转运、航空运输和目的地分拨之间的批次关系。箱码、重量、环境观测、封识与交接事件都保留原始来源,支撑口岸"随到随装"的鲜活窗口作业。

## 架构:事件溯源 + 只增不改日志

所有状态变化先落盘为事件(`data/events.jsonl`,逐行 JSON,写后 fsync),内存中的批次/箱码视图只是事件重放的结果:

- **重启不丢在途状态**:启动时按序重放日志即恢复全部批次、箱码索引与幂等键。
- **谱系可溯**:捕捞批次 → 暂养池批次 → 拆分/合并 → 订单/车辆/航班/目的地,全部以事件链保存(`GET /api/lots/{id}/history`)。
- **可解释**:每条事件携带 `actor`/`reason`/`caused_by`,`GET /api/lots/{id}` 的 `explanation` 字段直接回答"为何一次放行、为何改配、在哪一步被中止"。
- **幂等**:每条命令可带幂等键(请求体 `idempotency_key` 或 `Idempotency-Key` 头)。遥测按 `设备+序号`、检查按 `机构+回调号`、放行按 `放行号`、交接按 `交接号` 自动派生幂等键——离线补传与多部门重复回调命中已存事件,返回 `duplicate: true`,不产生新状态,**不会二次放行**;已放行批次上的不同放行号返回 409。

## 状态机与业务规则

```
HOLDING → ALLOCATED → INSPECTING → SEALED → RELEASED → IN_TRANSIT → DELIVERED
              ↑            ↓(开封)                         ↘ HALTED(任何环节中止,可 resume)
              └────────────┘
拆分/合并仅在封运前;CLOSED 为拆分/合并后的父批次终态
```

- **守恒**:交接时箱数与活体重量必须与台账一致,否则记录 `handover_discrepancy` 并在交接环节中止;死亡抽检先调整台账再守恒校验;拆分须不重不漏,合并求和;入池不得超过捕捞批次余量。
- **改配**:仅 `HOLDING/ALLOCATED/INSPECTING`(尚未封运)可改配。航班延误、订单变化会把受影响批次标记为 `reallocation_suggested`;已封运批次须先 `unseal`——开封即回到联合核验环节(该结论作废),重新核验、封识后才能放行。
- **优先级**:温度/溶氧越界或发现死亡个体自动升为 `high`;扫码时剩余时限不足 90 分钟升 `high`、不足 45 分钟升 `critical`。

## 运行

```bash
go run ./cmd/server          # 监听 :8080,事件日志 data/events.jsonl
# TRANSIT_ADDR=:9090 TRANSIT_DATA=/var/lib/transit/events.jsonl 可覆盖
go test ./...                # 全部检查
```

## API 一览

| 方法与路径 | 说明 |
| --- | --- |
| `GET /api/boxes/{code}` | **扫码视图**:当前责任方、剩余时限、监管结论、允许目的地、优先级 |
| `POST /api/catch-batches` | 登记捕捞批次(谱系根,含可存活时长) |
| `POST /api/lots` | 建立保税暂养批次(箱码+单箱重量,入暂养池) |
| `POST /api/lots/{id}/split` · `POST /api/lots/merge` | 拆分 / 合并(守恒校验,保留谱系) |
| `POST /api/orders` · `POST /api/orders/{id}/changes` | 订单登记 / 变化(触发待改配标记) |
| `POST /api/lots/{id}/allocate` · `/reallocate` | 分配 / 改配(仅未封运) |
| `POST /api/telemetry` | 追加温度/溶氧/死亡抽检(按设备+序号幂等,支持离线补传 `occurred_at`) |
| `POST /api/inspections` | 安检/海关/联合核验结论(按机构+回调号幂等;fail 即在相应环节中止) |
| `POST /api/lots/{id}/seal` · `/unseal` | 封识(需三检通过)/ 开封(回到联合核验环节) |
| `POST /api/lots/{id}/release` | 放行(按放行号幂等,杜绝二次放行) |
| `POST /api/handovers` | 交接(箱数/活体重量守恒校验,责任方转移) |
| `POST /api/flights` · `POST /api/flights/{id}/delay` | 航班登记 / 延误(标记受影响批次待改配) |
| `POST /api/lots/{id}/halt` · `/resume` · `/depart` · `/deliver` | 中止 / 恢复 / 起运 / 交付 |
| `GET /api/lots/{id}` | 批次详情 + `explanation`(放行依据、改配原因、中止环节) |
| `GET /api/lots/{id}/history` | 批次全部相关事件(谱系链) |
| `GET /api/lots` · `GET /api/orders` · `GET /api/flights` | 列表查询 |

## 示例:948 箱帝王蟹赴港

```bash
# 捕捞批次 → 暂养批次(948 箱)→ 订单/航班 → 分配
curl -s localhost:8080/api/catch-batches -d '{"catch_batch_id":"RUS-KC-0919","species":"俄罗斯帝王蟹",
  "origin":"Vladivostok","harvested_at":"2026-09-19T16:00:00Z","survival_hours":30,
  "total_boxes":948,"total_weight_kg":11186.4}'
curl -s localhost:8080/api/lots -d '{"lot_id":"LOT-A","catch_batch_id":"RUS-KC-0919","tank_id":"tank-03",
  "custodian":"珲春保税暂养库","boxes":[{"box_code":"KC-000001","weight_kg":11.8}, ...]}'
curl -s localhost:8080/api/orders  -d '{"order_id":"ORD-HK-01","destination":"Hong Kong", ...}'
curl -s localhost:8080/api/lots/LOT-A/allocate -d '{"order_id":"ORD-HK-01","vehicle_id":"truck-01","flight_id":"cargo-882"}'

# 遥测(离线补传)→ 守恒交接 → 三检 → 封识 → 放行 → 起运
curl -s localhost:8080/api/telemetry -d '{"device_id":"tank-03","device_seq":1001,"lot_id":"LOT-A",
  "kind":"temperature","value":2.3,"occurred_at":"2026-09-20T04:30:00Z"}'
curl -s localhost:8080/api/handovers -d '{"handover_id":"HO-1","lot_id":"LOT-A",
  "from_party":"珲春保税暂养库","to_party":"冷藏车 truck-01","box_count":948,"live_weight_kg":11186.4}'
curl -s localhost:8080/api/inspections -d '{"lot_id":"LOT-A","kind":"joint","result":"pass",
  "authority":"口岸联检","callback_id":"cb-joint-1"}'   # security/customs 同理
curl -s localhost:8080/api/lots/LOT-A/seal    -d '{"seal_id":"SEAL-1"}'
curl -s localhost:8080/api/lots/LOT-A/release -d '{"release_id":"REL-1","authority":"珲春海关"}'
curl -s localhost:8080/api/boxes/KC-000501    # → 责任方/剩余时限/已放行/Hong Kong
```

`fixtures/shipment.json` 展示保税暂养批次、箱码和运输计划的基础字段。
