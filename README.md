# 鲜活水产保税联运服务

面向"珲春保税暂养 → 冷藏车 → 延吉机场 → 货运包机 → 香港"链路的鲜活水产联运台账。
扫描任一箱码即可得到**当前责任方、剩余时限、监管结论、允许目的地**;后台保留
捕捞批次 → 暂养池 → 保税批次 → 箱码 → 订单/车辆/航班 → 目的地 的完整拆分合并谱系。

## 架构

事件溯源,仅标准库,单二进制:

```
cmd/server            服务入口(加载日志 → 回放 → HTTP)
internal/transit
  types.go            领域模型:批次/箱/航班/订单、环节状态机、优先级
  events.go           事件类型与负载(外部事件 + 规则引擎派生事件)
  store.go            JSONL 只追加事件日志,写入即 fsync,幂等键去重
  ledger.go           内存投影:Apply 应用事件,扫码/谱系/解释/队列查询
  service.go          命令校验 + 规则引擎(守恒、一次放行、开封回检、优先级)
  httpapi.go          REST 端点
```

- **持久化与重启恢复**:所有状态变化先追加到 `data/events.jsonl`(可用 `-data` 指定),
  再应用到内存投影;重启时从头回放,在途状态完整恢复。
- **幂等**:每条事件携带 `source + event_id` 幂等键。离线补传、断网重发、
  多部门重复回调携带相同键时只应用一次;海关放行再按**决定号**幂等,
  从机制上杜绝二次放行。
- **派生事件**:联合核验通过、一次放行、指标越界、优先级提升、守恒差异等
  由规则引擎产生,同样写入日志,回放后历史完全一致。

## 关键不变量

| 需求 | 机制 |
| --- | --- |
| 不二次放行 | 事件幂等键 + 放行决定号幂等;重复回调记 `release.duplicate`,冲突决定记 `release.conflict`,均不改变已生效放行 |
| 交接守恒 | 每次交接/交付申报箱数与活重,与台账比对(容差 0.5kg 或 0.5%);失败即中止待核查,纠正申报后自动恢复 |
| 仅未封运可改配 | 封运前批次可拆分/合并/改绑订单、目的地、航班;封运后一律拒绝 |
| 开封回检 | 已封运批次开封(含封识破损自动开封)→ 回到联合核验环节,原放行作废,须重新核验、封运、放行 |
| 生存阈值优先 | 温度/溶氧越界、死亡率>5%、剩余时限<2h/45min 自动提升处置优先级并写入决策日志 |
| 可解释 | 每批次保留决策日志:为何一次放行(依据的三项结论+决定号)、为何改配(触发原因)、在哪一步中止 |

## 运行

```bash
go run ./cmd/server -addr :8080 -data data/events.jsonl
go test ./...
```

`fixtures/shipment.json` 展示保税暂养批次、箱码和运输计划的基础字段。

## 主要端点

```
POST /v1/lots                         注册保税批次(箱码、捕捞批次、暂养池、订单、存活预算、环境阈值)
POST /v1/observations                 追加观测(温度/溶氧/死亡抽检/封识/安检/海关),支持批量离线补传
POST /v1/handovers                    交接(申报箱数+活重,守恒校验)
POST /v1/lots/{lot}/pick              出库(启动存活时钟)
POST /v1/lots/{lot}/seal              封运(需联合核验通过)
POST /v1/lots/{lot}/open              开封 → 回到联合核验环节
POST /v1/lots/{lot}/split|rebind      拆分 / 改配(仅未封运)
POST /v1/lots/{lot}/assign-flight     指定航班(仅未封运)
POST /v1/lots/{lot}/load|deliver      装机 / 交付(交付同样守恒校验)
POST /v1/lots/{lot}/halt|resume       中止 / 恢复
POST /v1/merges                       合并批次
POST /v1/flights[/{flight}/delay|depart|arrive]   航班登记/延误/起飞/到港
POST /v1/orders/{order}/change        订单变化(未封运批次跟随改配)

GET  /v1/boxes/{code}                 扫码:责任方/剩余时限/监管结论/允许目的地
GET  /v1/boxes/{code}/lineage         谱系:捕捞批次→暂养池→批次链→订单/车辆/航班→目的地
GET  /v1/lots/{lot}                   批次详情
GET  /v1/lots/{lot}/explain[?focus=release|reassign|halt]   决策解释
GET  /v1/queue                        处置队列(按优先级与剩余时限排序)
GET  /v1/flights                      航班列表
```

命令请求体可携带 `event_id`、`source`、`occurred_at` 作为幂等键与发生时间;
重复提交返回 `{"duplicate": true}` 且不产生任何状态变化。

## 示例

```bash
# 注册批次(3 箱帝王蟹,存活预算 8 小时,温度 2~8℃,溶氧 ≥6mg/L)
curl -X POST localhost:8080/v1/lots -d '{
  "event_id":"reg-001","source":"port-system",
  "lot_id":"KC-LOT-948","catch_batch":"RU-KC-20260918-A","tank":"tank-03",
  "boxes":[{"code":"KC-000001","weight_kg":11.8}],
  "order_id":"ORD-HK-948","destination":"香港","flight_id":"CA-CHARTER-01",
  "custodian":"珲春保税暂养场","survival_budget_hours":8,
  "temp_min":2,"temp_max":8,"do_min":6}'

# 海关合格结论(决定号 HG-2026-0901)→ 已封运批次自动一次放行
curl -X POST localhost:8080/v1/observations -d '{
  "event_id":"cus-001","source":"customs-sys","lot_id":"KC-LOT-948",
  "kind":"customs","conclusion":"pass","decision_id":"HG-2026-0901"}'

# 扫码
curl localhost:8080/v1/boxes/KC-000001
```
