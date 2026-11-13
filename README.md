# 03-changeflow（变流）

Changeflow — CDC / 变更流落库：事件入账 → 序/缺口裁决 → 幂等应用到 sink → 游标提交与对账。

**不是** Feed 多源归并（Feedstitch），**不是** 作业依赖编排（Batchgate）。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5300 |
| API | http://localhost:10300 |

本地开发（可选）：

```bash
# backend
cd backend && pip install -r requirements.txt
set DATA_DIR=..\data
uvicorn app.main:app --reload --port 10300

# frontend
cd frontend && npm install && npm run dev
```

## 主链

配置变更流与夹具 → 触发 Apply → 按序应用 / 缺口隔离 → Sink + 游标 + 运行报告对账

## 种子说明

- **Orders CDC（clean）**：JSONL，seq 1→5 连续 upsert/delete，首跑应全部应用、游标推进到 5。
- **Inventory CDC（clean）**：JSONL，seq 1→3 连续 upsert，独立实体键，用于第二路流演示。
- **Orders Archive CDC（history）**：启动时自动灌入的历史账本（event_ts 2025-09），用于过期清理演示。
- 自动造数还包含一条「超窗 + 隔离区」账本行（ORD-0790 / seq 90），用于验证清理排除游标证据。

打开「应用台」选流跑一次，再看账本 / 隔离区 / Sink / 游标页计数是否与本次报告一致。

## 账本过期清理与保全冻结

入口：前端「账本清理」页（`/cleanup`），API 在 `/api/ledger/cleanup/*` 与 `/api/legal-holds`。

- **保留窗**：`retention_days` 存于 settings（默认 30），可改可保存。截止点 = 现在 − 保留天数；
  只有 `event_ts` 早于截止点的账本行才是「超窗」。
- **保全冻结**：按实体键（可限定流）或时间范围（`[start_ts, end_ts]`，可限定流）登记，登记即生效；
  可解除。命中启用中冻结的账本行即使超窗也**必须保留**，并在批次报告中单列为「因冻结保留」。
  数据库层另有 DELETE 触发器兜底：启用中的冻结行被直接删除会 `RAISE(ABORT)`。
- **两段式清理**：① 预览（扫描超窗生成清单，不删）→ ② 执行（只按预览可删清单删除）。
  预览时已冻结的行不会在后续执行中变成可删；冻结解除后需重新预览。
  执行前会复核冻结/隔离，比对冻结**只收缩不扩张**。
- **冲突与排除**：冻结优先于删除；仍在隔离区（`quarantine` 中同 `stream_id, seq`）的账本行被视为
  游标停点证据，一律排除并注明原因。执行前新增的冻结/隔离同样保留并计入对账。

### 对账口径

批次记录预览数：`scanned_count`、`deletable_count`、`frozen_retained_count`、`cursor_protected_count`、
`ledger_count_at_preview`；执行后记录 `ledger_count_before / ledger_count_after`、`deleted_count`、
`newly_frozen_count`、`newly_protected_count`、`missing_at_execute_count`、`active_hold_count`。
执行报告的 `checks` 逐项校验：

- `scanned = within_window + deletable + frozen_retained + cursor_protected`
- `ledger_before − deleted = ledger_after`
- `preview_deletable = deleted + newly_frozen + newly_protected + missing`
- `frozen_retained = preview_frozen − frozen_missing + newly_frozen`
- `cursor_protected = preview_protected − protected_missing + newly_protected`
- 预览清单总条数 = 各执行态计数之和；删除只能来自预览可删清单

全部通过时 `report.reconciled = true`。冻结名单与清理批次持久化在 SQLite，刷新/重启后仍在。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现，见题单）

当前仓库已实现「连续序幂等应用」主链。缺口等待/跳过、乱序缓冲、事务整批、双 sink 部分失败、毒丸选择性跳过等均在 `backend/app/modules/*` 留空包，按题单逐题换主链路实现。
