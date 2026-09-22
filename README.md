# 本地持久化工作流执行引擎（Go）

一个零外部依赖、基于本地文件的持久化工作流执行引擎。目标：**进程重启、重复调度、
活动失败、多实例并发与崩溃接管**之后，工作流仍能得到一致且可解释的执行结果。

- 所有状态变化以**不可变事件**形式持久化（JSON Lines + fsync）；
- 步骤执行由**租约**保护，乐观并发（全局 seq CAS）拒绝静默覆盖；
- 活动失败区分 **TRANSIENT / PERMANENT / TIMEOUT**，重试有预算、退避可复现；
- 冲突与非法状态迁移一律返回**可区分原因码**，不静默吞掉；
- 每个状态变化打印包含 `workflow / step / event / attempt / failure_class` 的日志。

## 目录结构

| 包 | 职责 |
| --- | --- |
| `internal/workflow` | 类型、原因码错误、事件重放投影（纯函数） |
| `internal/store` | `events.jsonl` 追加日志：flock + 内存锁 + 全局 seq CAS + fsync |
| `internal/activity` | 活动接口、分类失败（`*activity.Failure`）、注册表 |
| `internal/engine` | 状态机、租约领取/接管、超时执行、重试退避、调度循环 |
| `cmd/demo` | 可运行的“启动后崩溃 → 重启恢复”演示 |

## 工作流状态机

### 实例状态

```
                 START
PENDING ──► RUNNING ───────────────► COMPLETED
              │  ▲                       ▲
              │  │ 每步成功后推进         │
              │  └── STEP_SCHEDULED      │
              │                          │
              ├──────────────────────────┤
              ├────► FAILED              │  （步骤永久失败 / 重试耗尽 /
              └────► CANCELED            │    接管时重试预算耗尽）
```

- `RUNNING → COMPLETED`：最后一步提交 `STEP_SUCCEEDED` 时同批追加 `WORKFLOW_COMPLETED`。
- `RUNNING → FAILED`：活动返回 PERMANENT、重试预算耗尽、或接管的崩溃 attempt 已耗尽预算。
- `RUNNING → CANCELED`：显式 `Cancel`；对已终结实例再次取消返回 `INVALID_STATE_TRANSITION`。

### 步骤状态与事件

步骤：`PENDING → RUNNING → SUCCEEDED`；失败路径在 `PENDING / RUNNING` 之间按重试循环，
终态为 `FAILED`（实例同时 FAILED）。每个状态变化都是一条事件：

| 事件 | 含义 |
| --- | --- |
| `WORKFLOW_STARTED` | 启动；携带步骤名列表与工作流输入 |
| `STEP_SCHEDULED` | 步骤就绪；携带活动名与 `NextAttemptAt` |
| `STEP_LEASED` | 某 worker 领取租约；携带 attempt、holder、过期时间 |
| `STEP_SUCCEEDED` | **成功提交的去重点**，携带结果 JSON |
| `ATTEMPT_FAILED` | 一次尝试失败（TRANSIENT/PERMANENT），释放租约 |
| `STEP_TIMED_OUT` | 活动执行超时，或租约过期被接管（旧 attempt 归类 TIMEOUT） |
| `RETRY_SCHEDULED` | 已安排重试，携带确定的 `NextAttemptAt` |
| `STEP_FAILED` | 步骤终结失败（`PERMANENT_FAILURE` 或 `RETRY_EXHAUSTED`） |
| `WORKFLOW_COMPLETED` / `WORKFLOW_FAILED` / `WORKFLOW_CANCELED` | 实例终态 |
| `LEASE_TAKEN_OVER` | 接管时记录于日志（接管批次为 `STEP_TIMED_OUT + STEP_LEASED`） |

当前状态完全由事件重放得到（`workflow.Project`，纯函数）：
**事件顺序确定 ⇒ 投影结果确定、可复现**，崩溃恢复就是“重新打开日志再 Project 一遍”。

## 持久化与恢复语义

- 存储为数据目录下的 `events.jsonl`：每行一个 JSON 事件，`seq` 全局单调。
- 写入流程：`flock(LOCK_EX)`（跨进程）+ 进程内互斥锁 → 读尾部 seq →
  与调用方的 `expectLastSeq` 比对 → 追加 → `flush + fsync`（并 fsync 目录）。
  期望 seq 不匹配时返回 `CONCURRENT_MODIFICATION`，调用方重读重判，**绝不覆盖**。
- 工作流定义是“代码即定义”：进程重启后必须用**同名同形**的 `Definition` 重新
  `RegisterDefinition`（形状变化会被拒绝）；活动同样按名称重新注册。
- 重启后通过 `Get` 重放事件即可得到全部状态：已成功步骤带结果、失败步骤带历史、
  运行中步骤带租约持有者与过期时间。
- 租约过期的 `RUNNING` 步骤被视为前 worker 崩溃：旧 attempt 以 `TIMEOUT` 计入失败
  历史；仍有预算则立即以新 attempt 重新领取，预算耗尽则工作流终结。

## 重试、超时与终止策略

`workflow.RetryPolicy{MaxAttempts, Timeout, InitialInterval, MaxInterval, Multiplier}`：

- `MaxAttempts` 含首次执行（默认 1）；`Timeout<=0` 表示不限制。
- 失败分类由活动返回 `*activity.Failure{Class, Code, Message}` 决定；
  普通 `error` 视为 TRANSIENT，`context` 超时由引擎包装为 TIMEOUT。
- **PERMANENT：一次失败立即终止**，永不重试。
- **TRANSIENT / TIMEOUT**：`attempt < MaxAttempts` 时追加 `RETRY_SCHEDULED`，
  退避 `InitialInterval * Multiplier^(attempt-1)`（上限 `MaxInterval`）；
  纯函数计算，相同输入与策略 ⇒ 相同的重试时刻与最终结论。
- 单次活动执行通过 `context.WithTimeout` + goroutine/select 兜底超时
  （活动即使忽略 ctx 也会被判定超时）。
- 每个步骤的全部尝试都保存在 `StepStatus.History`，终结原因在 `Failure`
  （`PERMANENT_FAILURE` / `RETRY_EXHAUSTED`），可用 `Get` 查询。

## 并发约束

- 同一步骤任一时刻只有一个有效租约持有者：他人领取返回 `LEASE_HELD_BY_OTHER`，
  自己重复领取返回 `ALREADY_LEASED`，未过期接管返回 `LEASE_NOT_EXPIRED`。
- 提交 `STEP_SUCCEEDED` 前进行持有者与租约有效期校验，并以全局 seq CAS 为最终防线：
  若执行期间租约过期被他人接管，提交失败（`CONCURRENT_MODIFICATION`），
  **成功结果只可能提交一次**；重复执行已成功步骤返回 `ALREADY_SUCCEEDED`。
- 因此同一逻辑步骤的业务副作用不会被引擎重复确认。注意活动函数在极端情况下
  （已执行但提交前崩溃/被接管）可能被再次调用，**活动的外部副作用必须幂等**。
- 多个引擎（同进程或跨进程）指向同一数据目录即可构成“竞争 worker”模型；
  后台用 `eng.Run(ctx, interval)` 调度，也可手动 `AcquireStep` / `ExecuteLeased`。

## 可区分的原因码

`NOT_FOUND, ALREADY_STARTED, INVALID_STATE_TRANSITION, STEP_NOT_READY,
LEASE_HELD_BY_OTHER, LEASE_NOT_EXPIRED, LEASE_EXPIRED, CONCURRENT_MODIFICATION,
RETRY_EXHAUSTED, PERMANENT_FAILURE, RETRY_NOT_DUE, ALREADY_SUCCEEDED,
ALREADY_LEASED, WORKFLOW_FINISHED, DEFINITION_NOT_FOUND`。
用 `workflow.GetErrorCode(err)` 提取。

## 本地验证

```bash
# 全部单元测试（含 -race 竞态检测）
go test ./... -count=1 -race -v

# 端到端演示：崩溃 → 重启恢复 → 瞬时失败重试 → 完成
rm -rf .demo-data
go run ./cmd/demo start    # 完成第一步后“崩溃”
go run ./cmd/demo resume   # 新进程重放事件并跑完全程

# 直接查看人类可读的事件日志
cat .demo-data/events.jsonl
```

测试覆盖场景（`internal/engine/*_test.go`、`internal/store/store_test.go`、
`internal/workflow/projection_test.go`）：

1. **重启恢复**：完成一步后关闭重开存储，新引擎重放状态并继续，副作用不重复；
2. **重复调度**：重复 Start / Acquire / Execute 均被幂等拒绝，原因码可区分；
3. **活动超时**：活动超过 `Timeout` 归类 TIMEOUT，重试耗尽后终结；
4. **永久失败**：PERMANENT 错误不重试，即使预算充足、时钟推进也不再调度；
5. **瞬时重试与退避**：按指数退避重试成功，历史与 `NextAttemptAt` 可查询；
6. **并发接管**：租约互斥、崩溃过期后接管、4 worker 并发 Tick 不重复提交、
   8 路并发 Start 仅一个成功。

测试日志（`t.Log` + 引擎 logger）会打印工作流标识、步骤、事件/状态变化、
attempt 与失败分类，例如：

```
workflow=retry-wf/inst-retry step=s1 event=ATTEMPT_FAILED attempt=1 failure_class=TRANSIENT code=TEMP_UNAVAILABLE
workflow=timeout-wf/inst-timeout step=s1 event=STEP_TIMED_OUT attempt=2 failure_class=TIMEOUT
```
