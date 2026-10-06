# 本地持久化工作流执行引擎（Go）

一个零外部依赖、基于本地文件系统的持久化工作流执行引擎。它保证：

- 工作流启动、步骤推进、结果查询，每次状态变化都先落盘再返回；
- 同一逻辑步骤的重复调度/重试/崩溃接管不会重复产生业务副作用；
- 冲突与非法状态迁移一律被**拒绝并返回可区分原因**，不静默覆盖；
- 活动失败按「瞬时 / 超时 / 永久」分类：瞬时可重试、永久立即终止、重试预算耗尽即失败；
- 失败历史、最终分类、重试依据全部持久化、可查询、同输入结论可复现；
- 多执行者并发推进时，靠「租约 + fencing 令牌 + 乐观版本」保证步骤不并发执行、状态不回退、结果不丢失。

包路径：`github.com/xingjianluo/durable-workflow-runtime-go/workflow`

---

## 1. 状态机

### 1.1 工作流状态 `WorkflowStatus`

```
                 Start
                   │
                   ▼
              ┌─────────┐   所有步骤成功    ┌────────────┐
              │ running │ ───────────────▶ │ completed  │（终态）
              └─────────┘                   └────────────┘
                 ▲  │  永久错误 / 重试预算耗尽 / Terminate
                 │  ▼
   Resume(+n) │ ┌─────────┐
              └─│ failed  │（终态，可显式续跑）
                └─────────┘
```

- `running`：至少还有步骤未结束（步骤可能在等待退避）。
- `completed`：全部步骤成功；`Result` 为末步结果。不可续跑。
- `failed`：某步骤永久失败或重试预算耗尽；`Failure` 记录最终分类与原因。
  **可由调用方显式 `Resume` 续跑**（见第 3.4 节），续跑后回到 `running`。
- 终态不可逆（除 `Resume` 这条显式通道）：重复 `Tick`、重复 `Advance`、
  重复 `Terminate` 都不会复活或重复提交。

非法迁移（如对终态实例获取租约、退避未到期推进、无租约推进）返回 `*ConflictError`，
用其 `Kind` 字段（`errors.As`）区分原因：

| Kind | 触发场景 |
| --- | --- |
| `workflow_already_exists` | 相同 ID 重复 `Start` |
| `invalid_transition` | 工作流不存在等非法操作 |
| `not_terminal` | 对非终态实例调用 `Result` |
| `step_already_completed` | 已完成步骤被要求重跑（由游标保护，直接跳过） |
| `no_runnable_step` | 步骤处于退避等待（`NextAttemptAt` 未到） |
| `workflow_terminal` | 对终态实例获取租约/推进 |
| `lease_not_held` | 未持有租约却推进 |
| `lease_held_by_another_worker` | 他人有效租约未过期 |
| `lease_expired` | 租约已过期，需重新领取/接管 |
| `lease_version_stale` | token/fence 不匹配（租约已被接管） |
| `version_conflict` | 乐观版本不匹配（并发提交落败） |
| `workflow_already_completed` | 续跑已成功完成的实例 |
| `workflow_not_failed` | 续跑未失败的实例（如仍在运行） |
| `invalid_extra_attempts` | 续跑未给出正的追加尝试次数 |

### 1.2 步骤状态 `StepStatus`

```
 pending ──Advance──▶ running ──成功──────────────▶ succeeded（终态）
                        │  │
                        │  └─瞬时/超时失败且仍有预算─▶ waiting ──(退避到期)─▶ running
                        │
                        └─永久错误 / 预算耗尽────────▶ failed（终态）
```

- 线性工作流：上一步的输出（`StepState.Result`）作为下一步输入；`Cursor` 指向下一个待处理步骤。
- 游标只在「步骤成功提交」后前移，因此已成功步骤在任何重试/接管后都不会重跑。
- `StepState.Attempt` 为已开始的尝试次数（首次为 1）；`Failures` 追加保存每次失败。

---

## 2. 持久化与崩溃恢复语义

### 2.1 存储布局（`FileStore`）

每个工作流实例对应目录下一个原子文件 `<dir>/<id>.json`，内容为：

```json
{
  "state":  { /* 最新快照：状态、游标、租约、fence、版本…… */ },
  "events": [ { "seq": 1, "type": "workflow_started", ... } ]
}
```

- **原子写**：写入同目录临时文件 → `fsync` → `rename` 覆盖 → `fsync` 目录。
  任何时刻崩溃都只会看到「旧完整文件」或「新完整文件」，不会出现撕裂。
- **每次状态变化都持久化**：`step_started / step_succeeded / step_failed /
  step_retry_wait / lease_* / workflow_*` 全部追加进 `events`，序号严格递增，
  可通过 `Engine.History` 查询，是执行结果「可解释」的依据。
- **乐观版本**：快照有单调递增 `Version`。所有提交都必须带期望版本，
  不匹配返回 `version_conflict`，杜绝丢失更新（lost update）。

### 2.2 恢复规则（重新 `NewEngine` 即“重启”）

引擎自身无易失状态，全部状态在 Store 里；用同一目录重新打开就是恢复：

1. 若崩溃发生在**步骤成功提交之后**：游标已前移，恢复后从下一步继续，上一步绝不重跑。
2. 若崩溃发生在**活动执行期间**：磁盘上是 `running + Attempt=n + 旧租约`。
   新执行者必须等旧租约过期后才能接管（产生 `lease_taken_over`，fence 递增），
   随后以**同一个幂等令牌**重新执行该步骤（`Attempt` 变为 n+1）。
   被遗弃的旧协程晚到的结果会因 fence/版本不匹配而被**丢弃**，不会覆盖新状态。
3. 若崩溃发生在**退避等待期间**：`NextAttemptAt` 已持久化，恢复后在到期前
   不会提前重试，到期后正常重试。

### 2.3 幂等令牌（业务副作用恰好一次）

同一逻辑步骤的所有尝试共享稳定令牌：

```
IdempotencyToken = "<workflowID>#step<index>"
```

活动通过 `ActivityInput.IdempotencyToken` 拿到它。活动侧应以该令牌做去重
（先查后写 / 唯一约束：令牌已存在则直接返回首次结果），从而在“引擎至少执行一次活动”
的基础上实现“业务副作用恰好一次”。引擎保证的是：不重复推进逻辑步骤、不覆盖、不回退。

---

## 3. 活动失败、重试、超时与终止

### 3.1 失败分类

| 分类 | 构造方式 | 是否重试 |
| --- | --- | --- |
| `transient`（瞬时） | `workflow.Transient(err)` / `Transientf(...)`；未分类错误默认按瞬时 | 是（受预算限制） |
| `timeout`（超时） | 单次尝试超过 `AttemptTimeout`，或活动返回 `context.DeadlineExceeded` | 是（按瞬时处理） |
| `permanent`（永久） | `workflow.Permanent(err)` / `Permanentf(...)`；活动未注册也按永久 | 否，立即终止 |

- 未显式分类的普通错误默认按瞬时处理，但因为有 `MaxAttempts` 上限，**不会无限重试**。
- 每次失败都追加一条 `Failure{Attempt, Class, Message, At, DuringLease, TimedOut}`，
  可用 `Engine.Failures(id, stepIndex)` 查步骤历史，`stepIndex<0` 查工作流终态失败。
- 最终分类与原因写入 `StepState.FinalClass/FinalReason` 与 `WorkflowState.Failure`，
  分类只依赖输入与活动逻辑，因此同一输入结论可复现。

### 3.2 退避与预算（`RetryPolicy`）

- `MaxAttempts`：最大尝试次数（含首次）；默认 3，最小归一化为 1。
- `InitialBackoff` / `MaxBackoff` / `Multiplier`：指数退避，
  第 n 次失败后等待 `min(InitialBackoff * Multiplier^(n-1), MaxBackoff)`。
- `AttemptTimeout`：单次尝试超时；超时即取消活动 context 并按瞬时错误进入退避。
- 预算耗尽后工作流进入 `failed`，原因明确记录为
  `retry budget exhausted after N attempt(s)`。

### 3.3 终止

`Engine.Terminate(id, reason)` 以永久失败终止实例：当前步骤记 `failed`、
工作流记 `failed`，并记录原因；重复终止幂等（不产生新提交）。他人持有活跃租约时拒绝终止。

### 3.4 失败后续跑（Resume）

`Engine.Resume(id, extraAttempts)` 让已失败的实例**从卡住的步骤接着跑**：

- **适用条件**：仅 `failed` 实例（重试预算耗尽、超时按瞬时耗尽、人工 `Terminate`）。
  `completed` 实例拒绝（`workflow_already_completed`），`running` 实例拒绝
  （`workflow_not_failed`），两种原因可区分。
- **追加预算语义**：`extraAttempts` 必须由调用方显式给出且为正数，
  否则拒绝（`invalid_extra_attempts`）——不默认放开。追加的预算累计到卡住步骤的
  `StepState.ExtraAttempts`，有效预算 = `RetryPolicy.MaxAttempts + ExtraAttempts`。
- **身份与历史不变**：不新建实例、ID 不变、游标不动、尝试编号从既有 `Attempt`
  继续递增；已成功步骤绝不重跑；事件、失败记录全部保留，并追加一条
  `workflow_resumed` 事件与 `WorkflowState.Resumes` 记录（第几次续跑、给哪一步、
  追加多少、何时、由谁发起）。
- **并发安全**：并发续跑只有一个能提交（乐观版本），其余收到 `version_conflict`；
  串行的重复续跑因实例已不在 `failed` 而被拒绝。续跑只把实例置回 `running`，
  之后的推进仍走租约 + fencing 互斥，同一步骤任意时刻只有一个执行者。

### 3.5 崩溃中断尝试的记账规则

尝试在 `step_started` 落盘时即视为**已用掉一次预算**（`Attempt` 已递增并持久化）。
若进程在活动执行期间崩溃，留下 `running + Attempt=n` 的在途尝试：

- 接管者（租约过期后）或续跑方看到 `Attempt=n` 时，n 已计入消耗，
  下一次尝试编号为 n+1，**不会漏记也不会重记**；
- 若 n 已等于有效预算，接管/推进时**不再发起新的活动调用**，直接把该在途尝试
  补记为 `interrupted` 失败并落账「预算耗尽」（原因中含
  `last attempt interrupted before completion`）——不会凭空多送一次活动调用；
- 被中断的尝试可由 `Resume` 追加预算后继续，编号从 n+1 开始。

**「被中断（interrupted）」与「业务失败（failed）」的判定口径**（运维必须能分开看）：

| 复盘去向 | 判定条件（全部只依据已落盘的事件与快照） |
| --- | --- |
| `succeeded` | 该次 `step_started` 已被同编号 `step_succeeded` 闭环 |
| `failed` | 该次 `step_started` 已被同编号 `step_failed` 闭环，且事件**没有** `interrupted=true` —— 活动真实返回了（瞬时/超时/永久）错误 |
| `interrupted` | 满足二者之一：① 该次尝试只有 `step_started`、没有任何闭环事件（崩溃/接管打断，没跑出结果）；② 被同编号 `step_failed(interrupted=true)` 闭环，即接管时发现「最后一次预算的在途尝试已随崩溃消失」而补记的预算耗尽 |
| `in_progress` | 未闭环，**并且**它就是当前快照游标步骤中编号最新（`Attempt` 相等）的那次尝试、步骤为 `running`、快照中的租约仍未过期。任一条件不满足都不是 in_progress |

由此保证：

- **同一步骤同一时刻至多一条 `in_progress`**：接管后新尝试编号更大，旧尝试编号
  小于当前 `Attempt`，必然落为 `interrupted`，不会出现新旧两条都「进行中」；
- **租约过期但尚无人接管**时也不显示 in_progress——此时已没有执行者在跑，
  旧尝试按 `interrupted` 显示；而崩溃后立即重启、磁盘上旧租约仍在有效期内时，
  复盘如实显示 in_progress（这正是崩溃瞬间的落盘状态，重启不改写）；
- 终态实例里不存在 in_progress；被中断的尝试没有 `ended_at`（活动从未产出结果，
  不存在真实结束时刻）。
- 除审计去向之外，步骤失败历史与工作流终态失败中的对应记录带有显式
  `Failure.Interrupted=true` 标志，程序里不必靠消息字符串区分。

### 3.6 审计轨迹（Audit）

`Engine.Audit(id)` 从**持久化的事件与快照**重建完整复盘报告（不依赖进程内存，
重启后查询结果逐字段一致）：

- `Attempts`：每一次尝试的步骤、编号、开始/结束时间与时长、去向分类
  （`succeeded` / `failed` / `interrupted` / `in_progress`）、失败分类、
  租约标识，以及**该次尝试开始时按当时预算计算的剩余可用次数**。
  剩余可用次数口径：`RemainingBudget = 当时的有效预算 − 本次尝试编号`，
  即「本次尝试已经入账之后、还允许再发起多少次尝试」；因此第一次尝试后为
  `MaxAttempts−1`，最后一次预算的尝试为 0，被中断的尝试同样消耗预算、
  剩余次数照算（不会因为没跑出结果就把预算退回去）。有效预算取该次
  `step_started` 事件里持久化的 `max_attempts`（= 定义基准 + 当时已追加预算）；
  旧事件缺少该明细时，按「定义基准 + 历次 `workflow_resumed` 追加」的时间线推算；
  定义未注册且无法推算时为 -1（未知）。
- `Resumes`：实例被续跑过几次、每次给哪一步追加了多少预算、何时由谁发起。

**可相互印证、不自相矛盾**：每条 `interrupted` 都对应一次已落盘的
`step_started`（预算确实用掉），且没有任何活动结果事件（确实没跑出结果）；
预算随 `workflow_resumed` 追加、随 `step_started` 单调消耗；
最终状态（`completed`/`failed`）与轨迹末条记录及快照 `Failure` 一一对应。

把 `Attempts` 与 `Resumes` 串起来即可解释「当时为什么继续、为什么终止」：
预算随续跑事件追加、随 `step_started` 消耗，最终状态与轨迹末条记录一致。

---

## 4. 并发约束（多执行者 / 接管）

- **租约**：推进前必须 `AcquireLease`。租约带 `Holder`、随机 `Token`、
  单调递增 `Fence` 与过期时间。
- **互斥**：他人有效租约未过期时，领取/推进/续租/释放均被拒绝
  （`lease_held_by_another_worker`）；同一执行者重复领取幂等返回原租约。
- **接管**：租约过期后其他执行者可接管，`Fence+1` 并记录 `lease_taken_over`。
  旧持有者手中的 token/fence 立即失效，其任何提交都被拒绝——
  因此租约过期或重复领取**不会**导致步骤并发执行或状态回退。
- **fencing + 乐观版本双重防覆盖**：活动执行结束、提交结果前会重新加载状态，
  同时校验持有者身份、token、fence、过期时间与版本；任一不符即拒绝提交。
- **跨进程锁**：`FileStore` 在“读取版本 → 校验 → 原子替换”临界区内短暂持有
  目录 `flock(LOCK_EX)`，配合每文件原子 rename 与版本号，串行化多进程并发写入；
  冲突落败方收到 `version_conflict`，由调度循环跳过本轮，而不是静默覆盖。
- **调度循环 `Tick`**：扫描所有非终态实例，跳过终态/退避未到期/他人活跃租约，
  对可推进实例各推进至多一步。重复调用安全且幂等。

---

## 5. 本地验证

需要 Go 1.25+（开发环境为 Go 1.26）。无外部依赖。

```bash
# 编译与静态检查
go build ./...
go vet ./...

# 全部单测（含重启恢复、重复调度、超时、永久失败、并发接管、失败续跑、审计轨迹等）
go test ./... -v

# 竞态检测 + 重复运行（并发用例建议多跑）
go test ./... -race -count=20
```

测试清单（`workflow/*_test.go`）：

| 测试 | 覆盖场景 |
| --- | --- |
| `TestStartAndCompleteHappyPath` | 启动、线性推进、结果/历史查询、终态拒绝 |
| `TestDuplicateStartRejected` | 重复启动被拒绝且不覆盖原输入 |
| `TestDuplicateSchedulingIsIdempotent` | 重复 Tick 不重跑已完成步骤、副作用计数 |
| `TestTransientRetryThenSuccess` | 瞬时失败退避重试、失败历史、事件计数 |
| `TestPermanentFailureStopsImmediately` | 永久错误不重试、终态不复活 |
| `TestRetryBudgetExhaustion` | 重试预算耗尽即失败（不会无限重试） |
| `TestDeterministicFailureConclusion` | 同一输入失败结论可复现 |
| `TestAttemptTimeoutRetriedAsTransient` | 单次尝试超时分类为 timeout 并退避 |
| `TestLeaseMutualExclusionAndExpiryTakeover` | 互斥、续租、过期接管、旧租约失效、fence 递增 |
| `TestConcurrentTickersOnlyOneWins` | 两执行者并发 Tick，恰好一方成功、步骤一次 |
| `TestRecoverAfterCompletedStep` | 成功一步后重启，从下一步续跑 |
| `TestRecoverAfterCrashMidAttempt` | 执行中崩溃 → 过期接管 → 旧结果被丢弃 |
| `TestRecoverDuringBackoff` | 退避中重启，不提前重试，到期后续跑 |
| `TestTerminate` / `TestUnknownActivityIsPermanent` | 终止幂等、未注册活动即永久失败 |
| `TestLeaseRenewalAndAdvanceWithWrongLease` | 续租延长有效期、错误 fence 被拒 |
| `TestTickAdvancesMultipleInstances` | 多实例同批推进 |
| `TestResumeContinuesFromStuckStep` | 续跑从卡住步骤继续、尝试编号连续、已成功步骤不重跑、历史不丢 |
| `TestResumeRequiresExplicitBudget` | 续跑必须显式给出正的追加预算，否则拒绝 |
| `TestResumeCompletedAndRunningRejected` | 成功/运行中实例拒绝续跑且原因可区分 |
| `TestConcurrentResumeOnlyOneWins` | 并发续跑同一实例恰好一个成功 |
| `TestResumeThenConcurrentTicksSingleAdvancer` | 续跑后并发调度只有一个执行者推进同一步骤 |
| `TestCrashInterruptedAttemptBudgetAccounting` | 崩溃在途尝试计入预算、接管不突破上限、续跑后续编号继续 |
| `TestResumeTerminatedInstance` | 人工终止的实例可续跑且终止历史保留 |
| `TestAuditTrailMatchesFinalState` | 审计轨迹与最终状态一致、重启后重建结果稳定 |
| `TestAuditMarksInterruptedAttempt` | 崩溃中断的尝试被标记 interrupted 且计入预算 |
| `TestAuditInProgressAttempt` | 在途尝试标记为 in_progress |
| `TestAuditCrashOnLastAttemptMarkedInterrupted` | **崩溃打断正好用完预算**：实例判失败，复盘把该尝试标 interrupted 而非 failed；失败记录带 `interrupted` 标志；重启后一致 |
| `TestAuditCrashWithBudgetLeftRetriesAndInterruptedIsSeparated` | **崩溃打断但预算有剩余**：接管方连续编号重试，旧尝试 interrupted（无失败记录）、新尝试按真实业务结果显示，剩余预算逐次递减 |
| `TestAuditTakeoverLeavesOnlyOneInProgress` | **接管后新尝试执行中**：同一步骤只有一条 in_progress（最新编号+活跃租约），历史被打断尝试全部 interrupted |
| `TestAuditStableAcrossRestartsAtEveryStage` | **重启一致性**：在途、崩溃后立即重启（旧租约未过期）、旧租约过期未接管、接管终态四个阶段反复重开目录，复盘结论稳定且不自相矛盾 |

测试日志会打印工作流标识、步骤下标、状态变化、尝试序号、租约/fence 与失败分类，例如：

```
level=info workflow=wf-1 step=0 msg="step started" status=running attempt=1 activity=charge lease=lease-… fence=1
level=warn workflow=wf-r step=0 msg="step failed" status=waiting attempt=1 class=transient will_retry=true backoff=50ms
level=error workflow=wf-p step=0 msg="step failed" status=failed attempt=1 class=permanent will_retry=false
```

只跑“崩溃/接管复盘”相关用例并查看每个业务场景的结论日志：

```bash
go test ./workflow/ -v -run 'TestAudit'
# 日志中每个用例末尾有一行 scenario=... result=...，例如：
# scenario=crash-exhausts-budget result=attempt1=interrupted remaining=0 workflow=failed final_reason="retry budget exhausted ... last attempt interrupted before completion" stable_after_restart=true
# scenario=crash-budget-left-continues result=attempts=[interrupted(rem=2) failed(rem=1) succeeded(rem=0)] stable_after_restart=true
# scenario=takeover-single-in-progress result=live_audit=[interrupted,interrupted,failed(in_progress=1)] final=[interrupted,interrupted,failed,succeeded]
# scenario=audit-stable-across-restarts result=stages=[live:in_progress after-crash:in_progress lease-expired:interrupted terminal:interrupted]
```

手工核对口径时可关注三点：

1. `interrupted` 的尝试**没有** `ended_at`，且对应步骤失败记录里 `interrupted=true`；
   业务真实失败的记录该标志为 false、有正常结束时间——两类不要混看；
2. 同一 `step_index` 的全部尝试记录里 `outcome=in_progress` 至多一条；
3. 每条尝试的 `remaining_budget` 与事件时间线吻合（续跑追加会让后续尝试的剩余变多），
   且最终 `status` 与最后一条尝试的去向一致。

### 端到端示例

```bash
go run ./cmd/dwf-demo -dir ./wfdata -id order-42 -input hello
# 再次运行同一命令：重复启动被拒绝，进入恢复模式，已完成步骤不重跑
go run ./cmd/dwf-demo -dir ./wfdata -id order-42 -input hello
cat ./wfdata/order-42.json   # 查看持久化快照与事件历史
```

最小代码用法：

```go
store, _ := workflow.NewFileStore("./wfdata")
eng := workflow.NewEngine(store, workflow.Options{WorkerID: "w1", LeaseDuration: 30 * time.Second})
_ = eng.RegisterDef(workflow.WorkflowDef{Name: "demo", Steps: []workflow.StepDef{
    {Name: "s1", Activity: "echo", Retry: workflow.DefaultRetryPolicy()},
}})
eng.RegisterActivity("echo", func(ctx workflow.ActivityContext, in workflow.ActivityInput) (string, error) {
    // 用 in.IdempotencyToken 在业务侧去重
    return "ok:" + in.Input, nil
})
_, _ = eng.Start(context.Background(), "wf-1", "demo", "hello")
for { // 或用 eng.Run(ctx, interval)
    _, _ = eng.Tick(context.Background())
    if st, _ := eng.Get(context.Background(), "wf-1"); st.Status.Terminal() { break }
    time.Sleep(100 * time.Millisecond)
}
```

## 兼容性（老实例与已有数据）

- **旧数据可直接打开**：`StepState.ExtraAttempts`、`WorkflowState.Resumes`、
  `workflow_resumed` 事件、`Failure.Interrupted` 均为新增可选字段，旧实例文件
  缺省为零值，语义与升级前完全一致（有效预算 = 定义中的 `MaxAttempts`；
  历史失败记录默认按“业务真实失败”理解）。
- **旧失败实例可续跑**：升级前进入 `failed` 的实例同样可 `Resume`，
  尝试编号从既有 `Attempt` 继续。
- **审计对旧事件降级可用**：旧 `step_started` 事件缺少 `max_attempts` 明细时，
  剩余预算按「定义基准 + 历次续跑追加」的时间线推算；定义未注册时该字段为 -1（未知），
  其余轨迹信息不受影响。
- **行为变化点**：尝试开始前新增预算入账检查——崩溃留下的在途尝试若已耗尽预算，
  接管时不再多跑一次活动，而是落账「预算耗尽（interrupted）」。这只影响
  「崩溃恰好发生在最后一次预算尝试执行中」的实例，使其不再凭空多获一次活动调用。

## 设计边界与扩展点

- 当前为线性步骤定义；`WorkflowDef`/`StepDef` 可扩展为 DAG（状态模型里步骤已按下标独立持久化）。
- `Store` 为接口，`FileStore` 是本地实现；可替换为 SQLite/数据库实现，
  引擎依赖的语义只有“按版本条件提交 + 事件追加 + 崩溃可重读”。
- `Clock` 可注入（`FakeClock`），因此租约过期、退避、超时都能确定性测试。
