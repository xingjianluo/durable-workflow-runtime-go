# 本地持久化工作流执行引擎（Go）

一个零外部依赖、基于本地文件系统的持久化工作流执行引擎。它保证：

- 工作流启动、步骤推进、结果查询，每次状态变化都先落盘再返回；
- 同一逻辑步骤的重复调度/重试/崩溃接管不会重复产生业务副作用；
- 冲突与非法状态迁移一律被**拒绝并返回可区分原因**，不静默覆盖；
- 活动失败按「瞬时 / 超时 / 永久」分类：瞬时可重试、永久立即终止、重试预算耗尽即失败；
- 失败历史、最终分类、重试依据全部持久化、可查询、同输入结论可复现；
- 已失败的实例可由调用方**显式续跑**（`Resume`）：追加明确的尝试预算后从卡住的步骤继续，
  已成功步骤不重跑、实例身份与历史不变；
- 每一轮尝试的完整轨迹（起止、分类、是否被崩溃/接管中断、当时剩余预算）与每次续跑记录
  可通过 `Audit` 从持久化数据稳定重建；
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
              │ running │ ───────────────▶ │ completed  │（终态，不可逆）
              └─────────┘                   └────────────┘
                 │  ▲  永久错误 / 重试预算耗尽 / Terminate
                 │  └──────────────────────┐
                 ▼                         │
              ┌─────────┐   Resume(extra)  │
              │ failed  │ ─────────────────┘
              └─────────┘（失败态：可显式续跑）
```

- `running`：至少还有步骤未结束（步骤可能在等待退避）。
- `completed`：全部步骤成功；`Result` 为末步结果。不可续跑、不可逆。
- `failed`：某步骤永久失败或重试预算耗尽；`Failure` 记录最终分类与原因。
  失败态不再自动推进，但可由调用方 `Resume` 显式追加预算后回到 `running`（见第 3.4 节）。
- 除「failed →（显式 Resume）→ running」外终态不可逆：重复 `Tick`、重复 `Advance`、
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
| `resume_completed_workflow` | 对已成功完成的实例调用 `Resume` |
| `resume_not_failed` | 对非 failed 状态（如 running）的实例调用 `Resume` |
| `resume_invalid_budget` | `Resume` 未显式给出有效的追加尝试次数（<=0） |
| `lease_not_held` | 未持有租约却推进 |
| `lease_held_by_another_worker` | 他人有效租约未过期 |
| `lease_expired` | 租约已过期，需重新领取/接管 |
| `lease_version_stale` | token/fence 不匹配（租约已被接管） |
| `version_conflict` | 乐观版本不匹配（并发提交落败） |

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
   新执行者必须等旧租约过期后才能接管（产生 `lease_taken_over`，fence 递增）。
   **在途尝试的记账规则**：`step_started` 落盘即视为一次已消耗的尝试——接管者
   先补记一条 `Interrupted` 失败记录（事件 `step_attempt_interrupted`），再按预算判定：
   - 预算仍有剩余：步骤回到待调度，以**同一个幂等令牌**开始第 n+1 次尝试；
   - 预算已耗尽（n ≥ 有效预算）：**不再发起任何新的活动调用**，直接以
     「重试预算耗尽」提交终局失败。既不会凭空多送一次活动调用，也不会漏记已消耗的尝试。

   被遗弃的旧协程晚到的结果会因 fence/版本不匹配而被**丢弃**，不会覆盖新状态。
3. 若崩溃发生在**退避等待期间**：`NextAttemptAt` 已持久化，恢复后在到期前
   不会提前重试，到期后正常重试。

### 2.3 与老实例数据的兼容

续跑/审计引入的字段（`StepState.ExtraAttempts`、`WorkflowState.Resumes`、
`Failure.Interrupted`）全部 `omitempty`：老版本写出的状态文件读进来后这些字段为零值，
行为与之前完全一致（有效预算 = 原 `MaxAttempts`，无续跑记录）；老实例可以直接
`Resume`，追加预算从 0 开始累计。事件日志是纯追加的，新事件类型
（`step_attempt_interrupted`、`workflow_resumed`）只在新路径上产生，不影响旧事件的解释。

### 2.4 幂等令牌（业务副作用恰好一次）

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
终止本身**不消耗尝试预算**（`Attempt` 不变），因此终止后仍可续跑。

### 3.4 失败后显式续跑（`Resume`）

`Engine.Resume(id, extraAttempts)` 让一个 `failed` 实例从卡住的步骤接着跑：

- **适用条件**：仅当实例处于 `failed`（重试预算耗尽、超时/永久失败、人工 `Terminate`）。
  已成功完成的实例返回 `resume_completed_workflow`，仍在运行的实例返回
  `resume_not_failed`，均为可区分的拒绝原因。
- **追加预算语义**：`extraAttempts` 是本次愿意追加的尝试次数，**必须显式给出且 ≥ 1**；
  不指定（0）或负数一律拒绝（`resume_invalid_budget`），绝不默认放开。
  步骤的有效预算 = `RetryPolicy.MaxAttempts` + 历次追加累计（`StepState.ExtraAttempts`），
  已消耗的尝试编号继续累进，不重置。
- **身份与历史不变**：续跑不是新建实例——ID、游标、已成功步骤的结果、失败历史、
  事件日志全部保留；每次续跑追加一条 `ResumeRecord`（第几次、何时、谁发起、
  追加多少预算）与 `workflow_resumed` 事件。
- **与正常跑一致**：续跑后的实例与一路正常跑的同类实例遵循同一套判定——
  相同的失败分类、相同的预算检查、相同的终局结论；已成功步骤绝不重跑，
  业务副作用靠稳定幂等令牌保证不重复。
- 续跑只是把工作流置回 `running` 并追加预算，实际推进仍走租约 + fencing + 乐观版本，
  因此续跑与接管并发时同一步骤任一时刻只有一个执行者在推进。

### 3.5 审计轨迹（`Audit`）

`Engine.Audit(id)` 从持久化的事件历史与快照重建一个实例的完整轨迹，不依赖进程内存，
重启后查询结果稳定一致：

- 每一轮尝试一条 `AttemptRecord`：尝试编号、开始/结束时间与时长、结局分类
  （`succeeded` / `failed` / `interrupted` / `in_flight`）、失败分类与原因、
  执行租约，以及**该尝试结束后按当时有效预算计算的剩余可用尝试次数**；
- 每一次续跑一条 `ResumeRecord`：第几次续跑、追加了多少预算、卡在哪个步骤；
- 每个步骤当前的有效预算 `EffectiveMaxAttempts`。

这些信息串起来即可解释「当时为什么继续、为什么终止」：例如某次尝试是活动返回失败、
还是被崩溃/接管中断；预算耗尽终止时已经消耗了几次；续跑后又追加了多少。

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

# 全部单测（含重启恢复、重复调度、超时、永久失败、并发接管等）
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
| `TestResumeContinuesFromFailedStep` | 续跑从卡住的步骤继续、尝试编号累进、已成功步骤不重跑、历史保留 |
| `TestResumeRequiresExplicitBudget` | 不指定/无效追加预算一律拒绝且状态不变 |
| `TestResumeCompletedAndRunningRejected` | 成功实例/运行中实例续跑被可区分原因拒绝 |
| `TestResumeAfterTerminate` | 人工终止后续跑，终止不消耗预算 |
| `TestResumeMatchesStraightRunConclusion` | 续跑实例与一路正常跑实例的最终状态/尝试数/失败记录一致 |
| `TestResumeConcurrentTakeoverSingleAdvancer` | 续跑后两执行者并发推进，恰好一方成功、副作用一次 |
| `TestInterruptedAttemptAccountingAtBudgetLimit` | 崩溃在途尝试计为已消耗，预算耗尽不再发起活动调用，续跑后继续 |
| `TestAuditTrailMatchesFinalState` | 审计轨迹覆盖每轮尝试与续跑，重启后重建一致、与最终状态对得上 |

测试日志会打印工作流标识、步骤下标、状态变化、尝试序号、租约/fence 与失败分类，例如：

```
level=info workflow=wf-1 step=0 msg="step started" status=running attempt=1 activity=charge lease=lease-… fence=1
level=warn workflow=wf-r step=0 msg="step failed" status=waiting attempt=1 class=transient will_retry=true backoff=50ms
level=error workflow=wf-p step=0 msg="step failed" status=failed attempt=1 class=permanent will_retry=false
level=error workflow=wf-c step=0 msg="attempt interrupted" status=failed attempt=2 class=transient will_retry=false max_attempts=2
level=info workflow=wf-c step=0 msg="workflow resumed" status=running resume_seq=1 extra_attempts=1 attempts_used=2
```

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

## 设计边界与扩展点

- 当前为线性步骤定义；`WorkflowDef`/`StepDef` 可扩展为 DAG（状态模型里步骤已按下标独立持久化）。
- `Store` 为接口，`FileStore` 是本地实现；可替换为 SQLite/数据库实现，
  引擎依赖的语义只有“按版本条件提交 + 事件追加 + 崩溃可重读”。
- `Clock` 可注入（`FakeClock`），因此租约过期、退避、超时都能确定性测试。
