# ADR-005: Monthly Budget Guard (Soft Budget)

- **Status**: Proposed
- **Date**: 2026-11-19
- **Deciders**: MixInfer Author
- **Related**: [ADR-004](./004-usage-cost-rate-limit-quota.md), [ADR-002](./002-streaming-data-plane.md), [governance.md](../governance.md)

---

## Context

### ADR-004 留下的三个前提

ADR-004 决策 5 明确把 Quota / Budget 顺延到 V0.4.1，并给出三个必须先解决的问题：

1. 明确额度累计的权威来源
2. 明确写入失败对额度的影响
3. 明确缓存与持久化的一致性

本 ADR 的唯一目标是把这三条变成可执行、可验证、边界清晰的决策，再进入实现。

### 为什么是独立编号，而不是 ADR-004 的延续

ADR-004 已处于 `Accepted` 状态，描述的是 V0.4 当时做出的决定。V0.4.1 是一组新的架构选择（独立台账、软预算、状态机、结算协议），应当拥有独立编号以保留历史决策的完整性。ADR-004 原样不动；ADR-005 负责冻结 V0.4.1 的预算设计。

### 版本定位：Soft Budget，不是强一致 Quota

本版本**不实现**金额预扣、reservation / commit / release 状态机、分布式额度。它实现的是一组明确边界的**软预算阈值**：

> 当月已知成本累计达到配置阈值后，拒绝该 Virtual Key 的新请求；不保证已放行请求完成后的总费用不超过预算。

因此版本名称定为 **Monthly Budget Guard**，文档中一律使用 `Budget`，不使用 `Quota`。`Quota` 一词保留给未来真正实现预留与强一致的版本。

### 现有代码现状（核对于 V0.4.0）

| 项 | 状态 |
|---|---|
| `usage_records` 写入 | best-effort，失败仅 warn（ADR-004 决策 1） |
| `CostCalculator` | 整数 microcents，未知返回 `null`，非 0 |
| `UsageSettlementStatus` | `PENDING / FINAL / PARTIAL / UNKNOWN` 已存在 |
| `ChatCompletionService.handleStream` | 同步消费整条流，结算在 `finally` |
| `handleStream` 的流创建前失败路径 | **不进入**流转发的 `finally`，但已各自记录 `UsageRecord` |
| 限流 | `InMemorySlidingWindowRateLimiter`，滑动窗口 |
| Migration | `DatabaseInitializer` 使用 `List<String>`，当前最高版本为 **V4** |

最后一行意味着新增预算表使用 **V5**，不是 V3。

### 需要冻结的决策

1. 预算台账的权威来源与表结构
2. 幂等写入协议
3. 缓存金额拆分与更新协议
4. 台账状态机
5. pending 队列的边界与溢出处理
6. 结算时机与单次结算保证
7. 未知成本的统计口径
8. HTTP 语义
9. 配置语义与版本指纹
10. Admin API 形状

### 交付顺序（供实施参考，不在本 ADR 冻结）

```text
P1. 台账持久化与幂等协议      ← 数据链路先做对
P2. 缓存、状态机、准入组件    ← 在正确数据上做判断
P3. 接入请求生命周期          ← 覆盖所有终止路径
P4. Admin API 与诊断
```

顺序不颠倒：先把台账写对，再让准入读它。

---

## Decision

### 决策 1：预算台账是独立表 `budget_settlements`，与 `usage_records` 职责分离

`usage_records` 是 best-effort 审计记录，不能作为预算依据（ADR-004 已论证）。预算使用独立台账。

```sql
-- Migration V5
CREATE TABLE budget_settlements (
    request_id          TEXT PRIMARY KEY,
    virtual_key_id      TEXT NOT NULL,
    period              TEXT NOT NULL,          -- "2026-11"
    cost_microcents     INTEGER,                -- NULL = 无法计算
    pricing_version     TEXT,
    usage_source        TEXT NOT NULL,          -- PROVIDER / ESTIMATED / NONE
    settlement_status   TEXT NOT NULL
        CHECK (settlement_status IN ('FINAL', 'PARTIAL', 'UNKNOWN')),
    budget_version      TEXT NOT NULL,          -- 预算配置指纹
    created_at          INTEGER NOT NULL,       -- UTC epoch millis
    CHECK (cost_microcents IS NULL OR cost_microcents >= 0)
);

CREATE INDEX idx_budget_key_period
    ON budget_settlements(virtual_key_id, period);
```

**`settlement_status` 的 CHECK 约束与决策 9 一致**：只允许 `FINAL / PARTIAL / UNKNOWN`。`PENDING` 是 V0.4 流式的进行态，不是预算台账的终结状态，数据库层拒绝它进入台账，避免错误数据混入。

**两张表的职责边界**：

| 表 | 职责 | 写入语义 |
|---|---|---|
| `usage_records` | 请求审计、诊断、运营查询 | best-effort，含被限流/被预算拒绝的请求 |
| `budget_settlements` | 预算累计、已知成本、预算状态判断 | 每个 `BudgetTicket` 最多一条，含无定价/用量未知的终结结果 |

**`budget_settlements` 与 `usage_records` 的条数不相等是正常现象**，不得用 `COUNT(*)` 互相校验：

- `budget_settlements` 记录已准入请求的**终结结算结果**，包含 `FINAL / PARTIAL / UNKNOWN / 无定价`
- `usage_records` 记录**审计事件**，额外包含被限流、被预算拒绝的请求，且可能因写入失败而丢失

**不做物化汇总表**：`(virtual_key_id, period)` 联合索引足以支撑单实例初期查询。等真实数据显示 `SUM` 成为瓶颈，再拆汇总表。

### 决策 2：幂等写入协议 —— Repository 返回枚举，事务内完成冲突检测

**不使用 `INSERT OR REPLACE`。** 普通重试策略不能静默覆盖已有结算。

Repository 只负责数据库操作，不感知业务语义（普通结算 vs pending 重试）。写入方法返回明确结果枚举：

```java
public enum SettlementWriteResult {
    INSERTED,              // 首次写入成功
    DUPLICATE_IDENTICAL,   // requestId 已存在，且全部结算字段一致
    IDEMPOTENCY_CONFLICT,  // requestId 已存在，但结算内容不一致
    PERSISTENCE_FAILED     // 写入本身失败（DB 不可写、超时等）
}
```

**事务内协议**：

1. 尝试 `INSERT ... ON CONFLICT(request_id) DO NOTHING`
2. 若影响行数为 0，读取已有记录
3. 比较 `virtual_key_id` / `period` / `cost_microcents` / `pricing_version` / `usage_source` / `settlement_status` / `budget_version`
4. 全部一致 → `DUPLICATE_IDENTICAL`；任一不一致 → `IDEMPOTENCY_CONFLICT`

**缓存更新协议（按 Ledger 入口区分）**：

Repository 不知道调用方上下文。由 `BudgetLedger` 决定从哪个入口调用、如何解读结果、如何更新缓存。两个入口：

- `settleNew(ticket, settlement)`：正常结算入口，仅当 Ticket 为 `ACTIVE` 时调用
- `retryPending(ticket)`：后台重试入口，仅当 Ticket 为 `PENDING` 时调用

| Repository 结果 | `settleNew` 后缓存更新 | `retryPending` 后缓存更新 |
|---|---|---|
| `INSERTED` | `persisted += cost` | `persisted += cost; pending -= cost`（effective 不变） |
| `DUPLICATE_IDENTICAL` | 不增加金额 | `persisted += cost; pending -= cost`（effective 不变） |
| `IDEMPOTENCY_CONFLICT` | 不增加金额；该 Key 转 `QUARANTINED` | 不增加金额；该 Key 转 `QUARANTINED` |
| `PERSISTENCE_FAILED` | `pending += cost`；入队 | 保持 `pending`；退避后重试 |

**为什么必须区分**：`settleNew` 遇 `DUPLICATE_IDENTICAL` 说明新请求撞了已有 requestId，不增加金额是正确的。但 `retryPending` 遇 `DUPLICATE_IDENTICAL` 说明**上一次实际已提交、只是被误判为失败**——数据库已有金额，内存中仍在 pending。若不把 `pending` 转成 `persisted`，pending 将永久滞留，`effective` 虽正确但状态失真。

**为什么不用 `boolean retryContext` 参数**：Repository 无法也不应该感知业务语义，使用布尔参数会让调用方与实现都容易误用（例如在 `SETTLING` 状态下错误地传 `retryContext=true` 导致 `pending -= cost` 变负）。改成两个独立入口后，`pending -= cost` 的合法性由 Ledger 根据 Ticket 状态保证，编译期与运行期都更安全。

**幂等冲突的故障级别**：冲突意味着账本完整性异常（UUID 几乎不可能碰撞，出现即说明内部 bug）。处理为：

- 当前请求的原有响应语义不变。结算阶段发生的幂等冲突不得改变已经确定的业务结果，也不得覆盖原始上游异常。
- 台账保留第一条记录，不覆盖
- 记 ERROR 日志，含 `requestId`，不含明文 Key
- 后续该 Key 的预算准入 **fail-closed**（返回 503），直到人工排查、确认根因修复后**重启进程**恢复

### 决策 3：`PeriodBucket` 拆分三层金额，重试不重复累计

**单个 `AtomicLong` 不够。** 系统有三条写入路径（直接落账、失败入队、pending 重试成功），单一计数器会导致重试重复累计或 Admin 展示失真。

**组件与状态归属**：

```text
BudgetLedger
└── BudgetLedgerState                      （全局：HEALTHY / DEGRADED / UNAVAILABLE）

BudgetKeyStateRegistry
└── Map<virtualKeyId, BudgetKeyState>      （单 Key：ACTIVE / QUARANTINED）

BudgetCache
└── Map<BudgetKey, PeriodBucket>           （BudgetKey = (virtualKeyId, period)）

PeriodBucket
├── persistedAmount
├── pendingAmount
├── unrecoverablePendingAmount
└── activeTicketCount
```

**`PeriodBucket` 不再保存任何状态字段。** 全局状态由 `BudgetLedger` 持有，单 Key 状态由 `BudgetKeyStateRegistry` 持有，`PeriodBucket` 只负责金额与在途计数。

```java
final class PeriodBucket {
    long persistedAmount;            // 已成功落库的金额
    long pendingAmount;              // 已计入缓存但尚未落库的金额
    long unrecoverablePendingAmount; // 队列满时无法入队的金额（见决策 5）
    int  activeTicketCount;          // 在途请求数（诊断用）
}

// effective = persistedAmount + pendingAmount + unrecoverablePendingAmount
```

**状态变化表**：

| 操作 | persisted | pending | unrecoverablePending | effective |
|---|---|---|---|---|
| 正常落账 100 | +100 | — | — | +100 |
| 写入失败，进入 pending 100 | — | +100 | — | +100 |
| pending 重试成功 | +100 | −100 | — | 不变 |
| 队列满，无法入队 100 | — | — | +100 | +100 |

**同步机制**：`PeriodBucket` 内部持一把 `ReentrantLock`，**同一把锁覆盖完整的"持久化 + 缓存更新"状态切换**。

- **结算路径**：

  ```text
  acquire Bucket Lock
    → DB write（SQLite 单连接池，受 busy_timeout 约束）
    → 根据 Repository 返回值更新 persisted / pending / unrecoverablePending
  release Bucket Lock
  ```

- **pending 重试路径**：与结算路径使用同一把锁协议

- **准入路径**：

  ```text
  acquire Bucket Lock
    → 读 effective = persisted + pending + unrecoverablePending
    → 检查 effective >= budget
    → 通过则登记 BudgetTicket（见决策 8）
  release Bucket Lock
  ```

**为什么 DB 写入必须在锁内**：早前草案把 DB 写入放在锁外、写入成功后短暂持锁更新缓存，声称"消除 DB 已写、缓存未更新的窗口"——这个结论不成立。实际序列仍可能是：线程 A 事务已提交但尚未持锁 → 线程 B 持锁读旧 `effective` → B 错误放行 → A 才更新缓存。把 DB 写入纳入锁内，才能保证"持久化"与"缓存可见性"对同一 Key 是原子的。

**这把锁不是预扣**：锁不覆盖上游 LLM 调用，只在准入检查与结算落账的临界区内持有。多个请求仍可同时通过准入（软预算的并发超支语义不变，见决策 7）。

**代价**：同一 Key 的准入检查在结算 DB 写入期间会短暂等待。V0.4 的 SQLite 是单连接池 + `busy_timeout=5000`，等待上限已被配置约束。**实现时必须测量并记录锁等待时间**，若同 Key 高并发下锁竞争显著，再评估乐观更新或分段策略。

### 决策 4：台账状态拆两层 —— 全局健康与单 Key 完整性

状态分两层，职责不同，不能混在一个枚举里。

**第一层：`BudgetLedgerState`（全局）**

描述存储与持久化通道的健康，影响所有配置了预算的 Key。

```java
enum BudgetLedgerState { HEALTHY, DEGRADED, UNAVAILABLE }
```

| 状态 | 进入条件 | 准入行为（有预算配置的 Key） |
|---|---|---|
| `HEALTHY` | 无待持久化结算 | 正常检查预算 |
| `DEGRADED` | 有 pending，但未触发不可用阈值 | `permissive` 放行；`strict` 返回 503 |
| `UNAVAILABLE` | 启动加载失败 / 连续持久化失败达阈值 / 队列溢出 | 返回 503 |

**第二层：`BudgetKeyState`（单 Key）**

描述某个 Virtual Key 的预算完整性，只影响该 Key。

```java
enum BudgetKeyState { ACTIVE, QUARANTINED }
```

| 状态 | 进入条件 | 准入行为 |
|---|---|---|
| `ACTIVE` | 正常 | 按 `BudgetLedgerState` 与预算判断 |
| `QUARANTINED` | 该 Key 发生幂等冲突，或该 Key 的**任一 Period** 金额溢出 | 返回 503 |

**作用范围对照**：

| 故障 | 隔离范围 | 自动恢复 |
|---|---|---|
| 启动加载失败 | 全局 `UNAVAILABLE` | 否（需重启进程并重新完整加载） |
| 连续持久化失败达阈值 | 全局 `UNAVAILABLE` | 是（见下方恢复规则） |
| pending 队列溢出 | 全局 `UNAVAILABLE` | **否**（见决策 5：溢出金额不在重试队列，无法通过重试恢复） |
| 幂等冲突 | 该 Key `QUARANTINED` | **否**（需人工排查后重启进程） |
| 单 Key 金额溢出（任一 Period） | 该 Key `QUARANTINED` | 否（需人工排查） |

**金额溢出的隔离范围说明**：金额溢出通常意味着存在严重逻辑 bug（累加了天文数字），视为 Key 级完整性事件，隔离整个 Key，不限于出问题的 Period。

**恢复规则（按故障原因区分，不用单一通用公式）**：

1. **普通数据库写入故障导致的全局 `UNAVAILABLE`**：当 pending 全部成功持久化、金额转移完成、连续失败计数清零，且数据库可正常写入后，`UNAVAILABLE → HEALTHY`。
2. **启动加载失败导致的全局 `UNAVAILABLE`**：需重启进程并**完整重新加载与校验**台账，不能仅凭一次 `INSERT` 成功恢复。
3. **队列溢出导致的全局 `UNAVAILABLE`**：不自动恢复。`unrecoverablePendingAmount` 无法通过现有重试机制补齐。**不能**把重启描述为数据恢复——重启后 `unrecoverablePendingAmount` 丢失。
4. **`QUARANTINED`（幂等冲突 / 金额溢出）**：当前进程内不自动恢复。必须先完成人工排查、确认根因修复后，才允许**重启进程**恢复服务。`QUARANTINED` 状态只存在于进程内存，**重启后不持久化**，Key 恢复为 `ACTIVE`；但若根因未修复，冲突会再次触发隔离。**重启不是数据修复手段，只是恢复服务可用性的手段。**
5. **一次成功的持久化操作重置连续失败计数**；pending 全部成功持久化后，`DEGRADED → HEALTHY`。
6. **没有配置预算的 Virtual Key 完全不受任何状态影响**，继续使用 V0.4 行为。

**默认值 `permissive` 是明确的产品取舍**：

> 数据库暂时写入失败时，优先服务可用性；若此时进程崩溃，未持久化结算可能丢失，预算累计可能低估。对更重视消费保护的部署，切换为 `strict`。

这条取舍必须同时出现在 ADR-005、`governance.md`、`troubleshooting.md`。

### 决策 5：有界 pending 队列、退避重试与队列满处理

```yaml
mixinfer:
  budget:
    degraded-mode: permissive
    pending-queue-capacity: 1000
    persistence-failure-threshold: 3
```

- **有界**：队列容量固定，不无限增长
- **退避重试**：`@Scheduled` 任务按指数退避重试，成功后 **只做 `pending → persisted` 转移，不重复增加 `effective`**
- **连续失败计数**：每次 `PERSISTENCE_FAILED` 递增，任一 `INSERTED` / `DUPLICATE_IDENTICAL` 归零；达到阈值 → `UNAVAILABLE`

**队列满时，当前刚结算的请求不能丢弃**：

- 金额计入 `unrecoverablePendingAmount`
- 全局台账立即转 `UNAVAILABLE`
- **该金额不进入重试队列，因此无法通过 pending 重试恢复**
- 进程存活期内 `effective` 仍然准确；进程崩溃后该笔丢失
- Admin 暴露 `unrecoverablePendingCount` / `unrecoverablePendingAmount`

理由：当前请求已经响应客户端，成本不能凭空消失；但队列满意味着持久化通道已失效，必须 fail-closed。

**为什么不设计自动恢复**：溢出的金额不在重试队列中，即使原有 pending 全部重试成功，`unrecoverablePendingAmount` 仍然存在。要让它可恢复，必须为溢出的完整结算记录提供另一个可持久化位置（Journal 或溢出表），这超出 V0.4.1 的复杂度预算。V0.4.1 的处理是：队列溢出 → 全局 `UNAVAILABLE` → 等待人工排查 → 重启后恢复服务（溢出金额本身丢失，需通过日志与 Admin 历史核对补账）。

### 决策 6：`BudgetTicket` 携带归属与版本，按准入时刻定周期

```java
public record BudgetTicket(
        String requestId,
        String virtualKeyId,
        String period,          // 准入时刻的 UTC 月份，"2026-11"
        String budgetVersion    // 预算配置指纹
) {}
```

**Ticket 不含预留金额。** 它只记录归属与版本，不构成预扣。

**周期归属规则**：

- `period` 由**准入时刻**的 UTC 月份决定
- `created_at` 由**实际结算时刻**决定
- 月结统计以 `period` 为准，不以 `created_at` 为准

**必然的副作用**：11 月 30 日 23:59 UTC 准入、12 月 1 日 00:01 UTC 结算的请求，会计入 `period = 2026-11`，使 11 月的台账在 12 月继续增长。这是规则的正确结果，不是 bug，需写入文档。

**`budgetVersion` 指纹算法**：

```text
SHA-256( vkId + "|" + microcentsPerMonth + "|" + "USD" + "|" + "monthly-budget-v1" )
保留完整 64 个 hex 字符（256 bit）
```

不包含认证秘密。预算金额或策略版本变化 → 指纹变化 → 历史结算可追溯。保留完整 256 bit 与截断在实现成本上零差异，更稳妥。

**拼接约束**：`virtualKeyId` 是配置 ID，不是任意用户输入。当前配置规则要求 `virtualKeyId` 不含 `|` 与换行；实现时加启动校验，违反即启动失败。不为此额外引入复杂的字段转义机制。

### 决策 7：Soft Budget 语义与已知的超支边界

**准入判断**（对配置了预算的 Key）：

```text
effective < microcentsPerMonth  → 允许
effective >= microcentsPerMonth → 拒绝（429 budget_exceeded）
```

**这不是硬上限。** 超支量取决于：

- 最后一个放行请求的实际成本
- 已放行但尚未结算的在途请求
- 未知成本请求（不参与金额累计）

**ADR-005 明确不承诺固定超支上限。** 早期草稿中的"(N−1)×单请求成本"公式是错误的，删除。反例：预算 100、已用 99.9、50 个并发请求各花 1，最终 149.9，超支 49.9 > 49。

未来若需要硬上限，必须实现 Reservation，属于独立版本的工作。

### 决策 8：结算时机 —— 统一入口，覆盖所有终止路径，每个 Ticket 只结算一次

**统一入口**：

```java
void recordSettlementSafely(BudgetTicket ticket, SettlementResult settlement);
```

**`SettlementResult` 使用独立枚举，不复用 `UsageSettlementStatus`**：

```java
public enum BudgetSettlementStatus {
    FINAL,    // 完整结算
    PARTIAL,  // 部分结算
    UNKNOWN   // 用量无法确定
}

public record SettlementResult(
        Long costMicrocents,               // NULL = 无法计算
        String pricingVersion,             // NULL = 无定价
        UsageSource usageSource,           // PROVIDER / ESTIMATED / NONE
        BudgetSettlementStatus settlementStatus  // FINAL / PARTIAL / UNKNOWN
) {}
```

**为什么独立枚举**：V0.4 的 `UsageSettlementStatus` 包含 `PENDING`。预算台账只接受终结状态，独立枚举能在编译层面阻止 `PENDING` 进入台账，与决策 1 的 SQL CHECK 双重保护。

**必须覆盖的终止路径**：

| 路径 | 是否进入流式 `finally` | 必须结算 |
|---|---|---|
| 非流式成功 | — | ✅ |
| 非流式路由失败 | — | ✅ |
| 非流式不可 failover 错误 | — | ✅ |
| 非流式全部 Endpoint 失败 | — | ✅ |
| 流式成功（DONE） | ✅ | ✅ |
| 流式客户端断开 | ✅ | ✅ |
| 流式上游中途断流 | ✅ | ✅ |
| **流式 `routePlanner.plan()` 失败** | ❌ | ✅ |
| **流式不可 failover 的 ProviderException** | ❌ | ✅ |
| **流式所有 Endpoint 打不开** | ❌ | ✅ |

后三行是易漏点：`handleStream` 在这些路径上**不会进入**流转发的 `finally`，因此不能只在该 `finally` 里调用结算。

**实现建议**：把现有的流式 `finally` 结算位置保留（已确认这是正确的同步生命周期终点），并在三个流创建前的终止路径分别调用同一个 `recordSettlementSafely`。不新增 `BudgetSettlementFinalizer` 类——当前 `handleStream` 是同步消费，不需要额外抽象。

**单次结算保证**：`activeTicketCount` 只用于 Admin 展示在途请求数，**不构成幂等机制**。去重必须靠请求级状态。

```java
enum TicketState {
    ACTIVE,        // 准入通过，尚未结算
    SETTLING,      // 正在执行结算（持锁）
    PERSISTED,     // 已落库
    PENDING,       // 已入队等待重试（中间态）
    UNRECOVERABLE, // 队列满，金额进入 unrecoverablePending
    QUARANTINED    // 幂等冲突或溢出
}
```

**各状态下允许的操作**：

| TicketState | 允许的操作 |
|---|---|
| `ACTIVE` | `settleNew`（转为 `SETTLING`） |
| `SETTLING` | 拒绝重复进入 |
| `PENDING` | **只有后台重试**可以执行 `retryPending` |
| `PERSISTED` | 已完成；本地去重记录注销后的重复调用由数据库幂等兜底（相同内容 → `DUPLICATE_IDENTICAL` 缓存不变；不同内容 → `IDEMPOTENCY_CONFLICT` 隔离 Key） |
| `UNRECOVERABLE` | 拒绝再次结算，保留本地去重记录 |
| `QUARANTINED` | 拒绝再次结算，保留隔离状态 |

**去重记录的保留策略**：

| TicketState | 去重记录 | 理由 |
|---|---|---|
| `ACTIVE` | 保留 | 结算过程中防止并发重复 |
| `SETTLING` | 保留 | 与 ACTIVE 相同 |
| `PENDING` | **保留** | 等待重试完成，提前删除会允许重复累计 |
| `PERSISTED` | **注销** | 数据库已有持久化记录，天然幂等 |
| `UNRECOVERABLE` | **保留至进程退出** | 金额在 `unrecoverablePendingAmount` 中但数据库无记录，注销后同一请求重复进入结算会**重复累计** |
| `QUARANTINED` | **保留隔离状态** | 重复结算不得重新累计；需人工排查后重启恢复 |

**关键约束**：

1. 处于 `PENDING` 的 Ticket 不能提前删除去重状态。只有落到 `PERSISTED` 终态后，才释放去重条目。`UNRECOVERABLE` 与 `QUARANTINED` 同样保留去重条目（分别保留至进程退出 / 人工隔离解除），防止 pending 重试、生命周期重复调用或业务重试触发同一 `requestId` 的重复累计。
2. **`PERSISTED` 之后的重复调用**：去重条目已注销，重新登记为 `ACTIVE` 走 `settleNew`。Repository 返回 `DUPLICATE_IDENTICAL` 时缓存不增加金额，返回 `IDEMPOTENCY_CONFLICT` 时该 Key 转 `QUARANTINED`。这是安全的幂等降级，但**不应被视为正常路径**——若频繁出现，说明生命周期管理有 bug，需排查调用方。
3. `activeTicketCount` 与去重记录的生命周期**不绑定**。`activeTicketCount` 在进入终态（`PERSISTED / UNRECOVERABLE / QUARANTINED`）时递减，`PENDING` 作为中间态继续计数；去重条目的释放则遵循上表各自的规则。
4. **`retryPending` 只能在 Ticket 为 `PENDING` 时调用**。若 Ticket 为 `ACTIVE / SETTLING / PERSISTED / UNRECOVERABLE / QUARANTINED`，拒绝重试并记 error 日志，不改变缓存金额。这条约束避免 `pending -= cost` 导致负数。

实现上使用轻量的请求级状态管理即可，不新建复杂服务类。

**异常隔离**：

- `usageRecorder.record(...)` 与 `budgetLedger.recordSettlementSafely(...)` **各自独立 try/catch**
- `UsageRecorder` 失败不得跳过预算结算
- 预算落账失败不得覆盖已确定的客户端响应或原始流异常

### 决策 9：未知成本的统计口径 —— 四指标不互斥

`cost_microcents = 0` 表示**已知成本为零**；`cost_microcents = NULL` 表示**无法计算**。两者语义不同，NULL 不参与金额累计。

**指标定义**：

| 指标 | 含义 |
|---|---|
| `chargedAmount` | 已知成本累计 = `persisted + pending + unrecoverablePending`。进程存活期内有效；重启后未持久化的金额无法恢复 |
| `unpricedCount` | 用量已知但缺少价格配置 |
| `unknownUsageCount` | 用量无法确定（`settlement_status = UNKNOWN`） |
| `partialUsageCount` | 用量不完整（`settlement_status = PARTIAL`） |

**后三个计数不互斥**。例如 PARTIAL 用量也可能同时缺定价，此时 `partialUsageCount` 与 `unpricedCount` 会同时计入。文档不得把它们描述为互斥分类。

**终结结算状态约束**：`budget_settlements.settlement_status` 只允许 `FINAL / PARTIAL / UNKNOWN`。`PENDING` 是 V0.4 流式的进行态，不是预算台账的终结状态——每个 `BudgetTicket` 在终结时写一条记录，不会出现 `PENDING` 行。此约束同时由决策 1 的 SQL `CHECK` 与决策 8 的 `BudgetSettlementStatus` 枚举保证。

**`budget_settlements` 的条数定义**：

> 按已准入请求的终结结算结果记录，每个 `BudgetTicket` 最多对应一条结算记录，包括可计价、无定价及用量未知的结果。NULL 不代表零成本，不参与金额累计，但参与相应的状态统计。

**Failover 成本统计边界**：预算累计沿用 V0.4 的请求级 `CostCalculator` 结算口径，仅统计该请求结算结果中能够计算的成本，**不保证覆盖 Failover 过程中所有可能产生费用的上游尝试**，也不等同于供应商实际账单。典型场景：Endpoint A 的请求可能已被供应商处理但连接异常，Failover 后 Endpoint B 成功返回 Usage——MixInfer 只得到 B 的可计算成本，而 A 可能同样产生了费用。预算功能不能宣称控制了供应商的真实总支出。

**明确的产品边界**：本版本的 Budget 只能约束**可计算成本**，不能保证限制供应商真实消费。若需避免未知成本绕过预算，需要单请求成本上限、可靠的 Usage 估算或预扣——这些都不在本版本范围。

### 决策 10：HTTP 语义

| 情况 | HTTP | code |
|---|---|---|
| 月度预算达到阈值 | 429 | `budget_exceeded` |
| 普通请求限流 | 429 | `rate_limited` |
| 台账 `UNAVAILABLE` 或 `strict` 下的 `DEGRADED` | 503 | `admission_unavailable` |

**不使用 402**：Payment Required 语义容易被理解为付款流程，而这里实现的是周期预算准入。429 + 独立 code 更直观。

**`Retry-After`（预算耗尽）**：

```text
Retry-After = ceil((nextUtcMonthStart - now) / 1 second)
最小 1 秒，无上限
```

UTC 自然月最长 31 天，`Retry-After` 完全可由 HTTP Header 表达，**不设置 30 天封顶**（封顶会使重试时间早于预算重置时间）。

**预算拒绝也写审计**：

```text
HTTP 429 budget_exceeded
  └─ UsageRecord(success=false, reason=budget_exceeded)
     └─ 不写 budget_settlements（未扣减）
```

与 V0.4 的限流拒绝审计规则一致。

**执行顺序**：限流先于预算检查。限流更便宜，先过滤高频请求，再做预算判断。

### 决策 11：配置语义与版本指纹

```yaml
mixinfer:
  virtual-keys:
    - id: vk_team_a
      name: "Team A"
      key: sk-a
      rate-limit:
        requests-per-minute: 60
      budget:
        microcents-per-month: 1_000_000_000   # $10.00

  budget:
    degraded-mode: permissive
    pending-queue-capacity: 1000
    persistence-failure-threshold: 3
```

**预算配置语义**：

| 配置 | 语义 |
|---|---|
| 未配置 `budget` | 不限制 |
| `microcents-per-month: 0` | **拒绝所有新请求** |
| `microcents-per-month > 0` | 月度预算上限 |
| `microcents-per-month < 0` | 启动配置校验失败 |

**为什么零预算写"拒绝所有新请求"而不是"不允许可计价请求"**：预算准入发生在上游调用之前，此时系统还不知道本次请求是否有价格、用量是否可确定、成本是否为 NULL。准入公式是 `effective >= budget → 拒绝`，当 `effective = 0 && budget = 0` 时所有请求都被拒绝。配置注释必须与实际行为一致。

**不采用"0 = 不限"**：零美元预算本身是合理且有用的配置。

**配置变更语义**：修改预算配置后重启生效。若新预算 **小于或等于**当月有效已用金额：

- 该 Key 立即进入预算耗尽状态（准入返回 429）
- 直到下一个 UTC 月初或管理员再次调整配置
- 已发生的消费不会清零

注意：准入规则是 `effective >= budget → 拒绝`，所以"等于已用"同样应拒绝，不只是"小于"。

**启动失败场景**（必须 fail-fast，不静默退回默认值）：

- `microcents-per-month` 为负
- `degraded-mode` 不是 `permissive` / `strict`
- `pending-queue-capacity <= 0`
- `persistence-failure-threshold <= 0`
- `virtual-keys[].budget` 或 `mixinfer.budget.*` 配置解析失败（类型不符、无法解析为数字、枚举值拼写错误等）
- `virtualKeyId` 含 `|` 或换行（指纹拼接安全要求，见决策 6）

### 决策 12：Admin API —— 两个端点

| 端点 | 返回 |
|---|---|
| `GET /admin/budget/status?virtualKeyId=&period=YYYY-MM` | 单 Key 的预算、已用、剩余、周期、全局台账状态与单 Key 状态、各类未知计数 |
| `GET /admin/budget/summary?period=YYYY-MM` | 所有 Key 的预算概览、超限状态、台账健康状况、`idempotencyConflictCount` |

- `period` 可选，默认当前 UTC 月份；两个端点都支持，不新增第三个端点
- **`period` 必须严格解析为有效的 `YearMonth`**，不是仅正则匹配。以下均返回 `400 invalid_request_error`：`2026-1`、`2026-13`、`2026-AB`、`2026-11-01`。`period` 是 UTC 月份标识，不做时区换算
- 不单设 `/pending` 端点，`ledgerState`（全局）与 `keyState`（单 Key）内嵌在上述响应
- **剩余额度计算**：`max(0, budget - effectiveSpent)`，已超支不返回负数
- **金额展示必须分层**：`persistedAmount` / `pendingAmount` / `effectiveSpent` / `unrecoverablePendingAmount`。只查 SQLite `SUM` 在 `DEGRADED` 下会漏算 pending
- **`idempotencyConflictCount` 是进程级运行期指标**，重启归零，文档说明它是诊断指标而非持久化历史统计；只放在 `summary`
- 响应不含明文 Key，只返回 `virtualKeyId`

**`GET /admin/budget/status` 响应字段**：

```json
{
  "virtualKeyId": "vk_a",
  "period": "2026-11",
  "budgetMicrocents": 1000000000,
  "persistedAmount": 800000000,
  "pendingAmount": 50000000,
  "unrecoverablePendingAmount": 0,
  "effectiveSpent": 850000000,
  "remaining": 150000000,
  "ledgerState": "DEGRADED",
  "keyState": "ACTIVE",
  "pendingSettlementCount": 1,
  "unrecoverablePendingCount": 0,
  "unpricedCount": 3,
  "unknownUsageCount": 1,
  "partialUsageCount": 2
}
```

**不做趋势 API**：V0.4 的 `usage_records` 聚合已支持时间窗口与模型维度，重复造一套预算趋势 API 没有价值。

### 决策 13：本版本不做的事

以下明确**不在 V0.4.1 范围内**，写入 `governance.md` 的"不做的内容"：

- 金额预扣 / reservation / commit / release
- 分布式额度（Redis / 多实例共享）
- 多维配额（组织 / 团队 / 项目层级）
- 动态预算调整 API
- `BudgetCacheCleanupTask`（内存 Bucket 数量有限，等实测证明有内存问题再做）
- 严格的"月结后上月数据不再变动"
- 本地 Journal（见 Future Work）
- 402 Payment Required

---

## Consequences

### Positive

- **权威来源明确**：预算依赖独立台账，不再受 `usage_records` best-effort 语义牵连
- **幂等可验证**：枚举返回值 + 事务内冲突检测 + `settleNew` / `retryPending` 双入口，重复结算不重复累计，冲突可观测
- **金额展示诚实**：三层拆分让 `DEGRADED` 下的 `effectiveSpent` 可解释
- **状态职责清晰**：全局 `BudgetLedgerState` 与单 Key `BudgetKeyState` 分层，`PeriodBucket` 只承载金额与在途计数
- **结算覆盖完整**：所有终止路径（含流创建前失败）都结算，每个 Ticket 只结算一次
- **边界写进文档**：软预算、崩溃丢失、无法约束未知成本、并发超支 —— 都是公开承诺而非隐含假设
- **为后续留口**：Journal、严格 Quota、分布式额度都有明确的演进位置

### Negative

- **崩溃丢 pending**：`permissive` 在 DEGRADED 状态下继续接受新请求，进程崩溃可能导致 pending 丢失和预算低估；`strict` 会在 DEGRADED 状态下拒绝后续新请求，降低故障期间进一步累积未持久化结算的风险，但不能消除已有 pending 的崩溃丢失风险
- **并发可超支**：无固定上限承诺，需要使用者理解软预算语义
- **单实例**：多实例部署时额度不共享，每实例独立缓存
- **无法约束未知成本**：无定价或用量未知的请求不扣减
- **幂等冲突 fail-closed**：冲突后该 Key 预算准入不可用，需人工排查后重启进程恢复
- **队列满时金额进入不可恢复桶**：进程存活期准确，崩溃后丢失

### Neutral

- `usage_records` 语义完全不变
- 新增 `VirtualKey.budget` 字段，无预算配置的 Key 行为与 V0.4 完全一致
- 新增 Migration V5
- 新增 `GlobalExceptionHandler` 两个异常映射

---

## Alternatives Considered

### Alternative 1：直接在 `usage_records` 上做 `SUM`

- **优点**：零新表
- **拒绝理由**：`usage_records` 是 best-effort，写入失败会导致少算用量、错误放行。ADR-004 已论证，本条不可行

### Alternative 2：金额预扣 + commit / release

- **优点**：硬上限，不超支
- **拒绝理由**：需要估算 max tokens 对应成本（可能远超实际，阻碍正常使用）、处理流式中断与重启恢复的预留状态。复杂度不成比例，且与"单实例软预算"的版本定位冲突。作为 Future Work 保留

### Alternative 3：进程崩溃安全的本地 Journal

- **优点**：pending 不丢失，崩溃后可恢复
- **拒绝理由**：V0.4.1 不引入 Journal。它属于"提高崩溃恢复能力"的独立工作，是否做、何时做应根据 V0.4.1 真实使用情况决定。列入 Future Work 候选，不锁定 V0.4.2

### Alternative 4：`INSERT OR REPLACE` 作为幂等策略

- **优点**：实现最简单
- **拒绝理由**：静默覆盖。同一 `requestId` 对应不同金额时应暴露冲突，不能覆盖第一条

### Alternative 5：单一 `AtomicLong` 缓存

- **优点**：简单
- **拒绝理由**：三条写入路径下会重复累计或展示失真。必须拆 `persisted / pending / unrecoverablePending`

### Alternative 6：402 Payment Required

- **优点**：语义直观
- **拒绝理由**：容易被理解为付款流程。429 + `budget_exceeded` 与限流的客户端处理一致

### Alternative 7：每次准入都查 DB

- **优点**：绝对权威
- **拒绝理由**：单连接池下会成为瓶颈，且 `DEGRADED` 状态无法通过"查 DB"表达（DB 可读不代表 pending 已落库）。缓存 + 状态机是更准确的模型

### Alternative 8：按结算时刻确定周期

- **优点**：`created_at` 与 `period` 一致，月结后不再变动
- **拒绝理由**：月末准入、月初结算的长请求会被错误归入下月，语义错误。按准入时刻归属是正确的选择，接受"上月数据月初增长"的副作用

### Alternative 9：`BudgetCacheCleanupTask` 定时清理旧月份 Bucket

- **优点**：控制内存
- **拒绝理由**：上月的 `BudgetTicket` 可能仍在途，或存在 pending。安全清理需要引用计数，复杂度不成比例。当前 Bucket 数量受 Virtual Key 数量约束，量级可忽略。等实测证明有问题再做

### Alternative 10：新增 `BudgetSettlementFinalizer` 抽象

- **优点**：职责更清晰
- **拒绝理由**：`handleStream` 是同步消费，现有 `finally` 已是正确的生命周期终点。真正需要补的是"流创建前失败路径也要结算"，用统一入口方法即可，不需要额外类。若 V0.5 改为异步流，再抽才有意义

### Alternative 11：`boolean retryContext` 参数区分普通结算与 pending 重试

- **优点**：单方法入口，签名简单
- **拒绝理由**：Repository 不应感知业务语义；布尔参数容易被误用（在非 `PENDING` 状态下错误传入会导致 `pending -= cost` 变负）。改用 `settleNew` / `retryPending` 双入口，合法性由 Ticket 状态在 Ledger 层保证

---

## Future Work

以下均为候选，不承诺版本：

1. **本地 Journal**：追加式 WAL 风格文件，启动时 replay 未持久化的结算。用于消除崩溃丢 pending 的边界
2. **严格 Quota**：reservation / commit / release 状态机，硬上限
3. **分布式额度**：Redis 或外部数据库，多实例共享
4. **单请求成本上限**：缓解未知成本绕过预算的问题
5. **预算变更审计**：记录预算配置的变更历史与生效时间

---

## Acceptance Criteria

| # | 验收点 | 预期结果 |
|---|---|---|
| 1 | 未配置预算的 Key | 与 V0.4 行为完全一致，零回归 |
| 2 | 零预算 | **拒绝所有新请求**（429） |
| 3 | 正常预算 | 达到阈值后返回 429 `budget_exceeded` |
| 4 | `Retry-After` | 指向下一个 UTC 月初，向上取整到秒，最小 1 秒，无上限 |
| 5 | 跨月请求 | 按准入时 `BudgetTicket.period` 结算 |
| 6 | 幂等：相同 requestId 相同内容 | `DUPLICATE_IDENTICAL`，不重复扣减 |
| 7 | 幂等：相同 requestId 不同内容 | `IDEMPOTENCY_CONFLICT`，当前响应不变，对应 Key `QUARANTINED`（503），人工排查后重启进程恢复 |
| 8 | 部分 Usage | 按已知部分成本结算，`PARTIAL` 状态落库 |
| 9 | 未知 / 无定价 | 分别统计，NULL 不参与金额累计，不当成 0 |
| 10 | 并发准入 | 验证允许超支的实际边界，不断言固定公式 |
| 11 | DB 写入故障（permissive） | 继续放行，pending 增加，Admin 可见，重试不重复扣款 |
| 12 | DB 写入故障（strict） | **已完成的请求保持原响应**；**后续新请求**返回 503 `admission_unavailable` |
| 13 | Pending 金额一致性 | 重试成功后 `pending → persisted`，`effectiveSpent` 不重复增加 |
| 14 | Pending 重试遇 `DUPLICATE_IDENTICAL` | `pending -= cost; persisted += cost`，`effective` 不变，pending 不滞留 |
| 15 | 队列满 | 金额入 `unrecoverablePending`，全局 `UNAVAILABLE`，Admin 可见；**重启后不自动恢复** |
| 16 | 重启恢复 | 已持久化结算恢复正确；pending 按 best-effort 边界验证；`unrecoverablePending` 丢失 |
| 17 | 流式所有终止路径 | DONE / PARTIAL / UNKNOWN / CLIENT_DISCONNECTED / 流创建前失败，`BudgetTicket` 均只结算一次 |
| 18 | **TicketState 去重与状态校验** | 同一 requestId 的并发结算最多一次进入 `SETTLING`；`PENDING` / `UNRECOVERABLE` / `QUARANTINED` 三种状态下均不释放去重条目；`retryPending` 仅在 Ticket 为 `PENDING` 时有效，其他状态拒绝并记 error；`PERSISTED` 后重复调用由数据库幂等兜底（相同内容不变，不同内容触发隔离） |
| 19 | 启动加载失败 | 全局 `UNAVAILABLE`，预算 Key 返回 503；恢复需重启进程并完整重新加载 |
| 20 | 金额溢出 | `SUM` 或缓存累计超过 `long` 范围时返回明确的台账异常并将对应 Key 转 `QUARANTINED`（整个 Key 隔离，不限于出问题的 Period），不将溢出当 0 继续放行 |
| 21 | 配置变更：新预算 < 当月已用 | 立即 429 |
| 22 | 配置变更：新预算 = 当月已用 | **立即 429**（准入规则是 `effective >= budget`） |
| 23 | 预算拒绝审计 | 写 `UsageRecord(reason=budget_exceeded, success=false)`，不写 `budget_settlements` |
| 24 | **`period` 参数校验** | `2026-11` 有效；`2026-1` / `2026-13` / `2026-AB` / `2026-11-01` 均返回 400 `invalid_request_error` |
| 25 | **配置解析失败 fail-fast** | `microcents-per-month` 为负 / `degraded-mode` 非法 / `pending-queue-capacity <= 0` / `persistence-failure-threshold <= 0` / `virtualKeyId` 含 `\|` 或换行 / 数值类型错误 —— 均导致启动失败，不静默退回默认值 |
| 26 | V0.4 回归 | 全部既有测试通过 |

---

## References

- [ADR-002: Streaming Data Plane and Usage Settlement](./002-streaming-data-plane.md)
- [ADR-004: Usage Persistence, Cost Calculation, and Rate Limiting](./004-usage-cost-rate-limit-quota.md)
- [governance.md](../governance.md)
- [SQLite UPSERT](https://www.sqlite.org/lang_upsert.html)
- [HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429)
- [HTTP Retry-After](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After)