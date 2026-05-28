# Wallet 双账法（复式记账）账本

> 写给 Java 后端：把会计学复式记账法搬进交易所账本，是钱包系统**最容易出 bug 也最难救**的一段。理解凑零不变量 + trace_id 幂等闸是命门。

---

## 一、为什么必须双账，单边记账不行吗

### 题面
"用户充值 1 ETH" 一行 `UPDATE account SET available = available + 1` 不就完了？为什么要写 2 行 journal？

### 要点
- **单边记账的死穴**：钱"凭空多出来"。审计追问"这 1 ETH 从哪来" → 数据库里查不到对手方 → 出现"幻象资金"。
- **双账法**：每笔资金移动**必须**拆成出入两条 `account_journal` 行，金额相等、direction 相反、共享 `trace_id`。
- 凑零不变量：任意时刻 `SUM(direction × amount) GROUP BY coin_id` 必为 0；破坏 = bug 立刻可见。
- 系统中间账户（INFLOW/HOT_WALLET/FEE/FROZEN_BUFFER，user_id 取负数）就是为了给"链上来的钱"提供对手方 — 充值 = `INFLOW.available -1` ↔ `user.available +1`。

### 项目中
- `wallet.core.ledger.LedgerServiceImpl#credit`：充值 → INFLOW DEBIT + user CREDIT
- `wallet.core.ledger.SystemAccountConstants`：4 个负数 user_id 常量
- `LedgerServiceImplIT.credit_writes_double_journal_and_zero_sum_invariant_holds`：测 `journalMapper.sumDirectionalAmount(coinId) == 0`

### 延伸追问
- 为什么用负 user_id 而不是单独表？— 系统账户与用户账户共享 (user_id, coin_id) uk + 同一套 CAS 路径，少一张表少一处分支。
- "凑零"的英文叫法？— accounting balance / zero-sum invariant / **double-entry consistency**。

---

## 二、`uk(trace_id, direction, account_id)` 三字段唯一约束

### 题面
为什么不直接用 `(trace_id)` 单字段做唯一约束做幂等？

### 要点
- **(trace_id) 单字段不行**：一笔业务必写 2 条 journal，trace_id 共享，单字段唯一会自我冲突。
- **(trace_id, direction) 也不行**：跨账户场景下，A 转 B：A 的 journal 是 DEBIT、B 的 journal 是 CREDIT —— 但若是同账户内部 freeze（available -1 / frozen +1），两条 journal 同 account_id 但不同 direction —— 这种情况 (trace_id, direction) 还是只有 2 条；但 transferAvailable A→B 同样符合 → 看似都能挡，但**外部双 transfer 同 trace_id 复用时会被错误放行**。
- **(trace_id, direction, account_id) 三字段**：覆盖三种场景：跨账户转账两条不同 account_id；同账户冻结两条不同 direction；同 account 同 direction 同 trace_id 必然是重入。
- LedgerServiceImpl 把 `DuplicateKeyException` 当作"幂等命中"信号 — 直接抛给调用方，由 Spring `@Transactional` 自动回滚整笔操作（调用方第一次已成功，第二次什么都不要做）。

### 项目中
- `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql` `account_journal` 表 `UNIQUE KEY uk_trace_direction_account`
- `LedgerServiceImplIT.credit_reentry_with_same_traceId_throws_duplicate_key`

### 延伸追问
- 调用方怎么用？— 业务层 `try { ledger.credit(cmd); } catch (DuplicateKeyException dup) { /* 已处理过，幂等成功 */ }`。
- 为什么不在 LedgerServiceImpl 内 `catch` 后吞掉？— 业务期望"账动了就该看到事件"，吞掉会导致调用方误以为新写入生效。

---

## 三、`account` 表 CAS 更新

### 题面
`account.available` / `frozen` 在并发下怎么保证一致？为什么不用 `SELECT ... FOR UPDATE`？

### 要点
- **乐观锁 CAS**：`UPDATE account SET available=?, frozen=?, version=version+1 WHERE id=? AND version=#{old}`，0 行影响 = 余额不足或并发冲突。
- **行锁会退化为串行**：`SELECT FOR UPDATE` 在高并发下连续阻塞，吞吐崩；CAS 不阻塞，失败抛异常由上层重试或回滚。
- **更严的方案**：`UPDATE ... AND available >= #{amount}`，在 SQL 内做余额校验单 round-trip 原子；本期实现先在 service 层 `if` 校验 + CAS（更易读），生产建议合并到 SQL（少 1 次 round-trip + 抗并发）。
- LedgerService 不做 CAS 重试 — 失败立刻抛 `IllegalStateException`，业务期望"扣钱失败立刻知道"，不应隐式重试。

### 项目中
- `wallet.core.mapper.AccountMapper#casUpdate(id, available, frozen, version)`
- `LedgerServiceImpl#casOrThrow`：affected == 0 抛 IllegalStateException
- `freeze_with_insufficient_available_throws_and_no_journal_written` IT 验证"扣钱失败 + journal 未落库"

### 延伸追问
- BigDecimal 比较为什么用 compareTo 不用 equals？— `new BigDecimal("1.0").equals(new BigDecimal("1.00")) == false`（scale 不同）；compareTo 才是数值比较。
- 已确认余额够后再 CAS，可能 CAS 时余额突变？— 是的，CAS 失败抛 IllegalStateException 由 `@Transactional` 回滚，调用方按业务策略重试或失败。

---

## 四、`balance_after` 字段的设计意图

### 题面
journal 已有 amount，再加 `balance_after` 是不是冗余？

### 要点
- **不是冗余，是 reconcile 的捷径**：单账户回放对账时，最新一行的 `balance_after` 就是当前余额；不需要从历史第一行重头累加 N 条。
- 双重校验：`account.available + frozen` 与 `journal 最新 balance_after` 应一致；任意一方写入逻辑 bug 都会让两边漂移，对账日报立刻可见。
- balance_after 写入时机：CAS 更新 account 之后立刻知道新余额，直接填入即可。

### 项目中
- `AccountJournalEntity.balance_after`
- `AccountJournalMapper#sumByAccount` 提供按账户聚合，配合 `account.available + frozen` 双校验

---

## 五、append-only journal vs 反向冲账

### 题面
reorg / 风控驳回 / 误操作时，journal 直接 DELETE 行不行？

### 要点
- **永远不删，只追加**：删了等于篡改账本。
- **反向冲账**：`reverseCredit` 写一组反向双账（新 trace_id，direction 与原相反），原 journal 留存。审计追问"为什么余额从 +1 变 -0"时，能看到完整动作链。
- 同理不 UPDATE journal —— 一切修正动作都是新 INSERT。
- 这是会计学几百年的实践：一切错都靠"反向凭证"修，永不擦黑板。

### 项目中
- `LedgerServiceImpl#reverseCredit`：被 reorg 处理（Plan 2 落地）调用
- `BizType.REVERSE_DEPOSIT` 枚举值

### 延伸追问
- 反向冲账后还可见原 journal 吗？— 是，按 biz_id 聚合能看到正向 +1 与反向 -1 双行，金额相抵。
- 用户体感是负余额吗？— 不一定。reorg 通常发生在确认数不够时，资金还没到 user.available；只有"已结算后又被回滚"才会出现暂时负数，需配合冷热分层 + 业务侧补救流程。

---

## 六、系统账户 @PostConstruct 自动建账

### 题面
HOT_WALLET account 不存在时业务调 settle 会怎样？

### 要点
- 系统账户必须在**业务用到之前**就在 DB 里有行。
- LedgerServiceImpl `@PostConstruct void initSystemAccounts()`：启动期 INSERT 4 行（INFLOW / HOT_WALLET / FEE / FROZEN_BUFFER）at coin_id = 0；多实例并发启动靠 `uk(user_id, coin_id)` 兜底（撞 uk → 已有，跳过）。
- coin_id=0 是占位 — 真业务的 coin_id 第一次出现时通过 `ensureAccount(systemUserId, realCoinId)` 自动建账；同样 uk 兜底。
- 这避免了"上线第一笔提现报 NPE"的尴尬。

### 项目中
- `LedgerServiceImpl#initSystemAccounts` + `ensureAccount`
- `SystemAccountConstants`: INFLOW=-1 / HOT_WALLET=-2 / FEE=-3 / FROZEN_BUFFER=-4

---

## 七、易踩的坑（测试必须覆盖）

| 坑 | 后果 | 测试覆盖 |
|---|---|---|
| 单边记账（漏写一条 journal） | 凑零不变量破，审计永远查不出 | `sumDirectionalAmount(coinId) == 0` 断言 |
| 跨账户转账没在同一事务 | A 扣 B 没加 → 总额不平 | `@Transactional` 默认 + IT 验证两端余额 |
| 同 traceId 重入未挡 | 余额翻倍 | `credit_reentry_with_same_traceId_throws_duplicate_key` |
| BigDecimal.equals 而非 compareTo | 1.0 != 1.00 误判失败 | IT 用 `isEqualByComparingTo` |
| Service 校验余额后 CAS 之间余额突变 | 出现负余额 | CAS 0 行影响 → throw → 回滚 |
| SystemAccount 不存在被引用 | NPE on first use | `@PostConstruct` 初始化 + uk 兜底 |
| CAS 重试隐式吞错 | 业务以为失败但实际成功 | 本实现明确不重试，失败即抛 |
