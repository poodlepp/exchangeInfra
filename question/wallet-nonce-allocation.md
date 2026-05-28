# Wallet Nonce 分配（DB 乐观锁版）

> EVM 系链每笔交易必须带连续递增的 nonce（同一地址，同一链）。空一个 nonce → 整条 pending 队列卡死直到那个空洞被填上。这就是热钱包退出最常见的死因之一。

---

## 一、为什么不直接 `eth_getTransactionCount(pending)`

### 题面
代码里要发一笔提现，直接 `provider.eth_getTransactionCount(addr, "pending")` 不就拿到下一个 nonce 了？

### 要点
- **节点的 pending 视图不是全局**：`pending` 只看本节点 mempool；其他节点已收到但尚未广播过来的 tx 不算。多实例服务并发取号 → 大概率两笔同 nonce。
- **同进程并发更糟**：5 笔提现同时进 worker，5 个线程都拿到 nonce=N → 5 笔同 nonce 提交 → broker 端只有 1 笔上链，4 笔被拒（"already known" / "nonce too low"）。
- **必须有"应用侧权威序号源"**：让 DB 表 `nonce_register` 来当唯一发号者，链上 pending count 只用作启动期校准。

### 项目中
`wallet.nonce.DbOptimisticNonceAllocator#allocate`：DB 是发号源；`reconcile` 在启动 / 重启 / 异常恢复时拿链上 pending 来对齐 DB。

### 延伸追问
- 那 reconcile 时也会和 RPC 节点对话啊，为啥它能信？— 不能完全信，所以 reconcile **只升不降**：`if (row.nextNonce < onChainPending)` 才覆盖。本地视图比链上新（已分配未广播）一律保留。

---

## 二、`(chain, address)` 单行 + version CAS

### 题面
为什么用 `version` 字段做 CAS 而不是 `SELECT ... FOR UPDATE`？

### 要点
- **乐观锁不阻塞**：`UPDATE ... SET next_nonce=next_nonce+1, version=version+1 WHERE chain=? AND address=? AND version=?old`，0 行 = 别人抢先；返回 1 行 = 我赢。
- **行锁串行化吞吐崩**：高频提现热门地址下，`FOR UPDATE` 把并发线程排成长队，QPS 直线掉到 1/网络延迟。
- **重试上界 5 次**：撞到的概率指数下降，5 次还输基本是 DB 主从延迟或时钟问题，宁可抛 IllegalStateException 让上层补救。
- **MyBatis-Plus `@Version` 注解** 只对自动 update 生效；这里是手写 SQL，version 完全自己管。

### 项目中
- `wallet.nonce.NonceRegisterMapper#casIncrement`：`SET next_nonce=next_nonce+1, version=version+1 WHERE version=#{version}`
- `DbOptimisticNonceAllocator#allocate` 循环 `MAX_RETRY=5` 次 CAS，赢则返回 allocated nonce，输则重读 row 再战

### 延伸追问
- CAS 撞 5 次不会丢号吗？— 不丢。每次 CAS 失败就**重读最新 next_nonce 再申请**，永远从最新位置往后取；丢号只发生在已取号的 tx 没成功广播且不打算重发。
- 为什么不用 Redis INCR 当发号器？— 持久性弱 + Redis 重启 / failover 后值可能回退；DB row 走 binlog + raft，强持久。

---

## 三、`@Transactional(REQUIRES_NEW)` 短事务隔离

### 题面
allocate 直接放在业务事务里不就好了？为什么要 REQUIRES_NEW 强行开新事务？

### 要点
- **业务事务通常很长**：发 outbox + 写 withdraw_order + allocate nonce + 调用签名服务 …… 嵌入业务事务 → CAS 行锁持有到业务结束 → 同一地址其他并发取号被阻塞。
- **REQUIRES_NEW = 独立短事务**：进入 allocate 时挂起外层事务，新开 1 个事务做完 CAS 立即 commit，外层接着跑。CAS 持锁时间 = 一次 UPDATE 往返。
- **取号成功但业务回滚怎么办**？— 这个 nonce 就跳过了，下笔顺移取下一个。**不允许复用跳过的 nonce**——会产生空洞。代价是 nonce 浪费一个，没什么。
- 但 EVM nonce 浪费就意味着空洞 ……？— 见第五节"加速取消"。

### 项目中
`DbOptimisticNonceAllocator#allocate` / `#reconcile` 都标 `@Transactional(propagation = Propagation.REQUIRES_NEW)`，对应注释明确写"避免嵌入业务事务持锁过久"。

### 延伸追问
- 为什么不 `Propagation.NOT_SUPPORTED`（无事务运行）？— CAS UPDATE 必须事务，不然锁释放时机不可控。
- 测试时 REQUIRES_NEW 注意：测试默认 `@Transactional` 回滚，但 REQUIRES_NEW 内的事务自己 commit 不受外层 rollback 影响 → 测试后数据残留，需要 `@Sql` 清表或 testcontainers fresh。

---

## 四、reconcile 只升不降 + INSERT IGNORE 兜底

### 题面
启动期 / 长时间空闲后重新启用某地址，怎么和链上 pending 对齐？

### 要点
- **三种 case**：
  1. DB 里没行 → `INSERT IGNORE`（多实例并发启动撞 uk → 已有，跳过）；
  2. DB.nextNonce ≥ onChainPending → 不动（本地有未广播的预占号，链上还没看见）；
  3. DB.nextNonce < onChainPending → 链上已经走得更远（外部工具或其他实例发了交易），本地落后 → CAS 升上去。
- **永远不降低 nextNonce**：哪怕链上 pending 比 DB 小（reorg / 节点回退），也不复用旧号；旧号代表"我们曾经发出去的 tx"，无论它现在怎样我们都不能复用，否则同 nonce 双发 = 一笔被吞。
- INSERT IGNORE 而非 ON DUPLICATE KEY UPDATE：第一次写入即定型，后续不动旧值；多实例并发安全。

### 项目中
`DbOptimisticNonceAllocator#reconcile`：
```java
if (row.getNextNonce() < onChainPendingNonce) {
    int affected = mapper.reconcile(...);  // CAS-升级
}
// 否则什么都不做
```

### 延伸追问
- "lost CAS race" 的 warn 日志意味着什么？— 同一地址同时被两个实例 reconcile，一个赢一个输，不影响正确性（赢家的版本就是最新）。频繁出现说明实例间 schedule 没分摊好。
- 启动期没 reconcile 直接 allocate 会怎样？— `find` 返回 null → 抛 IllegalStateException("nonce_register not initialized ... call reconcile() first")。设计上把"未初始化"明确暴露，不偷偷创建。

---

## 五、空洞、加速、取消

### 题面
nonce=N 的 tx 卡了（gas 太低 / 节点丢包），N+1, N+2 都已经发出但被 pending 堵在后面，怎么办？

### 要点
- **EVM nonce 必须严格连续**：N 不上链，N+1 / N+2 永远在 pending。
- **加速（speed up）**：用同 nonce N + 更高 gasPrice / maxFeePerGas（建议 +10% 起跳，部分节点要求 ≥10%）覆盖原 tx；新 tx 上链后挤掉旧 tx，N+1/N+2 自然排队上链。
- **取消（cancel）**：用同 nonce N + 更高 gas + `to=self, value=0, data=0x` 替换 → 链上看就是"自转 0 ETH"，意义在于把这个 nonce 占位填掉。
- **关键**：加速 / 取消都**复用同一个 nonce**，不是分配新号。allocator 必须配合提供"同笔订单复用旧 nonce"的接口（Plan 2 实现 `WithdrawOrder.nonce` 字段持久化 + reuse）。
- 如果误以为加速要新分配 nonce → 链上 pending 多出一笔孤儿 tx，nonce 空洞依然存在，没救活反而占资金。

### 项目中
- `WithdrawOrderEntity.nonce` 字段：本期 Plan 1 已留位，Plan 2 落 `accelerate(orderId)` / `cancel(orderId)` 时复用。
- `DbOptimisticNonceAllocator` 本期只提供 `allocate` —— "复用 nonce" 是上层订单服务的职责，不污染发号器。

### 延伸追问
- TRON / BTC 也有 nonce 吗？— TRON 的 sequence number 概念类似但宽松（部分场景不强制连续）；BTC UTXO 模型不需要 nonce，"nonce 空洞"问题不存在。`NonceAllocator` 接口对 BTC 实现可以返回 0 占位或直接 throw（取决于 Chain）。
- 同一笔 tx 加速 5 次怎么追？— `WithdrawOrder` 表关联 `tx_attempts` 子表，记录每次尝试的 hash + gasPrice，方便回查"这笔订单实际哪个 tx 上链了"。

---

## 六、易踩的坑（Plan 2 测试必须覆盖）

| 坑 | 后果 | 测试覆盖建议 |
|---|---|---|
| allocate 嵌入业务事务（漏 REQUIRES_NEW） | 同地址并发取号被串行 + 业务事务长时间持锁 | 显式断言 `@Transactional` propagation 注解 |
| reconcile 时直接 SET（非 CAS） | 多实例并发覆盖 → 老值压新值 | IT 验证 nextNonce 单调不降 |
| allocate 取号后业务回滚不重发 | nonce 空洞，全队卡死 | 提现状态机 + 兜底重试 |
| 加速时分配新 nonce | 旧 tx 没被覆盖，nonce 空洞依然存在 | accelerate 路径单测 |
| RPC 节点返回 stale pending count（节点落后） | reconcile 把 nextNonce 降回去 → 复用 nonce → 上链双发被拒 | reconcile 实现"只升不降"硬约束 |
| 启动期未 reconcile 直接 allocate | NPE / null row | allocate 先检查 row 不存在则 throw |
| @Version 注解和手写 CAS 共存 | MP 自动更新和手写 SQL 都改 version → 版本号跳变 | 项目内手写 SQL 路径不走 MP updateById |
