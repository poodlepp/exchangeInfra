# 项目协作约定

## 项目定位

这是一个为 **CEX 钱包/风控** 设计的"生产级"项目。技术深度优先于功能广度——业务代码可以简化，但**技术要点必须有血有肉**：能讲清楚、能展开、能扛追问。每一处取舍都按"面试官会怎么问"来评估。

回复用中文。

## 重要文档

任何非平凡改动前，必须先读以下文档（按顺序）：

1. `docs/superpowers/specs/2026-05-26-cex-wallet-design.md` —— 总体设计与所有核心机制（§6.1 ~ §6.14）
2. `docs/superpowers/plans/0000-roadmap.md` —— 5 阶段实施路线图与阶段间依赖
3. `docs/superpowers/plans/<当前阶段>.md` —— 当前阶段的逐 task 实施计划

跳过这三份文档直接动手 = 大概率破坏既有约束。如果 spec 与代码冲突，**spec 是权威**——除非用户明确改设计。

## 设计红线（不可逾越）

破坏以下任何一条都需要先与用户对齐，不要自行降级：

- **双账法不可降级**：每笔资金移动必须写两条 `account_journal`（direction +1 与 -1，金额相等，trace_id 共享）；`uk(trace_id, direction, account_id)` 是幂等闸；不允许"只更新 account 表"或"单边记账"。
- **签名隔离**：私钥仅存在于 `wallet.signer` 子包内存中；`AesGcmCipher.wipe` 用完立即清零；不写日志、不进异常 message、不出现在 `toString()`。
- **链抽象方向**：`wallet.core` / `wallet.withdraw` / `wallet.scanner` **不依赖**任何具体链实现（`wallet.chain.btc/eth/tron`），只通过 `wallet.chain.api` 的 SPI 接口联动。新增公链 = 新建 `wallet.chain.<name>` 子包实现 5 个 SPI，不动其他代码。
- **MQ 边界**：业务代码**不直接使用** `KafkaTemplate` / `@KafkaListener`，只用 `TransactionalEventPublisher` / `EventPublisher` / `IdempotentEventHandler`。`TransactionalEventPublisher.publish` 必须在调用方事务内执行（`Propagation.MANDATORY`），与业务变更同事务写 outbox 表。
- **不引入工作流引擎**：状态机用 Spring Statemachine 表达 + Kafka 事件驱动主推进 + ShedLock 兜底巡检。不上 Camunda / Activiti。
- **模块组织**：`wallet` 与 `common` 内部用 **Java 子包**做强分层，**不拆 Maven 子模块**——避免侵入现有所有依赖方的 pom。

## 执行节奏

按 `docs/superpowers/plans/<当前阶段>.md` 逐 Task 推进时：

- **一个 Task 一个 commit**：每完成一个 Task（所有 step 通过），立即 `git commit`。commit message 用 plan 中该 Task 的 `git commit -m` 模板（一般在最后一个 step）,再加入taskid。
- **不批量 commit**：不允许多个 Task 攒一起再提交，否则 review 与回滚粒度都会丢失。
- **测试不绿不准 commit**：如果该 Task 包含测试 step，测试必须先全绿。
- **commit 前 `git status` 自检**：避免遗漏新建文件 / 误带入无关改动。

## 知识沉淀

项目中遇到的技术要点、面试题、踩坑总结，统一沉淀到 `question/` 目录，按主题分类整理为 Markdown 文档。

### 自动记录规则（交互过程触发）

**只要交互中讨论到任何值得沉淀的技术点，必须自动写入 `question/`，无需用户额外指令。** 触发条件（满足任一即记录）：

- 用户问"原理 / 注意事项 / 为什么 / 怎么实现"且回答中包含可复用的技术要点；
- 完成一个核心机制（§6.x）或解决一个非平凡 bug；
- 出现可被面试官追问的取舍（如选 A 不选 B、为什么这样切位、为什么这个阈值）。

**记录方式**：

- 文件名格式：`<area>-<topic>.md`，简明扼要，按主题分类。`area` 例：`wallet` / `mq` / `id` / `cache` / `db` / `spring` / `concurrency`。
- **先查后写**：写入前先看 `question/` 目录有无同主题文件；有则**追加章节**，无则新建。严禁同主题分散到多个文件。
- 每篇文档结构固定为四段：**问题/题面 → 标准答案要点 → 在本项目中的体现 → 延伸追问**。简明扼要，不堆字数；要点用列表，避免长段落。
- 一次交互沉淀一处。落盘后在回复末尾**单行说明**已写入哪个文件，方便用户回查。

### 已规划主题示例

- `question/wallet-nonce.md` —— nonce 管理、加速取消、并发分配
- `question/wallet-reconcile.md` —— 双账法、链上对账、对账差异处置
- `question/wallet-fee.md` —— EIP-1559、UTXO selection、TRON energy
- `question/wallet-reorg.md` —— 区块重组检测、回滚、反向冲账
- `question/mq-outbox.md` —— Kafka 事务发送、幂等消费、DLQ
- `question/spring-statemachine.md` —— 状态机配置、事件驱动
- `question/id-snowflake.md` —— 雪花算法位段、时钟回拨、workerId 分配
