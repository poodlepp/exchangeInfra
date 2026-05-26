# CEX 钱包项目 5 阶段实施路线图

> 本路线图把 `2026-05-26-cex-wallet-design.md` 拆成 5 个独立可交付的 Plan，每个 Plan 完成后系统都处于"能跑、能测、能演示"的状态。详细任务步骤拆分位于 `docs/superpowers/plans/` 下各自的 plan 文档。

## Plan 1 — Foundation（基础设施）

**交付件路径**：`docs/superpowers/plans/2026-05-26-plan1-foundation.md`

**职责范围**：
- `common.mq` 子包：`DomainEvent` / `TransactionalEventPublisher` / `EventPublisher` / `IdempotentEventHandler` 抽象、Outbox 表 + OutboxRelay、ConsumedRecord 幂等闸、Kafka 配置封装、ShedLock 集成、Prometheus 指标
- 数据库 V2 迁移脚本（16 张业务表 + outbox + consumed_record + reconcile_report）
- `wallet.chain.api`：所有 SPI 接口（ChainClient / TxBuilder / TxBroadcaster / TxParser / AddressDerivator / Signer）+ 链无关 DTO（RawTx / SignedTx / ChainTx / Block / TransferRequest / TxStatus / KeyRef / DerivedAddress）
- `wallet.signer`：HD 派生（BIP32/39/44）、AES-GCM 加密、KmsProvider 抽象 + 本地 keystore 实现、Signer 实现、私钥清零
- `wallet.nonce`：NonceAllocator（DB 乐观锁 + Redis Lua 兜底 + 启动校准）
- `wallet.fee`：FeeStrategy 接口（具体实现在各链 plan 内落地）
- `wallet.core`：账户、双账法 LedgerService（freeze/unfreeze/credit/debit/transferInternal/reverseCredit）、地址池、AccountJournal 全套幂等闸

**完成判据**：
- `mvn clean test` 全绿，所有抽象接口与 wallet.core / common.mq / wallet.signer / wallet.nonce 单测通过
- 启动 bootstrap 不报错，actuator/health 正常
- LedgerService 跑过"凑零不变量"集成测试（双账平衡）

**不做的事**：
- 不接任何具体公链
- 不实现 scanner / withdraw / sweep / treasury / reconcile（在 Plan 2/3 落）
- 不实现 FeeStrategy 的具体实现（在各链 plan 落）

## Plan 2 — ETH 端到端

**交付件路径**：`docs/superpowers/plans/<date>-plan2-eth-e2e.md`（在 Plan 1 收尾时再写）

**职责范围**：
- `wallet.chain.eth`：MultiRpcChainClient（多 RPC 高可用 + circuit breaker）、TxBuilder（EIP-1559）、TxBroadcaster、TxParser（含 ERC20 Log 解析、特殊交易过滤）、AddressDerivator、FeeStrategy ETH 实现
- `wallet.scanner`：AbstractScanner + ETH 子类、reorg 主动检测与回滚、cursor 持久化
- `wallet.withdraw`：Spring Statemachine 配置、状态机推进 handler 集合、加速/取消流程
- `wallet.riskbridge` + `risk` 模块的钱包侧规则（额度 / 频控 / 黑名单 / 大额人工复核）

**完成判据**：
- 在 anvil 本地节点上跑通：用户充值 ETH → 扫块入账 → 用户提现 ETH → 风控放行 → 签名广播 → 链上确认 → 账本结算
- 提现加速（同 nonce 高 gas 替换）流程通过测试
- reorg 注入测试：手动让 anvil 回滚 3 块，已入账记录被反向冲账
- ERC20（USDT-ERC20）走完同样流程

## Plan 3 — 归集 + 冷热分层 + 对账

**交付件路径**：`docs/superpowers/plans/<date>-plan3-sweep-treasury-reconcile.md`

**职责范围**：
- `wallet.sweep`：归集状态机（PENDING → DRIPPING → DRIP_DONE → SIGNING → BROADCASTED → CONFIRMED）、ETH drip 模式、归集触发器（地址余额阈值 + 批量阈值）
- `wallet.treasury`：treasury_policy + TreasuryMonitor（水位监控）、自动入冷流程、出冷多签流程占位（PSBT/Gnosis Safe 接口预留）、提现资金不足时触发紧急 COLD_TO_HOT
- `wallet.reconcile`：三层对账 Job（账本自洽 / 账本余额 / 账本 vs 链上）、reconcile_report 落表、delta 处置

**完成判据**：
- 模拟 50 个 ETH 充值地址各 0.01 ETH 的场景，跑通归集到主热钱包
- 跑一次 reconcile job，输出 reconcile_report，三层校验都 PASS
- 故意制造账本与链上偏差（绕过 LedgerService 直接改 account），reconcile job 能正确报 WARN/FATAL

## Plan 4 — BTC 端到端

**交付件路径**：`docs/superpowers/plans/<date>-plan4-btc-e2e.md`

**职责范围**：
- `wallet.chain.btc`：bitcoinj 实现五大 SPI、UTXO selection（branch-and-bound）、PSBT 构造、UTXO 锁（Redis SETNX）
- BTC scanner（vout 多输出 / coinbase / OP_RETURN 过滤）+ BTC reorg 处理
- BTC withdraw 路径（多 input + RBF 加速 + UTXO 合并归集）
- BTC FeeStrategy（estimatesmartfee + RBF 自动加速）

**完成判据**：
- 在 bitcoind regtest 上跑通充值入账 / 提现广播 / 多用户归集（一笔 tx 多 input）
- RBF 加速：故意低 fee 提现卡 30 分钟，自动 RBF 重广播

## Plan 5 — TRON 端到端

**交付件路径**：`docs/superpowers/plans/<date>-plan5-tron-e2e.md`

**职责范围**：
- `wallet.chain.tron`：trident-java 实现五大 SPI、TRC20 合约调用解析、fee_payer 模式、energy 经济模型
- TRON scanner（TriggerSmartContract 解析 / TRC10 过滤）
- TRON FeeStrategy（energy 估算 + bandwidth 计算 + 主账户质押监控）
- TRON sweep：fee_payer 优先、降级 drip

**完成判据**：
- 在 Tron Nile 测试网跑通 TRX / TRC20（USDT-TRC20）充提
- fee_payer 模式归集：充值地址零余额时仍能成功转出 TRC20

## Plan 间依赖

- Plan 1 是所有后续 Plan 的硬前置
- Plan 2 必须在 Plan 3/4/5 之前（Plan 3 需要 ETH 充提跑通才能演示归集；Plan 4/5 沿用 Plan 2 建立的 scanner/withdraw 框架）
- Plan 3 可以与 Plan 4/5 并行（互不依赖）
- Plan 4、Plan 5 互不依赖
