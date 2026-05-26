# CEX 钱包系统设计（exchangeInfra · wallet 模块）

- 日期：2026-05-26
- 范围：在现有 exchangeInfra 单体多模块脚手架（Spring Boot 3.3.5 + Java 21）上，落地一套生产级 CEX 钱包子系统，覆盖 BTC、ETH/ERC20、TRON/TRC20 三链，含钱包侧风控
- 状态：待实施

## 1. 目标与边界

### 1.1 目标

- 在 `wallet` 顶层模块内部，按"链抽象 + 冷热分层 + 签名隔离 + 提现状态机 + 钱包风控"五个生产维度落地，能作为简历项目对外讲解
- 公链覆盖：BTC（UTXO）、ETH 主网原生币与 ERC20、TRON 主网原生币与 TRC20，三种主流模型全覆盖
- 密钥托管：HD 热钱包（BIP32/BIP39/BIP44）+ AES-GCM 加密 + KmsProvider 抽象（本地实现走 keystore，预留 AWS KMS / 阿里云 KMS 接入点）；冷钱包多签预留接入点
- 风控：仅覆盖钱包相关风险（额度 / 频控 / 黑名单地址 / 大额人工复核），不涉及交易侧风控
- 节点接入：本地/私有节点为主（bitcoind regtest、anvil、java-tron 或 Tron Nile 测试网）

### 1.2 非目标

- 不实现冷钱包多签离线签名工具链（仅在接口预留，treasury 子包按"多签收集"语义对接）
- 不集成 MPC/TSS 库（仅在 `Signer` 接口处预留实现位）
- 不接入第三方 AML/KYT 服务（如 TRM / Chainalysis），但 `RiskGuard` 抽象允许后续接入
- 不覆盖交易（撮合）侧风控
- 不引入 Camunda / Activiti 工作流引擎（论证见 §5）
- 不实现 ETH internal transactions 解析（仅在 `wallet.chain.eth` 留 `InternalTxScanner` 接口位）
- 不实现 PoR（储备金证明）/ Apollo 配置中心 / OpenTelemetry tracing（在 §12 提一笔为加分项）

## 2. 总体架构

### 2.1 模块结构

`wallet` 顶层模块内部按 Java 子包做强分层（不拆 Maven 子模块，保持单 jar）；`common` 顶层模块内部新增 `mq` 子包承载 MQ 基础设施（也不拆 Maven 子模块，避免侵入现有所有业务模块的 pom 依赖）。

```
exchangeInfra/
├── common/                                 # 现有 Maven 模块；内部新增 mq 子包
│   └── src/main/java/com/exchange/common/
│       ├── api/                            # 现有：响应封装
│       ├── config/                         # 现有：Jackson、Mybatis、Swagger
│       ├── constant/                       # 现有
│       ├── exception/                      # 现有：BizException、GlobalExceptionHandler
│       ├── security/                       # 现有：JwtUtil
│       ├── util/                           # 现有：JsonUtil、DateUtil、SnowflakeIdGenerator
│       └── mq/                             # 新增：Kafka 抽象 + Outbox + 幂等消费
├── wallet/                                 # 现有 Maven 模块；内部按子包分层
│   └── src/main/java/com/exchange/wallet/
│       ├── core/                           # 账户、余额、流水、冻结、对账（链无关）
│       ├── chain/api/                      # 链抽象 SPI 与 DTO
│       ├── chain/btc/                      # bitcoinj 实现
│       ├── chain/eth/                      # web3j 实现
│       ├── chain/tron/                     # trident-java 实现
│       ├── signer/                         # HD 派生 + AES-GCM + KmsProvider + Signer 实现
│       ├── nonce/                          # NonceAllocator: 并发安全的 nonce/UTXO 分配
│       ├── fee/                            # FeeStrategy: 链特化手续费估算（EIP-1559 / sat/vB / TRON energy）
│       ├── scanner/                        # 区块/交易扫描 + reorg 检测回滚
│       ├── withdraw/                       # 提现状态机（Spring Statemachine）
│       ├── sweep/                          # 归集状态机（用户地址 → 主热钱包）
│       ├── treasury/                       # 冷热分层水位 + 出冷/入冷调度
│       ├── reconcile/                      # 链上 vs 链下对账作业
│       └── riskbridge/                     # 防腐层，对接 risk 顶层模块
├── risk/                                   # 现有顶层模块：钱包侧风控规则、黑名单、额度、人工复核
├── trade/ user/ market/ admin/ bootstrap/  # 现有
└── infra/                                  # 现有 docker-compose（MySQL/Redis/Kafka）+ 新增 bitcoind/anvil 节点编排
```

子包间依赖通过 Spring Bean 装配 + 显式 import 约束体现，不依赖 Maven 模块边界。该约束在代码评审中强制（约定：`wallet.core` 包不允许 import `wallet.chain.btc/eth/tron`、`wallet.signer`、`wallet.scanner`）；后续可用 ArchUnit 测试自动校验。

### 2.2 子包依赖方向（强约束）

```
                      wallet.chain.api  （纯接口 + DTO，零链 SDK 依赖）
                          ▲       ▲       ▲       ▲
            ┌─────────────┘       │       │       └──────────────┐
   wallet.chain.btc      wallet.chain.eth      wallet.chain.tron     wallet.scanner
       │ bitcoinj             │ web3j                │ trident          │
       └──────────────────────┴──────────────────────┘                  │
                                                                        │
   wallet.signer ── 依赖 chain.api 的 RawTx/SignedTx                     │
       ▲                                                                │
       │                                                                │
   wallet.withdraw ──► wallet.core ◄────────────────────────────────────┘
       │                  ▲
       ▼                  │
   wallet.riskbridge      │
       │                  │
       ▼                  │
   risk                   │
                          │
                    user (校验 userId/KYC level)
```

强制约束：

- `wallet.core` 不依赖任何 `chain.*`、`signer`、`scanner`，保持业务层链无关
- 三个 `chain.*` 实现互不依赖
- `wallet.withdraw` 不直接依赖具体链实现，只通过 `chain.api` 接口拿
- 唯一接触私钥的子包是 `wallet.signer`，对外只暴露 `Signer.sign(rawTx, keyRef)` 一个出口
- 链实现通过 `@ConditionalOnProperty(name="wallet.chain.<chain>.enabled")` 开关装载

### 2.3 物理部署形态

- 单 JVM 启动（保留现有 bootstrap 聚合启动模式）
- 不拆微服务；模块间通过 Spring Bean 直调（同步路径）或 Kafka 事件（异步解耦路径）通信

## 3. common-mq —— 公共消息基础设施

### 3.1 设计目标

- 业务模块代码不直接接触 spring-kafka 的 `KafkaTemplate` / `@KafkaListener`
- 业务模块发消息只用 `TransactionalEventPublisher.publish(event)` 一行
- 业务模块消费消息只需继承 `IdempotentEventHandler<T>`
- 将来更换 MQ 实现（RocketMQ / Pulsar）业务侧零改动

### 3.2 对外抽象

```java
public interface DomainEvent {
    String eventId();        // 全局唯一，雪花 ID
    String aggregateId();    // Kafka partition key，保证同一聚合根有序
    String eventType();      // 命名规范：{module}.{aggregate}.{action}
    long occurredAt();
}

public interface TransactionalEventPublisher {
    void publish(DomainEvent event); // 必须在业务 DB 事务内调用，写 outbox 表
}

public interface EventPublisher {
    void publish(DomainEvent event); // 即时发送，用于不需要强一致的场景
}

public abstract class IdempotentEventHandler<T extends DomainEvent> {
    public final void onMessage(T event) {
        if (consumedRecordStore.exists(event.eventId(), handlerName())) return;
        try {
            handle(event);
            consumedRecordStore.markConsumed(event.eventId(), handlerName());
        } catch (RetriableException e) { throw e; }
          catch (Exception e) { dlqService.send(event, e); }
    }
    protected abstract void handle(T event);
    protected abstract String handlerName();
}
```

### 3.3 Outbox 与幂等表（属于 common-mq）

```sql
CREATE TABLE outbox (
  id            BIGINT PRIMARY KEY,
  event_id      VARCHAR(64) NOT NULL UNIQUE,
  topic         VARCHAR(64) NOT NULL,
  partition_key VARCHAR(64),
  payload       MEDIUMTEXT NOT NULL,
  status        TINYINT NOT NULL,            -- 0=待发, 1=已发
  retry_count   INT DEFAULT 0,
  next_retry_at DATETIME(3),
  created_at    DATETIME(3),
  KEY idx_status_next (status, next_retry_at)
);

CREATE TABLE consumed_record (
  event_id     VARCHAR(64) NOT NULL,
  handler_name VARCHAR(128) NOT NULL,
  consumed_at  DATETIME(3),
  PRIMARY KEY (event_id, handler_name)
);
```

`OutboxRelay`：独立线程，ShedLock 单实例锁，`SELECT ... WHERE status=0 ORDER BY id LIMIT 500` 批量投递 Kafka，成功置 1，失败按指数退避更新 `next_retry_at` 与 `retry_count`。

### 3.4 可观测性

common-mq 内统一提供 Prometheus 指标：

- `mq_outbox_pending` / `mq_outbox_failed_total`
- `mq_consumer_lag{topic, group}`
- `mq_idempotent_hit_total{handler}`（重复消息被幂等闸拦下的次数）
- `mq_dlq_total{topic}`

### 3.5 Topic 与命名规范

- 命名：`{module}.{aggregate}.{action}`，例：`wallet.withdraw.signed`、`wallet.deposit.confirmed`
- Partition key：`aggregateId`，对提现是 `orderId`，对充值是 `userId`
- 消息格式：JSON，统一带 `schemaVersion` 头

## 4. 数据模型

业务表分四组共 10 张（不含 common-mq 的 outbox / consumed_record）。金额字段统一 `DECIMAL(38,18)`，业务层按 coin 配置 scale 校验。

### 4.1 资产配置

```sql
CREATE TABLE coin (
  id        BIGINT PRIMARY KEY,
  symbol    VARCHAR(16) NOT NULL,        -- BTC / ETH / USDT
  chain     VARCHAR(16) NOT NULL,        -- BTC / ETH / TRON
  contract  VARCHAR(128),                -- 合约地址，原生币为 NULL
  decimals  INT NOT NULL,
  status    TINYINT NOT NULL,            -- 1=启用 0=停用
  UNIQUE KEY uk_symbol_chain (symbol, chain)
);

CREATE TABLE chain_config (
  id                BIGINT PRIMARY KEY,
  chain             VARCHAR(16) NOT NULL,
  deposit_confirms  INT NOT NULL,         -- 充值确认数 (BTC=3 ETH=12 TRON=20)
  withdraw_confirms INT NOT NULL,
  reorg_depth       INT NOT NULL,         -- 扫块器安全深度
  min_withdraw      DECIMAL(38,18) NOT NULL,
  max_withdraw      DECIMAL(38,18) NOT NULL,
  fee_strategy      VARCHAR(32) NOT NULL, -- FIXED / DYNAMIC_EIP1559 / TRON_ENERGY
  UNIQUE KEY uk_chain (chain)
);
```

### 4.2 地址与密钥

```sql
CREATE TABLE wallet_address (
  id          BIGINT PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  chain       VARCHAR(16) NOT NULL,
  address     VARCHAR(128) NOT NULL,
  hd_path     VARCHAR(64) NOT NULL,        -- 例 m/44'/60'/0'/0/123
  key_id      VARCHAR(64) NOT NULL,        -- 关联 key_material.key_id
  status      TINYINT NOT NULL,            -- 1=可用 0=回收
  created_at  DATETIME(3),
  UNIQUE KEY uk_chain_addr (chain, address),
  KEY idx_user_chain (user_id, chain)
);

CREATE TABLE hd_path (
  id        BIGINT PRIMARY KEY,
  chain     VARCHAR(16) NOT NULL,
  hd_path   VARCHAR(64) NOT NULL,
  used_at   DATETIME(3),
  UNIQUE KEY uk_chain_path (chain, hd_path)
);

CREATE TABLE key_material (
  id              BIGINT PRIMARY KEY,
  key_id          VARCHAR(64) NOT NULL UNIQUE,
  key_type        VARCHAR(16) NOT NULL,    -- HD_SEED / SINGLE
  cipher_text     MEDIUMBLOB NOT NULL,     -- AES-GCM 密文
  iv              VARBINARY(32) NOT NULL,
  kms_alias       VARCHAR(128) NOT NULL,   -- 解密时指明用哪个 KMS Key（本地实现 = local:default）
  algo_version    INT NOT NULL DEFAULT 1,  -- 加密算法版本，便于轮换
  created_at      DATETIME(3)
);
```

### 4.3 账户与流水（双账法，参见 §6.1）

```sql
CREATE TABLE account (
  id          BIGINT PRIMARY KEY,
  user_id     BIGINT NOT NULL,             -- 系统侧账户用约定 userId（如 -1）
  coin_id     BIGINT NOT NULL,
  available   DECIMAL(38,18) NOT NULL DEFAULT 0,
  frozen      DECIMAL(38,18) NOT NULL DEFAULT 0,
  version     INT NOT NULL DEFAULT 0,      -- 乐观锁
  updated_at  DATETIME(3),
  UNIQUE KEY uk_user_coin (user_id, coin_id)
);

CREATE TABLE account_journal (
  id            BIGINT PRIMARY KEY,
  trace_id      VARCHAR(64) NOT NULL,      -- 同笔业务的两条记录共享
  account_id    BIGINT NOT NULL,
  coin_id       BIGINT NOT NULL,
  biz_type      VARCHAR(32) NOT NULL,      -- DEPOSIT / WITHDRAW_FREEZE / WITHDRAW_SETTLE / INTERNAL_TRANSFER / FEE
  biz_id        BIGINT NOT NULL,
  direction     TINYINT NOT NULL,          -- 1=credit  -1=debit
  amount        DECIMAL(38,18) NOT NULL,
  balance_after DECIMAL(38,18) NOT NULL,   -- 操作后可用余额快照（对账锚点）
  remark        VARCHAR(255),
  created_at    DATETIME(3),
  UNIQUE KEY uk_trace_direction (trace_id, direction),  -- 双账幂等闸
  KEY idx_account_time (account_id, created_at)
);
```

### 4.4 链上交易快照

```sql
CREATE TABLE chain_tx (
  id            BIGINT PRIMARY KEY,
  chain         VARCHAR(16) NOT NULL,
  tx_hash       VARCHAR(128) NOT NULL,
  vout          INT NOT NULL DEFAULT 0,    -- BTC 输出序号；ETH/TRON 固定 0
  block_height  BIGINT NOT NULL,
  block_hash    VARCHAR(128) NOT NULL,
  parent_hash   VARCHAR(128) NOT NULL,     -- reorg 检测用
  from_address  VARCHAR(128),
  to_address    VARCHAR(128) NOT NULL,
  coin_id       BIGINT,
  amount        DECIMAL(38,18) NOT NULL,
  direction     TINYINT NOT NULL,          -- 1=入站 0=我方提现出站
  confirm_count INT DEFAULT 0,
  status        TINYINT NOT NULL,          -- 0=PENDING 1=CONFIRMED 2=ORPHANED(reorg失效) 3=DROPPED(链上回退)
  raw_json      MEDIUMTEXT,                -- 节点原始返回，审计/回放用
  created_at    DATETIME(3),
  UNIQUE KEY uk_chain_hash_vout (chain, tx_hash, vout),
  KEY idx_to_addr (chain, to_address),
  KEY idx_chain_height (chain, block_height)
);
```

### 4.5 业务流程

```sql
CREATE TABLE deposit_order (
  id            BIGINT PRIMARY KEY,
  user_id       BIGINT NOT NULL,
  coin_id       BIGINT NOT NULL,
  chain_tx_id   BIGINT NOT NULL,
  amount        DECIMAL(38,18) NOT NULL,
  status        VARCHAR(32) NOT NULL,      -- PENDING / CONFIRMED / CREDITED / FAILED
  confirm_count INT DEFAULT 0,
  version       INT DEFAULT 0,
  created_at    DATETIME(3),
  updated_at    DATETIME(3),
  UNIQUE KEY uk_chain_tx (chain_tx_id),
  KEY idx_user_status (user_id, status)
);

CREATE TABLE withdraw_order (
  id              BIGINT PRIMARY KEY,
  user_id         BIGINT NOT NULL,
  coin_id         BIGINT NOT NULL,
  chain           VARCHAR(16) NOT NULL,
  to_address      VARCHAR(128) NOT NULL,
  amount          DECIMAL(38,18) NOT NULL,
  fee             DECIMAL(38,18) NOT NULL,
  fee_estimate    DECIMAL(38,18),          -- 估算 fee（建单时）；fee 字段为实际入账 fee
  status          VARCHAR(32) NOT NULL,
  fail_reason     VARCHAR(255),
  risk_decision   VARCHAR(32),             -- PASS / REJECT / MANUAL
  signed_raw      MEDIUMTEXT,              -- 签名后 rawTx hex，签后回填
  tx_hash         VARCHAR(128),            -- 广播后回填
  nonce           BIGINT,                  -- ETH/TRON: 分配的 nonce / 序列号
  from_address    VARCHAR(128),            -- 主热钱包出金地址（建单时锁定）
  replace_of_id   BIGINT,                  -- 加速/取消时指向被替代的订单
  confirm_count   INT DEFAULT 0,
  version         INT NOT NULL DEFAULT 0,
  created_at      DATETIME(3),
  updated_at      DATETIME(3),
  KEY idx_status_time (status, created_at),
  KEY idx_user (user_id, status),
  KEY idx_chain_from_nonce (chain, from_address, nonce),
  UNIQUE KEY uk_tx_hash (tx_hash)
);

CREATE TABLE sweep_order (
  id            BIGINT PRIMARY KEY,
  chain         VARCHAR(16) NOT NULL,
  coin_id       BIGINT NOT NULL,
  src_address   VARCHAR(128) NOT NULL,     -- 用户充值地址
  dst_address   VARCHAR(128) NOT NULL,     -- 主热钱包归集地址
  amount        DECIMAL(38,18) NOT NULL,
  status        VARCHAR(32) NOT NULL,      -- PENDING / DRIPPING / DRIP_DONE / SIGNING / BROADCASTED / CONFIRMED / FAILED
  drip_tx_hash  VARCHAR(128),              -- ETH/TRON 给充值地址打 gas 的 tx
  sweep_tx_hash VARCHAR(128),              -- 归集本身的 tx
  nonce         BIGINT,                    -- 归集 tx 的 nonce（充值地址侧）
  retry_count   INT DEFAULT 0,
  version       INT NOT NULL DEFAULT 0,
  created_at    DATETIME(3),
  updated_at    DATETIME(3),
  KEY idx_chain_status (chain, status),
  UNIQUE KEY uk_sweep_tx (sweep_tx_hash)
);

CREATE TABLE treasury_movement (
  id            BIGINT PRIMARY KEY,
  chain         VARCHAR(16) NOT NULL,
  coin_id       BIGINT NOT NULL,
  direction     VARCHAR(16) NOT NULL,      -- HOT_TO_COLD（入冷）/ COLD_TO_HOT（出冷）
  amount        DECIMAL(38,18) NOT NULL,
  status        VARCHAR(32) NOT NULL,      -- PROPOSED / SIGNED / BROADCASTED / CONFIRMED / FAILED
  psbt          MEDIUMTEXT,                -- BTC: PSBT；ETH: Safe txHash + 多签收集状态
  tx_hash       VARCHAR(128),
  proposer      VARCHAR(64),               -- admin 用户名
  approver_list VARCHAR(512),              -- 多签批准人 JSON
  created_at    DATETIME(3),
  updated_at    DATETIME(3),
  KEY idx_status (status)
);

CREATE TABLE nonce_register (
  chain         VARCHAR(16) NOT NULL,
  address       VARCHAR(128) NOT NULL,
  next_nonce    BIGINT NOT NULL,           -- 下一个待分配的 nonce
  on_chain_nonce BIGINT NOT NULL,          -- 上次校准时链上 pending nonce
  reconciled_at DATETIME(3),               -- 上次校准时间
  version       INT NOT NULL DEFAULT 0,    -- 乐观锁
  PRIMARY KEY (chain, address)
);

CREATE TABLE address_balance (              -- 地址级链上余额快照（归集触发器 + 对账）
  chain         VARCHAR(16) NOT NULL,
  address       VARCHAR(128) NOT NULL,
  coin_id       BIGINT NOT NULL,
  balance       DECIMAL(38,18) NOT NULL,
  block_height  BIGINT NOT NULL,
  refreshed_at  DATETIME(3),
  PRIMARY KEY (chain, address, coin_id)
);
```

## 5. 工作流引擎选型论证

不引入 Camunda / Activiti，理由：

- 提现状态机仅 8 个线性状态，无动态流程变更需求
- 工作流引擎自带的事务上下文与本系统 `双账法 + outbox` 的事务边界冲突，会污染强一致保证
- Camunda / Activiti 默认 25+ 张引擎表，引入额外运维成本，对应面试讲解负担也大

代之以：

- 状态表达：Spring Statemachine（轻量，DSL 配置状态/事件/守卫，无引擎表）
- 状态推进：Kafka 事件驱动（主路径） + ShedLock 兜底定时（巡检超时订单）

## 6. 核心机制

### 6.1 双账法（复式记账）

每笔资金移动产生两条 `account_journal` 记录，方向相反、金额相等、`trace_id` 相同：

- 用户充值：系统中间账户 debit -amount，用户账户 credit +amount
- 用户提现冻结：用户可用账户 debit -amount，用户冻结账户 credit +amount
- 提现确认结算：用户冻结账户 debit -amount，系统中间账户 credit +amount
- 站内划转：A 账户 debit，B 账户 credit

不变量：`SELECT SUM(direction * amount) FROM account_journal WHERE coin_id = ?` 永远 = 0，是对账脚本的核心检查项。

幂等闸：`uk_trace_direction (trace_id, direction)`，重复执行同一笔业务不会重复扣账。

落地阶段策略：第一阶段用户侧 + 系统中间账户均记录（完整双账）；`account` 表通过约定 user_id（如 -1）建系统中间账户行。

### 6.2 提现状态机

状态：`INIT → RISK_CHECKING → SIGNING → BROADCASTING → BROADCASTED → CONFIRMED → SETTLED`，加 `REJECTED / FAILED` 终态。

事件流（事件驱动主路径，所有事件经 common-mq）：

```
Withdraw.apply()
  ├─同事务：withdraw_order(INIT) + ledger.freeze() + outbox 写 withdraw.init
  ▼
wallet.withdraw.init      → RiskCheckHandler  → 同事务：status=SIGNING 或 REJECTED + outbox
wallet.withdraw.risk_passed → SignHandler     → 同事务：sign 后 status=BROADCASTING + outbox
wallet.withdraw.signing_done → BroadcastHandler → 同事务：broadcast 后 status=BROADCASTED + outbox
wallet.withdraw.broadcast → (scanner WithdrawConfirmer 监听链上确认) → 推到 CONFIRMED
wallet.withdraw.confirmed → SettleHandler     → 同事务：ledger.settle() + status=SETTLED
```

幂等三道闸：

1. `withdraw_order` 乐观锁：`UPDATE ... WHERE id=? AND status=? AND version=?`
2. `withdraw_order.uk_tx_hash`：广播后落库，重复广播被 DB 拦截
3. `account_journal.uk_trace_direction`：重复结算被 DB 拦截

兜底巡检（ShedLock 单实例锁）：每分钟扫 `status IN ('SIGNING','BROADCASTING','BROADCASTED') AND updated_at < now()-5min`，重发对应事件，由幂等闸保证安全。

### 6.3 充值入账流程

```
chain-* 节点 → scanner 解析 → chain_tx 入库（uk 防重）→ 发 wallet.deposit.detected
            → DepositConfirmer 查 confirm_count → 达确认数 → 发 wallet.deposit.confirmed
            → DepositCreditHandler 同事务：deposit_order=CREDITED + ledger.credit() + outbox 发 wallet.account.credited
```

trade 模块订阅 `wallet.account.credited` 更新可用资金。

### 6.4 扫块器

```java
public abstract class AbstractScanner {
    @Scheduled(fixedDelay = 3000)
    public void scan() {
        long cursor = cursorStore.read(chain());
        long latest = chainClient.getLatestHeight();
        long safeHeight = latest - chainConfig.reorgDepth();
        for (long h = cursor + 1; h <= safeHeight; h++) {
            Block block = chainClient.getBlock(h);
            for (ChainTx tx : parser.parse(block)) {
                if (!addressIndex.isOurs(tx.toAddress, chain())) continue;
                chainTxMapper.insertIgnore(tx);
                txEventPublisher.publish(new DepositDetectedEvent(tx));
            }
            cursorStore.save(chain(), h);
        }
    }
    abstract String chain();
}
```

要点：

- `safeHeight = latest - reorgDepth` 兜底区块重组（BTC=6 / ETH=12 / TRON=20）
- 子类按链实现 `parser`：BTC 解析 vout 列表，ETH 解析 Transfer Log，TRON 解析 TriggerSmartContract
- `chain_tx` UNIQUE KEY 是天然幂等闸，重启重扫安全

### 6.5 签名隔离

- `Signer.sign(rawTx, keyRef)` 是私钥使用的唯一出口
- 内部流程：`KmsProvider.decrypt(cipher) → 内存中签名 → 私钥 byte[] 主动清零（Arrays.fill(key,(byte)0)）`
- 私钥不出 `wallet.signer` 子包；不写日志、不进异常 message
- KmsProvider 抽象：本地实现读取 keystore，预留 AWS KMS / 阿里云 KMS / HSM PKCS#11 实现位
- 算法版本字段 `key_material.algo_version` 支持密钥轮换

### 6.6 钱包风控（risk 模块 + riskbridge）

`riskbridge` 在 wallet 内定义 `RiskGuard.check(WithdrawCheckCtx)`，返回 `PASS / REJECT / MANUAL`。`risk` 模块实现：

- 单笔/日累计/月累计额度
- 频次限制（同地址、同用户）
- 黑名单地址（本地维护 + 接口预留外部 AML 服务）
- 大额阈值触发人工复核（`MANUAL` → `withdraw_order.status=MANUAL_REVIEW`，admin 后台审批后回写事件）

### 6.7 Nonce / 序列号管理

ETH/TRON 同一出金地址的并发交易必须严格按"分配号"顺序进入 mempool；BTC 没有 nonce，但 UTXO selection 也是同类问题（同一 UTXO 不能并发花两次）。统一在 `wallet.nonce.NonceAllocator` 中处理：

```java
public interface NonceAllocator {
    long allocate(String chain, String fromAddress);   // 分配下一个 nonce
    void release(String chain, String fromAddress, long nonce);  // 失败回收（仅当 tx 未广播）
    void reconcile(String chain, String fromAddress);  // 与链上 pending nonce 校准
}
```

**ETH/TRON 实现**：
- 单进程内基于 `nonce_register` 表 `UPDATE next_nonce = next_nonce + 1 WHERE chain=? AND address=? AND version=?` 乐观锁分配
- 多实例并发用 Redis Lua 脚本 `INCR + 校准` 兜底（脚本内同时读 DB version + Redis counter，差异超阈值告警）
- 启动时与每 100 笔签名后 `eth_getTransactionCount(pending)` 校准 `next_nonce`
- 失败回收只对"分配后未广播"有效；"广播后失败"必须填补（用 `replace_of_id` 发同 nonce 替换交易）

**Nonce gap 处理**：
- 卡住的 tx（gas 太低）→ 加速：同 nonce + 高 gas + 新签名，新建一笔 `withdraw_order(replace_of_id=旧id)`，旧订单进 `REPLACED` 终态
- 取消：同 nonce + 转给自己 + 足够 gas，专用工具流程，admin 触发

**BTC 实现**：
- UTXO selection 用 branch-and-bound（bitcoinj 内置 `CoinSelector`），同一 UTXO 在 `utxo_lock`（Redis SETNX，TTL 5 分钟）锁定后才进交易
- 双花在 mempool 层就会被节点拒绝，依赖节点检查兜底

**面试覆盖点**：并发分配 / 启动校准 / 卡住交易加速取消 / 多实例一致性。

### 6.8 归集流程（Sweep）

用户充值落到 `wallet_address` 表的散点地址，必须归集到主热钱包才能用于出金。归集状态机由 `wallet.sweep.SweepStateMachine` 驱动：

**触发条件**（`SweepTrigger` 定时扫，每 10 分钟一次）：
- `address_balance.balance >= sweep_threshold`（按 coin 配置，例：USDT-ERC20=100、BTC=0.01）
- 且充值地址数量超过 `batch_min_count`（避免频繁打 gas，浪费手续费）

**状态机**（`sweep_order`）：
```
PENDING → DRIPPING（ETH/TRON 给充值地址打 gas，BTC 跳过）
        → DRIP_DONE → SIGNING → BROADCASTED → CONFIRMED
        → 链上确认后调 ledger.transferInternal()（系统中间账户内部划转）
```

**ETH/ERC20 归集（drip 模式）**：
- 主热钱包先发 `eth_sendTransaction` 给充值地址打"刚好够 gas 的 ETH"（drip）
- drip 上链后，从充值地址发 ERC20 `transfer` 到主归集地址
- drip 金额需精算（避免给充值地址留余额，否则一直触发归集）

**TRON/TRC20 归集（fee_payer 模式优先）**：
- 主账户质押 TRX 获取 energy（不消耗 TRX）
- 用 `fee_payer` 由主账户代付 energy/bandwidth，充值地址零余额也能转出 TRC20
- fee_payer 不可用时降级到 drip 模式

**BTC 归集**：
- 一笔 tx 多 input：把 N 个充值地址的 UTXO 合并到主归集地址输出
- bitcoinj 多 input PSBT，每个 input 独立用对应地址私钥签名（多次调 `Signer.sign`）
- fee 平摊到 input 数量上，越多越便宜

**与提现的资金关系**：归集 tx confirmed 后，`treasury.HotWalletBalanceService` 增加主热钱包余额（仅链上视图），账本侧由 `LedgerService.transferInternal(系统散点中间账户 → 系统主热钱包账户)` 双账法双写。

**面试覆盖点**：drip / fee_payer / UTXO 合并 / gas 经济性 / 归集失败重试 / 与提现的资金衔接。

### 6.9 链上对账（不同于双账法的内部对账）

双账法保证账本"自洽"，链上对账保证账本与链上"一致"。`wallet.reconcile.ReconcileJob` 每天凌晨跑一次，按 coin 维度三层校验：

**第一层：账本自洽**
```sql
SELECT coin_id, SUM(direction * amount) FROM account_journal GROUP BY coin_id;
-- 任何一行 != 0 → 严重告警，停机调查
```

**第二层：账本余额 = journal 累加**
```sql
-- 对每个 account 做：account.available - SUM(journal where account_id=该行) = 0
```
不一致 → 余额被脏写，定位 bug。

**第三层：账本 = 链上**
```
LinkedAddresses = wallet_address 全集 + 主热钱包地址 + 系统中间账户对应链上地址
chainBalance = Σ chainClient.getBalance(addr, coin) for addr in LinkedAddresses
ledgerBalance = Σ account.available + Σ account.frozen for coin
delta = chainBalance - ledgerBalance
```

**delta 处理**：
- `delta > 0`（链上多）：可能是未识别的充值（陌生人转账、新合约）或扫块器漏处理。补扫 `chain_tx` 找差异 → 自动入账 `unknown_inflow` 账户 → 人工核验
- `delta < 0`（链上少）：严重告警，可能是账本脏写、密钥泄漏、扫块器误判。停机调查
- `|delta| < epsilon`（手续费抖动等）：纳入容忍

**对账结果落表**：
```sql
CREATE TABLE reconcile_report (
  id            BIGINT PRIMARY KEY,
  report_date   DATE NOT NULL,
  chain         VARCHAR(16) NOT NULL,
  coin_id       BIGINT NOT NULL,
  ledger_total  DECIMAL(38,18),
  chain_total   DECIMAL(38,18),
  delta         DECIMAL(38,18),
  status        VARCHAR(16),               -- OK / WARN / FATAL
  detail_json   MEDIUMTEXT,
  created_at    DATETIME(3),
  UNIQUE KEY uk_date_chain_coin (report_date, chain, coin_id)
);
```

**面试覆盖点**：双账法 vs 链上对账的语义差异 / 三层校验顺序 / delta 正负不同处置 / 跑批失败如何重入。

### 6.10 手续费策略（一链一策）

`wallet.fee.FeeStrategy` 接口，每条链一个实现：

```java
public interface FeeStrategy {
    String chain();
    FeeQuote quote(FeeQuoteRequest req);   // 返回估算 fee + 链特化参数
}
```

**ETH（EIP-1559）**：
- `eth_feeHistory(20, "latest", [25,50,75])` 取最近 20 块 baseFee 中位数 + 50% 分位 priorityFee
- `maxFeePerGas = baseFee * 2 + priorityFee`（2x 头寸防 baseFee 飙升）
- `maxPriorityFeePerGas = priorityFee`
- gasLimit：原生币转账 21000；ERC20 转账 65000（精确值通过 `eth_estimateGas` 实测）
- 上限保护：`maxFeePerGas` 超过 `chain_config.max_gas_price_gwei` 时拒绝出单（防极端拥堵把整笔提现烧没）
- 用户提现的 fee 从用户应收金额内扣：`实际到账 = amount - fee`

**BTC（sat/vB）**：
- 取 `estimatesmartfee 6`（6 块内确认）作为常规档；高优档用 `estimatesmartfee 1`
- UTXO selection：bitcoinj `BranchAndBoundCoinSelector`，最小化变更（dust 优化）
- 估算 vsize：input 数量 × 68 + output 数量 × 31 + overhead，乘 sat/vB 得 fee
- RBF：交易未确认 30 分钟自动 RBF 加速（fee +20%，重新签名广播）

**TRON（energy + bandwidth）**：
- TRX 转账：纯 bandwidth，每字节 1000 sun，主账户质押 TRX 获取免费 bandwidth
- TRC20 转账：energy + bandwidth；energy 通过 `triggerconstantcontract` 估算 + 30% 余量
- 主账户质押 TRX 获取 energy 远便宜于直接烧 TRX，定时 job 监控质押率
- fee 计算最终落到 SUN（1 TRX = 10^6 SUN）

**面试覆盖点**：EIP-1559 vs Legacy / RBF / TRON energy 经济模型 / 极端拥堵保护 / 估算误差兜底（实际 > 估算时谁兜底）。

### 6.11 冷热分层资金管理

`wallet.treasury` 子包统一管理热钱包水位与冷热互转：

**水位策略**（按 coin 配置）：
```sql
CREATE TABLE treasury_policy (
  id                BIGINT PRIMARY KEY,
  chain             VARCHAR(16) NOT NULL,
  coin_id           BIGINT NOT NULL,
  hot_low_ratio     DECIMAL(5,4),          -- 0.30 触发出冷
  hot_high_ratio    DECIMAL(5,4),          -- 0.70 触发入冷
  hot_target_ratio  DECIMAL(5,4),          -- 0.50 互转目标比例
  total_target      DECIMAL(38,18),        -- 总头寸目标（运营手动维护）
  daily_outflow_avg DECIMAL(38,18),        -- 日均出金（脚本统计后回写）
  UNIQUE KEY uk_chain_coin (chain, coin_id)
);
```

**TreasuryMonitor**（定时 5 分钟）：
- 计算热钱包当前比例 = `hot_balance / total_target`
- < `hot_low_ratio` → 创建 `treasury_movement(direction=COLD_TO_HOT)`，状态 `PROPOSED`，告警通知 admin 多签
- \> `hot_high_ratio` → 自动创建 `direction=HOT_TO_COLD`，无需多签直接走（钱往冷里送，安全）

**冷钱包多签流程**（仅 COLD_TO_HOT 走）：
- BTC：admin 后台构造 PSBT，下载到离线设备依次签名（2-of-3 或 3-of-5），合并后上传，由热钱包代理广播
- ETH：调用预部署的 Gnosis Safe 合约，多个签名收集后调用 `execTransaction`，由其中一个签名者广播

**热钱包资金占比经验**：日均出金 × 1.5-2 倍，对应 `hot_target_ratio = 0.5` 时 `total_target = 日均出金 × 3-4 倍`。

**与提现的联动**：提现建单时检查 `hot_balance < amount + reserve`，触发紧急 COLD_TO_HOT 流程，提现订单进入 `WAITING_TREASURY` 状态。

**面试覆盖点**：水位线设定依据 / 多签的 PSBT/Gnosis Safe 区别 / 自动入冷 vs 人工出冷 / 极端情况下提现暂停策略。

### 6.12 区块重组（Reorg）真实处理

`safeHeight = latest - reorgDepth` 是兜底，真实生产还要主动检测和回滚：

**主动检测**：每次扫块前校验链头一致性
```java
ChainTx lastStored = chainTxMapper.lastTxOnHeight(chain, h - 1);
Block currentBlock = chainClient.getBlock(h);
if (!currentBlock.parentHash.equals(lastStored.blockHash)) {
    handleReorg(chain, h);
    return;
}
```

**回滚流程**（`ReorgHandler`）：
- 从 h 倒着往前扫，找到分叉点（`block.hash == DB.block_hash` 的最大高度 h0）
- `(h0, h]` 区间内所有 `chain_tx` 标记 `status=ORPHANED`
- 关联的 `deposit_order`：
  - `status=PENDING/CONFIRMED` 但还没 CREDITED → 直接回滚到 PENDING 等待重扫
  - `status=CREDITED`（已入账）→ 反向冲账：`ledger.reverseCredit()` 写一条反向 journal 双账，`deposit_order.status=REVERSED`，告警通知用户与运营
- 关联的 `withdraw_order`（我方提现）：
  - `BROADCASTED/CONFIRMED` → 回到 `BROADCASTING`，重新等链上确认（同 tx_hash 通常会被重新打包）
- scanner cursor 回退到 h0

**超深重组兜底**：超过 `reorg_depth + safe_buffer` 的极端 reorg → 停机告警人工介入（PoW 链如 BTC 极端情况下可能发生 6+ 块重组）。

**面试覆盖点**：reorg 检测时机 / 已入账资金的反向冲账 / 提现是否需要重新签名 / 超深 reorg 的人工流程。

### 6.13 节点高可用

`wallet.chain.*` 每条链支持多 RPC endpoint：

```yaml
wallet.chain.eth:
  enabled: true
  endpoints:
    - { url: "http://geth-1:8545", weight: 3, role: PRIMARY }
    - { url: "http://geth-2:8545", weight: 2, role: SECONDARY }
    - { url: "https://mainnet.infura.io/v3/${KEY}", weight: 1, role: BACKUP }
  circuit-breaker:
    failure-rate-threshold: 50
    wait-duration-in-open-state: 60s
    sliding-window-size: 20
```

实现：
- `MultiRpcChainClient` 包装多个底层 `Web3j` 实例
- resilience4j `CircuitBreaker` 单 endpoint，失败率超阈值断路 60s
- 路由策略：权重轮询 + 健康优先，BACKUP 仅在 PRIMARY/SECONDARY 全断时启用
- 限流：每 endpoint `RateLimiter`，防触发上游配额
- 一致性校验：高频 read 调用（getBlockNumber）周期对比多 endpoint 返回值，差异超阈值告警（防恶意/被攻陷节点）

**面试覆盖点**：单点 → 多点 / 自建 vs 公有 RPC 互为备份 / 路由策略 / 节点不一致的检测。

### 6.14 特殊交易场景过滤（扫块器必处理）

ETH/TRON 实际链上数据远比"转账"复杂，scanner 必须过滤：

**ETH**：
- `tx.status == 0`：失败 tx，消耗 gas 但未转账，跳过
- ERC20 Transfer Log：通过 `eth_getLogs(topic0=0xddf252ad...)` 获取，按 `to_address` 入账
- internal transactions（合约调用产生的转账，如 multisig wallet 转出）：需要 `debug_traceBlockByHash` 或 `trace_block`（只支持 erigon/openethereum），生产大所必处理；本系统作为面试项目可在 `chain.eth` 留 `InternalTxScanner` 接口预留位
- 多 Transfer 同 tx：循环每条 log 各自入账，`chain_tx.vout` 用 logIndex 区分
- self-transfer（from == to）：跳过，不当充值
- 合约创建 tx：跳过

**TRON**：
- `Transaction.ret[0].contractRet != "SUCCESS"`：失败 tx 跳过
- TRC20：解析 `TriggerSmartContract` 的 `data` 前 4 字节为 `0xa9059cbb`（transfer 函数签名），后续 32 字节为 to，再 32 字节为 amount
- TRC10：另一套 ABI，本系统不支持（在配置层禁用）

**BTC**：
- coinbase tx：跳过
- 多 vout：每个 vout 独立成行，`chain_tx.vout` = output index
- OP_RETURN output：跳过
- segwit / taproot 地址识别：bitcoinj 已封装，但需校验地址格式 vs 链网络

**面试覆盖点**：失败交易过滤 / Transfer Log 解析 / internal tx / TRC20 ABI / coinbase 与 OP_RETURN。

## 7. 链抽象 SPI

`wallet.chain.api` 子包内定义如下接口与 DTO（`RawTx` / `SignedTx` / `ChainTx` / `Block` / `TransferRequest` / `TxStatus` / `KeyRef` / `DerivedAddress`）。

```java
public interface ChainClient {
    String chain();
    BigDecimal getBalance(String address, String coinSymbol);
    long getLatestHeight();
    Block getBlock(long height);
    TxStatus queryTxStatus(String txHash);
}

public interface TxBuilder {
    RawTx buildTransfer(TransferRequest req);    // 链无关入参
}

public interface TxBroadcaster {
    String broadcast(SignedTx signedTx);          // 返回 txHash
}

public interface TxParser {
    List<ChainTx> parse(Block block);             // 解析为统一 ChainTx
}

public interface AddressDerivator {
    DerivedAddress derive(String hdSeedRef, String hdPath);  // hdSeedRef 经 KmsProvider 解密
}

// 由 wallet.signer 子包实现，对 chain 实现透明
public interface Signer {
    SignedTx sign(RawTx rawTx, KeyRef keyRef);    // 内部：KMS 解密 → 签名 → 私钥清零
}
```

每条链实现 `ChainClient + TxBuilder + TxBroadcaster + TxParser + AddressDerivator`，注入到 Spring 容器后由 `wallet.core / wallet.withdraw / wallet.scanner` 通过 `chain` 字段路由到对应 bean。`Signer` 由 `wallet.signer` 提供唯一实现，对 chain 实现透明。

## 8. 错误处理与可观测性

### 8.1 错误分类与策略

- 链节点不可用 / 网络抖动：消费侧抛 `RetriableException`，Kafka 重投；连续失败超过阈值进 DLQ
- 签名失败 / 余额不足 / 风控拒绝：业务异常，状态机进入终态 `FAILED / REJECTED`，发事件释放冻结
- 数据冲突（乐观锁失败、UNIQUE 冲突）：视为已处理，幂等返回成功
- 区块重组：`safeHeight` 兜底；超出 reorgDepth 的重组触发告警人工介入

### 8.2 监控指标

业务：`wallet_deposit_credited_total{chain,coin}`、`wallet_withdraw_confirmed_total{chain,coin}`、`wallet_withdraw_failed_total{chain,reason}`、`wallet_balance_mismatch_total{coin}`（对账脚本告警）

基础设施（来自 common-mq）：见 §3.4

链相关：`chain_scanner_lag_blocks{chain}`、`chain_node_rpc_latency_seconds{chain,method}`

### 8.3 关键告警

- 双账不平衡（`SUM(direction * amount) != 0`）
- 扫块器水位停滞（cursor 5 分钟未推进）
- 提现卡在某状态超 10 分钟（兜底巡检数据）
- 风控拒绝率突增
- DLQ 消息积压

## 9. 测试策略

- 单元测试：`wallet-core` 的双账法事务、状态机守卫、幂等闸；mock chain-api
- 模块集成测试：用 testcontainers 起 MySQL + Kafka，覆盖 outbox + 消费 + 幂等
- 链集成测试：bitcoind regtest / anvil 容器，跑充提端到端
- 对账测试：构造充提混合事件流，跑完后断言双账平衡 + `account` 余额 = `journal` 累加值
- 故障注入：扫块器中断重启不丢账、签名节点挂掉重试不双花、Kafka 抖动消息不丢不重

## 10. 安全要点

- 私钥仅在 `wallet.signer` 内存中短暂出现，使用后立即清零；不写日志、不进异常
- `key_material.cipher_text` 用 AES-GCM；`iv` 每条独立；`algo_version` 支持轮换
- KMS Key 别名通过配置注入，本地走 keystore，生产替换为 KMS 服务（接口已抽象）
- 提现地址、金额、`tx_hash` 等关键字段在日志中脱敏
- HD 派生路径不复用：`hd_path` 表 UNIQUE KEY 防重；地址池预生成
- 审计：所有状态机推进与签名行为写专用审计日志（独立 topic + 独立存储）

## 11. 节点接入与 infra

- BTC：`bitcoind` regtest，docker compose 集成
- ETH：`anvil` 或 `geth --dev`，docker compose 集成
- TRON：默认接 Nile 测试网；可选 `java-tron` 本地节点（资源占用大）
- 节点 RPC 凭据通过环境变量注入；内置健康检查与 RPC 限流（resilience4j）

## 12. 未来扩展（接口已预留）

- MPC/TSS：替换 `Signer` 实现，业务侧零改动
- HSM：`KmsProvider` 增加 PKCS#11 实现
- 冷钱包多签：BTC P2WSH、ETH Gnosis Safe，预留 `MultiSigSigner` 接口（treasury 子包已使用）
- 第三方 AML：`RiskGuard` 内增加外部评分调用（TRM / Chainalysis），业务侧零改动
- 新增公链：复制 `wallet.chain.tron` 子包模板，实现 5 个 SPI 接口即可
- 拆微服务：将 `wallet.*` 子包拆出独立 jar，common-mq 已经是事件驱动，跨进程零改动
- ETH internal transactions：在 `wallet.chain.eth` 的 `InternalTxScanner` 接口位补实现（依赖 erigon trace API）
- 分布式 tracing：接入 OpenTelemetry，traceId 贯穿 apply→freeze→risk→sign→broadcast→confirm→settle 全链路
- 配置中心：风控规则 / 链开关 / 限额 / 水位策略迁移到 Apollo 或 Nacos 热配置
- 助记词分片备份：Shamir's Secret Sharing（5-of-3）做主助记词冷备
- 储备金证明（PoR）：Merkle Tree 公开用户余额聚合根 + 链上多签地址快照

## 13. 实施顺序（高层路径）

1. 在 `common` 模块内新增 `mq` 子包，含 outbox + 幂等表 + 抽象接口 + Kafka 配置
2. 落数据库 V2 迁移脚本（业务表 16 张：coin / chain_config / wallet_address / hd_path / key_material / account / account_journal / chain_tx / deposit_order / withdraw_order / sweep_order / treasury_movement / nonce_register / address_balance / treasury_policy / reconcile_report，加 common-mq 的 outbox + consumed_record）
3. 落 `wallet.chain.api` SPI 接口与 DTO
4. 落 `wallet.signer`：HD 派生 + AES-GCM + 本地 KmsProvider
5. 落 `wallet.nonce`：NonceAllocator（DB 乐观锁 + 启动校准）
6. 落 `wallet.fee`：FeeStrategy 抽象 + 三链实现（先 ETH，BTC/TRON 跟随各自链落地）
7. 落 `wallet.core`：账户、双账法 LedgerService、地址池
8. 落 `wallet.chain.eth`（含多 RPC 高可用 + 特殊交易过滤）
9. 落 `wallet.scanner`（ETH 实现 + reorg 检测回滚）+ 充值入账端到端
10. 落 `wallet.withdraw` 状态机 + Spring Statemachine + nonce 集成 + 提现端到端（含加速/取消）
11. 落 `wallet.sweep` 归集状态机（ETH drip 模式优先）
12. 落 `wallet.treasury` 水位策略 + 入冷自动 / 出冷多签预留
13. 落 `wallet.riskbridge` + `risk` 模块的钱包侧规则
14. 落 `wallet.reconcile` 三层对账 + reconcile_report
15. 复制实现 `wallet.chain.btc`（UTXO 模型 + UTXO 锁 + RBF）
16. 复制实现 `wallet.chain.tron`（合约调用模型 + fee_payer 归集 + energy 经济模型）
17. 监控指标 + 告警规则
18. 集成测试（充值 + 提现 + 归集 + reorg + 对账）+ 故障注入测试

详细实施步骤拆分将在 writing-plans 阶段完成。
