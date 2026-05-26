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

- 不实现冷钱包多签离线签名工具链（仅在接口预留）
- 不集成 MPC/TSS 库（仅在 `Signer` 接口处预留实现位）
- 不接入第三方 AML/KYT 服务（如 TRM / Chainalysis），但 `RiskGuard` 抽象允许后续接入
- 不覆盖交易（撮合）侧风控
- 不引入 Camunda / Activiti 工作流引擎（论证见 §5）

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
│       ├── scanner/                        # 区块/交易扫描
│       ├── withdraw/                       # 提现状态机（Spring Statemachine）
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
  from_address  VARCHAR(128),
  to_address    VARCHAR(128) NOT NULL,
  coin_id       BIGINT,
  amount        DECIMAL(38,18) NOT NULL,
  direction     TINYINT NOT NULL,          -- 1=入站 0=我方提现出站
  confirm_count INT DEFAULT 0,
  raw_json      MEDIUMTEXT,                -- 节点原始返回，审计/回放用
  created_at    DATETIME(3),
  UNIQUE KEY uk_chain_hash_vout (chain, tx_hash, vout),
  KEY idx_to_addr (chain, to_address)
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
  status          VARCHAR(32) NOT NULL,
  fail_reason     VARCHAR(255),
  risk_decision   VARCHAR(32),             -- PASS / REJECT / MANUAL
  signed_raw      MEDIUMTEXT,              -- 签名后 rawTx hex，签后回填
  tx_hash         VARCHAR(128),            -- 广播后回填
  confirm_count   INT DEFAULT 0,
  version         INT NOT NULL DEFAULT 0,  -- 乐观锁
  created_at      DATETIME(3),
  updated_at      DATETIME(3),
  KEY idx_status_time (status, created_at),
  KEY idx_user (user_id, status),
  UNIQUE KEY uk_tx_hash (tx_hash)          -- 广播后唯一闸
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
- 冷钱包多签：BTC P2WSH、ETH Gnosis Safe，预留 `MultiSigSigner` 接口
- 第三方 AML：`RiskGuard` 内增加外部评分调用，业务侧零改动
- 新增公链：复制 `wallet.chain.tron` 子包模板，实现 5 个 SPI 接口即可
- 拆微服务：将 `wallet.*` 子包拆出独立 jar，common-mq 已经是事件驱动，跨进程零改动

## 13. 实施顺序（高层路径）

1. 在 `common` 模块内新增 `mq` 子包，含 outbox + 幂等表 + 抽象接口 + Kafka 配置
2. 落数据库 V2 迁移脚本（业务表 10 张 + outbox + consumed_record）
3. 落 `wallet.chain.api` SPI 接口与 DTO
4. 落 `wallet.signer`：HD 派生 + AES-GCM + 本地 KmsProvider
5. 落 `wallet.core`：账户、双账法 LedgerService、地址池
6. 落 `wallet.chain.eth`（最简单的 Account 模型，作为第一条链跑通）
7. 落 `wallet.scanner`（ETH 实现）+ 充值入账端到端
8. 落 `wallet.withdraw` 状态机 + Spring Statemachine + 提现端到端
9. 落 `wallet.riskbridge` + `risk` 模块的钱包侧规则
10. 复制实现 `wallet.chain.btc`（UTXO 模型）
11. 复制实现 `wallet.chain.tron`（合约调用模型）
12. 监控指标 + 对账脚本 + 告警
13. 集成测试与故障注入测试

详细实施步骤拆分将在 writing-plans 阶段完成。
