# Plan 1 — Foundation 实施计划（重写版）

> 路线图见 `docs/superpowers/plans/0000-roadmap.md`。本 Plan 落地 CEX 钱包的全部基础设施层，为后续 Plan 2/3/4/5 各链 e2e 提供地基。
>
> **当前进度**：Phase 1–5 已完成（父 pom、数据库 V2、common-mq 消息总线、wallet.chain.api SPI、wallet.signer 签名服务）；Phase 6–9 待实施。

**目标**：消息总线 + 数据库 schema + 链抽象 SPI + 签名服务 + nonce 分配 + 手续费抽象 + 双账法账本，全部跑通 + 单/集成测试绿。

**技术栈**：Java 21 · Spring Boot 3.3.5 · MyBatis-Plus 3.5.7 · MySQL 8.4 · Redis · Kafka (KRaft) · Spring Kafka · ShedLock 5.x · Bouncy Castle · web3j-crypto · Testcontainers · Micrometer + Prometheus

---

## 阅读指引

每个 Phase 按统一结构组织：

1. **新增功能** — 这一阶段对外提供了什么能力
2. **背景知识** — 不懂钱包/区块链的同学先看这一段
3. **设计原理与决策** — 为什么这么做，关键架构权衡
4. **面试考点** — 这一段在面试时能讲什么
5. **新增类清单** — 文件路径 + 一句话职责（不贴完整代码；对核心点附关键代码片段）
6. **任务步骤** — Task 粒度 checklist，与代码提交对应

用户技术栈定位：**Java 后端 + 智能合约 + 不熟钱包**。所以本文档:
- 不会重复讲 Spring/MyBatis/SQL 这类常识
- 会专门解释 BIP-32/39/44、UTXO、nonce、双账法这些钱包特有概念
- 智能合约视角解释 EVM 链的 fee/nonce 模型

---

## 全局架构

```
┌─────────────────────────────────────────────────────────────┐
│   wallet.withdraw / wallet.sweep / wallet.scanner（业务）    │  ← 链无关业务
│                          ↓                                  │
│   wallet.chain.api  +  wallet.signer  +  wallet.nonce  +    │  ← 链抽象 + 基础设施
│   wallet.fee  +  wallet.core（双账法账本）                   │
│                          ↓                                  │
│   common.mq（事务性 outbox + 幂等消费 + Kafka）              │  ← 消息总线
│                          ↓                                  │
│   MySQL（业务表 + outbox + consumed_record）  +  Redis  +   │  ← 持久化
│   Kafka                                                      │
└─────────────────────────────────────────────────────────────┘
```

**子包依赖（强约束）**：
```
wallet.core / wallet.withdraw / wallet.scanner
            ↓ 只能依赖
wallet.chain.api（接口 + DTO）        wallet.signer / wallet.nonce / wallet.fee
            ↑ 各自实现                              ↑
wallet.chain.btc / eth / tron        被 withdraw / sweep 调用
```
核心红线：`wallet.core` **禁止** import 任何 `wallet.chain.{btc,eth,tron}`、`wallet.signer`、`wallet.scanner`。

---

## Phase 1 — 父 pom 版本管理 ✅ 已完成

### 新增功能

父 pom 引入 7 类新依赖的版本管理，子模块只声明 artifact 不写 version。

### 设计原理

所有第三方依赖版本号**只在父 pom `<properties>` 定义一次**，子模块继承 `dependencyManagement`。避免半年后升级 Spring Kafka 一个小版本要改 N 处 pom，且杜绝"common 用 3.2.4、wallet 用 3.1.x"撕裂。

**版本选型原则**：跟 Spring Boot BOM 走的不重定义（jackson/mybatis）；非 BOM 内的强制管控（spring-kafka/shedlock/bouncycastle）；testcontainers 用 BOM 而非单 artifact。

### 依赖清单（已加在 `pom.xml`）

| 依赖 | 版本 | 用途 |
|---|---|---|
| spring-kafka | 3.2.4 | Producer/Consumer/DefaultErrorHandler |
| shedlock-spring + jdbc-template | 5.16.0 | 多实例定时任务互斥 |
| bouncycastle (bcprov-jdk18on) | 1.78.1 | AES-GCM + secp256k1 |
| web3j-crypto | 4.12.2 | BIP-32/39/44 派生 + Keccak |
| testcontainers-bom | 1.20.4 | MySQL/Kafka 集成测试 |
| micrometer-core | 1.13.6 | Prometheus 指标 |
| spring-statemachine-core | 4.0.0 | Plan 2/3 提现/归集状态机 |

### 面试考点

- Maven `dependencyManagement` vs `dependencies` 的区别
- Spring Boot BOM 的覆盖关系
- 为什么 testcontainers 要用 BOM（mysql/kafka/junit-jupiter 必须版本一致）

### Task 清单

- [x] **Task 1**: 父 pom 加 properties + dependencyManagement

---

## Phase 2 — 数据库 V2 迁移 ✅ 已完成

### 新增功能

一份 Flyway V2 迁移脚本（`bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`），创建 19 张表（16 张业务 + outbox + consumed_record + shedlock）。

### 背景知识：钱包账本为什么这么多表

钱包系统比一般业务系统多了三层：
1. **链上视角**（`chain_tx`、`address_balance`）—— 区块链上发生了什么
2. **业务视角**（`deposit_order`、`withdraw_order`、`sweep_order`、`treasury_movement`）—— 用户的充提归集动作
3. **账本视角**（`account`、`account_journal`）—— 钱在我的账本里怎么动（双账法）

这三层各自独立，通过 `trace_id` / `chain_tx_id` / `tx_hash` 关联。任何一个层挂了，其他两层都能定位差异并修复。

### 表分组（19 张）

| 组 | 表 | 职责 |
|---|---|---|
| 消息基础设施 | `outbox` / `consumed_record` / `shedlock` | 事务性 MQ + 幂等闸 + 分布式锁 |
| 资产配置 | `coin` / `chain_config` | 币种、链、确认数、提现限额 |
| 地址与密钥 | `wallet_address` / `hd_path` / `key_material` | 用户地址、已用 HD path 占位、加密私钥种子 |
| 账户与流水 | `account` / `account_journal` | 余额（available + frozen） + append-only 流水 |
| 链上交易 | `chain_tx` | 链上交易快照（含 reorg 状态） |
| 业务流程 | `deposit_order` / `withdraw_order` / `sweep_order` / `treasury_movement` | 充值/提现/归集/冷热调拨工单 |
| 归集与水位 | `nonce_register` / `address_balance` / `treasury_policy` | EVM nonce、地址余额缓存、冷热水位 |
| 对账 | `reconcile_report` | 三层对账日报 |

### 设计原理与决策

**全表共用约定**：
- 主键：`BIGINT PRIMARY KEY`（雪花 ID，趋势递增减少 InnoDB 页分裂）；`account_journal` 单主键；`nonce_register`、`address_balance` 复合主键
- 金额：所有金额字段 `DECIMAL(38,18)`，38 位精度兼容 BTC（8 位小数）/ ETH（18 位小数）/ USDT 等。**禁止 DOUBLE**
- 时间戳：`DATETIME(3)` 含毫秒；不用 TIMESTAMP（避免 2038 / 时区漂移）
- 乐观锁：会被并发 update 的表（`account` / `*_order` / `nonce_register`）加 `version INT NOT NULL DEFAULT 0`
- 字符集：`utf8mb4`，避免地址 / payload 里非 BMP 字符炸表

**核心决策**：
- **不分库分表**：单库 InnoDB 撑得住——交易量 × 双账法 2x 放大 × 7 年 ≈ 10 亿行级，热点表加分区/归档即可
- **不引 NoSQL 副存储**：对账要求强一致快照，MySQL 是真理之源
- **不用 Hibernate**：双账法这种"必须看到 SQL 形态"的场景，ORM 反而是负担；MyBatis-Plus 注解 + 复杂场景写 XML mapper

### 关键约束（双账法的命门）

```sql
-- account_journal 的双账幂等闸
UNIQUE KEY uk_trace_direction_account (trace_id, direction, account_id)
```
三字段联合唯一：同 trace 同 direction 在不同账户上必须能各自插入（`transferInternal` 跨两个账户写双方各一条 +1/-1；`freeze` 同账户但 direction 不同）。这是双账法"重入安全"的硬保证。

### 面试考点

- 金额为什么用 DECIMAL 不能用 DOUBLE（IEEE 754 浮点累加误差 → 对账永远对不上）
- 雪花 ID 为什么比 UUID 好（趋势递增 → InnoDB 主键聚簇索引友好）
- `uk_trace_direction_account` 为什么是三字段而不是 `(trace_id, direction)`：跨账户场景

### Task 清单

- [x] **Task 2-7**: V2 迁移脚本骨架 + 5 组表分批落地
- [x] **Task 8**: 启动 bootstrap 验证 Flyway 应用 V2，`show tables` 见 19 张表

---

## Phase 3 — common.mq：事务性消息总线 ✅ 已完成

### 新增功能

业务模块**不再直接接触** `KafkaTemplate` / `@KafkaListener`：
- 发消息一行：`txPublisher.publish(event)`（在业务事务内调用）
- 消费消息：继承 `IdempotentEventHandler<T>`，重写 `handle(T event)`
- 多实例部署免抢消息（ShedLock 互斥 OutboxRelay）
- 切换 RocketMQ/Pulsar 业务侧零改动

### 背景知识：为什么需要 Outbox 模式

业务变更（落账/改单）写 MySQL，然后给下游发 Kafka 通知。如果两步任意一步失败：
- 业务已 commit + Kafka 没发 → 下游错过事件、对账不齐
- Kafka 已发 + 业务回滚 → 下游凭一条不存在的事件做反应（"幽灵事件"）

XA 两阶段提交跨 MySQL+Kafka 不可用且性能差。**Outbox 模式**：用一张本地表 `outbox` 当"准发事件"，业务变更 + outbox INSERT 在同一本地事务内。事务一 commit"事件即将发"成必然事实，独立 Relay 进程把 outbox 行搬到 Kafka（at-least-once）。下游用 `consumed_record` 表做幂等等价 exactly-once。

### 设计原理与决策

```
业务事务（同事务内）：UPDATE account ... + INSERT outbox PENDING
        ↓ commit
OutboxRelay（@Scheduled + ShedLock）每秒：
        SELECT * FROM outbox WHERE status=0 LIMIT 200
        → kafkaTemplate.send().get()
        → markStatus(SENT) 或 markRetry（指数退避）
        ↓
Kafka topic
        ↓
IdempotentEventHandler.onMessage(event):
        if consumed_record.exists(event_id, handler_name): return  // 快速路径
        handle(event)
        consumed_record.insertIgnore(...)
```

**关键决策**：

1. **`Propagation.MANDATORY`**（核心）— `TransactionalEventPublisher.publish` 必须在调用方事务内。`REQUIRED`（默认）会自己开新事务，业务回滚时 outbox 不回滚 → 幽灵事件。`MANDATORY` 把这个隐患变成启动期/运行期立刻抛错。

2. **OutboxRelay 用 ShedLock 而非 Redis 分布式锁** — 已经在用 MySQL，ShedLock 把锁状态也落 MySQL，少一层依赖。`lockAtMostFor=30s` 防进程崩死锁；`lockAtLeastFor=500ms` 防主从延迟导致同行被两个实例都拿到。

3. **失败指数退避** `min(60 * 2^min(retry,8), 600)` — 60s/120s/240s/.../600s 封顶。

4. **下游幂等用表而非 Redis** — Redis 持久化弱，掉一次数据就可能让账户事件被重复处理；MySQL 的持久性 + 双账法本身的幂等闸（`uk(trace_id, direction, account_id)`）才是真正可靠兜底。

5. **DLQ 不在 IdempotentEventHandler 内** — 异常分类二分法：`RetriableException` 透传给 Spring Kafka `DefaultErrorHandler` 重试；其他 `RuntimeException` 也透传，最终进 DLT topic。`IdempotentEventHandler` 只做"幂等闸 + 异常类型分类"。

### 面试考点

- **Outbox 模式 vs 两阶段提交（XA）vs 直接发 Kafka** — 各自一致性边界
- **at-least-once + 下游幂等 = exactly-once 等价** — 这是分布式系统的实践共识
- **为什么不用 Debezium CDC** — 引入额外组件成本高，轮询 outbox 表 1s 延迟在交易场景可接受
- **`Propagation.MANDATORY` 与 `REQUIRED/REQUIRES_NEW` 区别** — 这是 outbox 模式的隐藏命门
- **ShedLock 的 `lockAtLeastFor` 为什么必要** — 主从复制延迟的安全阈值
- **partition key 用 `aggregateId`** — 同 aggregate 事件分区内有序

### 新增类清单（已实现）

位于 `common/src/main/java/com/exchange/common/mq/`：

| 类 | 职责 |
|---|---|
| `DomainEvent` | 事件契约：eventId/aggregateId/eventType/occurredAt |
| `AbstractDomainEvent` | 基类，eventId 用 SnowflakeIdGenerator 生成 |
| `RetriableException` | 标记可重试异常 |
| `EventPublisher` | 即时发送接口（无事务保证，监控/告警用） |
| `TransactionalEventPublisher` | 事务性发送接口（写 outbox） |
| `IdempotentEventHandler<T>` | 抽象消费者：exists → handle → markConsumed |
| `MqAutoConfiguration` | Spring Boot 自动装配入口 |
| `outbox/OutboxEntity` + `OutboxMapper` + `OutboxStatus` | outbox 表 + CAS 更新 SQL |
| `outbox/OutboxRelay` | `@Scheduled(fixedDelay=1000)` + `@SchedulerLock` 批量投递 |
| `outbox/ShedLockConfig` | LockProvider Bean |
| `outbox/TransactionalEventPublisherImpl` | `@Transactional(MANDATORY)` 写 outbox |
| `consumed/ConsumedRecordEntity` + `Mapper` + `Store` | 幂等闸表 |
| `kafka/KafkaConfig` | KafkaTemplate（idempotence + acks=all） |
| `kafka/EventEnvelope` | schemaVersion + payload 跨进程封装 |
| `kafka/KafkaEventPublisher` | 即时发送默认实现 |
| `serializer/EventSerializer` | Jackson JSON 序列化 |

核心代码片段（`TransactionalEventPublisherImpl`）：
```java
@Override
@Transactional(propagation = Propagation.MANDATORY)
public void publish(String topic, DomainEvent event) {
    OutboxEntity entity = new OutboxEntity();
    entity.setEventId(event.eventId());
    entity.setTopic(topic);
    entity.setPartitionKey(event.aggregateId());
    entity.setPayload(serializer.toJson(EventEnvelope.wrap(event)));
    entity.setStatus(OutboxStatus.PENDING.code);
    outboxMapper.insert(entity);
}
```

### Task 清单

- [x] **Task 9**: common pom 加 spring-kafka/shedlock/micrometer/testcontainers 依赖
- [x] **Task 10**: DomainEvent / AbstractDomainEvent / RetriableException + 单测
- [x] **Task 11**: Outbox 实体 + Mapper + 状态枚举
- [x] **Task 12**: ConsumedRecord 实体 + Mapper + Store
- [x] **Task 13**: Kafka 配置 + EventEnvelope + KafkaEventPublisher
- [x] **Task 14**: TransactionalEventPublisher 接口 + 实现 + 单测
- [x] **Task 15**: OutboxRelay + ShedLock + 单测
- [x] **Task 16**: IdempotentEventHandler 抽象消费者 + 单测
- [x] **Task 17**: MqAutoConfiguration + AutoConfiguration.imports
- [x] **Task 18**: common-mq 集成测试（Testcontainers MySQL + EmbeddedKafka）

---

## Phase 4 — wallet.chain.api：链抽象 SPI ✅ 已完成

### 新增功能

业务层（`wallet.core` / `wallet.scanner` / `wallet.withdraw` / `wallet.sweep`）只依赖一组接口与 DTO，**不直接依赖** BTC/ETH/TRON 任何 SDK。新增公链 = 复制一个 `wallet.chain.<name>` 子包实现 5 SPI，业务代码零改动。

### 背景知识：三种链模型差异

| 链 | 模型 | 转账原语 | 手续费 | 多输出 |
|---|---|---|---|---|
| **BTC** | UTXO | 一笔 tx 多 input + 多 output；input 必须是之前未花的 UTXO | sat/vB × vsize | 是（vout 0..N-1） |
| **ETH** | Account + EVM | from→to 转账或合约调用；带 nonce 严格连续递增 | gasLimit × gasPrice (EIP-1559: baseFee + priorityFee) | 否（一笔 tx 一对 from/to）；ERC20 转账靠 Transfer Log |
| **TRON** | Account + 资源模型 | 类 EVM；带 sequence | energy + bandwidth（质押 TRX 获取免费额度） | 否；TRC20 转 TriggerSmartContract |

字段差异巨大但**业务流程一致**：build → sign → broadcast → wait confirm → ledger settle。SPI 把差异封到实现内。

### 设计原理与决策

**5 个 SPI**（按 chain 路由到 Spring Map<Chain, Bean>）：

| SPI | 职责 |
|---|---|
| `ChainClient` | 链上读：getBlock / getLatestHeight / queryTxStatus / getBalance / getOnChainNonce |
| `TxBuilder` | 构造未签名 RawTx：业务 TransferRequest → 链特化 RawTx |
| `TxBroadcaster` | 把 SignedTx 发到链，返回 txHash |
| `TxParser` | 把 ChainBlock 解析成 List<ChainTx>（识别 ERC20 Log / 多 vout / coinbase 跳过） |
| `AddressDerivator` | 公钥/HD path → 地址（EIP-55 / Bech32 / Base58） |

**`Signer` 单独抽**（不属于 5 SPI）：
- 业务层调 `Signer.sign(rawTx, keyRef)` 拿 SignedTx，**永远不接触私钥**
- 内部 `wallet.signer` 包闭环：取私钥 → 签 → 清零
- 业务依赖一个 `Signer` Bean，链路由对调用方透明（区别于 5 SPI 业务要按 chain 路由）

**关键决策**：
- **`RawTx` / `SignedTx` 是 byte[] + metadata 容器** — 不强抽公共字段，BTC PSBT 与 ETH RLP 字节布局完全不同
- **`ChainTx` 字段与 DB `chain_tx` 对齐** — scanner 直接 `mapper.insert`
- **`KeyRef` 不放私钥** — 只 `(keyId, hdPath, chain)`，让 Signer 去 `key_material` 表查
- **不用 SPI 自动发现（ServiceLoader）** — 直接用 Spring Bean + Map 路由，可控可测可替换
- **每个接口都有 `chain()` 方法** — 用于 `Map<Chain, Bean>` 启动期注册

### 面试考点

- **如何设计可扩展的链抽象层** — SPI 模式的工程化实践
- **链特化字段往哪里放**：`Map<String, Object> chainSpecific`，业务层不读，仅由各链 TxBuilder/FeeStrategy 自家 + scanner 解析时使用
- **为什么 Signer 不属于 5 SPI** — 私钥隔离原则
- **DTO 用 Lombok @Data + @Builder 而非 record** — Jackson 反序列化兼容 + 与项目其他模块风格一致

### 新增类清单（已实现）

位于 `wallet/src/main/java/com/exchange/wallet/chain/api/`：

| 类 | 职责 |
|---|---|
| `Chain` | 链枚举（BTC/ETH/TRON）+ `Chain.of(String)` |
| `ChainClient` / `TxBuilder` / `TxBroadcaster` / `TxParser` / `AddressDerivator` | 5 SPI 接口 |
| `Signer` | 签名接口（业务层唯一入口）|
| `dto/RawTx` | 未签名交易：chain + from/to + nonce + rawBytes + chainSpecific Map |
| `dto/SignedTx` | 已签名交易：signedBytes + hexEncoded + predictedTxHash |
| `dto/ChainTx` | 扫块产物，与 chain_tx 表对齐 |
| `dto/ChainBlock` | 块快照：height + hash + parentHash + rawBlock |
| `dto/TransferRequest` | 业务转账请求 |
| `dto/TxStatus` | Phase 枚举（NOT_FOUND/PENDING/MINED_OK/MINED_FAILED/DROPPED）|
| `dto/KeyRef` | 私钥引用：keyId + hdPath + chain |
| `dto/DerivedAddress` | 派生结果：address + hdPath + publicKeyHex |
| `dto/FeeQuote` / `dto/FeeQuoteRequest` | 手续费估算输入输出 |

### Task 清单

- [x] **Task 19**: wallet pom 加 web3j-crypto/bouncycastle/statemachine/risk
- [x] **Task 20**: Chain 枚举 + 10 个链无关 DTO
- [x] **Task 21**: 5 SPI 接口（ChainClient/TxBuilder/TxBroadcaster/TxParser/AddressDerivator）
- [x] **Task 22**: Signer 接口

---

## Phase 5 — wallet.signer：密钥与签名 ✅ 已完成

### 新增功能

钱包私钥的全生命周期闭环：生成助记词 → 派生 HD 子私钥 → AES-GCM 加密落库 → 签名时解密 → 用完立即清零。业务代码通过 `Signer.sign(rawTx, keyRef)` 间接使用，永远拿不到明文私钥字节。

### 背景知识：钱包密钥层次（针对不熟钱包的同学）

钱包不像传统系统给每个用户存一对密钥。区块链钱包有标准化的派生层次：

```
（1）BIP-39 助记词（12/24 个英文词，便于人类抄录备份）
        ↓ PBKDF2-HMAC-SHA512(2048 iter, passphrase)
（2）64B Seed（种子）
        ↓ HMAC-SHA512("Bitcoin seed", seed)
（3）BIP-32 主私钥 + chain code
        ↓ 按 BIP-44 路径派生：m/44'/coinType'/account'/change/index
（4）海量子私钥（每个用户每条链一个，甚至每次充值一个新地址）
        ↓ secp256k1 椭圆曲线 → 公钥 → 链特定哈希 → 地址
（5）地址（用户看到的 0x... / bc1q... / T...）
```

**关键性质**：
- **确定性派生**：同 seed + 同 path = 同私钥。所以"灾备恢复"只需要一份助记词，能恢复所有用户地址。
- **SLIP-44 coinType**：BTC=0、ETH=60、TRX=195（业界注册号）。
- **三大公链都用 secp256k1 椭圆曲线**（不是 ED25519），所以一套派生 + 一套签名能覆盖。

### 私钥安全的核心原则（命门）

钱包系统挂在哪里？私钥泄漏 = 项目归零。所以：

1. **明文私钥只在签名瞬间存在**：`fetch → derive → sign → wipe`，全程在同一个方法栈帧 try/finally 内
2. **不写日志、不进 toString、不进 exception message**
3. **不能用 String 持有**（String 不可变 → 没法清零，留在 String pool 里）；一律 `byte[]`
4. **`Arrays.fill(buf, (byte)0)` 立即清零** —— JVM heap 上 GC 后字节可能残留任意长，dump 时可能被读出

### 设计原理与决策

**Phase 5 子模块**：

| Task | 组件 | 作用 |
|---|---|---|
| 23 | `AesGcmCipher` | AES-256-GCM 加解密 + `wipe` 工具 |
| 24 | `KmsProvider` 接口 + `LocalKeystoreKmsProvider` | KMS 抽象（生产替换为 AWS KMS / Vault），本地实现走主密钥加密 |
| 25 | `Bip39MnemonicService` | 助记词生成/校验/转 seed |
| 26 | `Bip32HdKeyDeriver` | BIP-32/44 路径派生子私钥 |
| 27 | `KeyMaterial` 实体/Mapper/Service | 加密种子落 DB + 取出解密 |
| 28 | `ChainSpecificSigner` + `SignerImpl` | 链特定签名 + 路由 + 私钥清零 |

**关键决策**：

1. **AES-256-GCM 而非 AES-CBC**：GCM 自带认证（防篡改），CBC 必须额外 HMAC，多一步出错可能。GCM 的 12B IV + 16B Tag 是工业标准。

2. **IV 必须 SecureRandom 12B 随机** — 同 key 重用 IV = AES-GCM 灾难（可被逆推 key）。

3. **`KmsProvider` 是抽象**：本期 `LocalKeystoreKmsProvider` 用配置注入的 base64 主密钥（dev 环境）；生产替换为 `AwsKmsProvider`（调用云 KMS Decrypt API），业务代码零改动。`kms_alias` 字段支持密钥轮换。

4. **派生过程确定性 + Hardened derivation 只用前三段** — `m/44'/coin'/account'` 加撇号（hardened，子私钥不能反推父公钥），change/index 不加（方便给观察钱包导出 xpub）。

5. **`SignerImpl` 是单例 + 内部按 chain 路由** — 业务依赖一个 Bean；各链 `ChainSpecificSigner` 实现在 Plan 2/4/5 各自的 chain 子包提供（`BtcSigner` / `EthSigner` / `TronSigner`）。

6. **`fetch + derive + sign + wipe` 全部在 `try/finally` 同栈帧** — 哪怕 sign 抛异常也保证清零。

### 面试考点

- **BIP-32/39/44 三个标准的关系**（很多面试官以为是同一个）
- **为什么 secp256k1 比 ED25519 更适合公链**（历史原因 + 比特币兼容）
- **AES-GCM vs CBC vs CTR**（认证加密 vs 仅加密）
- **为什么不用 String 存私钥** + **JVM 内存清零的局限**（BC 的 BigInteger 用完后无法清零是已知妥协）
- **KMS 在密钥管理中的位置**：开发者根本不应该见到主密钥，只通过 alias 调 KMS 服务
- **为什么 Signer 用 KeyRef 而非 PrivateKey 入参**（隔离"取私钥"和"用私钥"两个职责）

### 新增类清单（已实现）

位于 `wallet/src/main/java/com/exchange/wallet/signer/`：

| 类 | 职责 |
|---|---|
| `kms/AesGcmCipher` | 加解密 + `wipe` 静态工具，`Cipherblob(iv, cipherText)` 内部类 |
| `kms/KmsProvider` | 接口：`resolveDataKey(alias)` + `defaultAlias()` |
| `kms/LocalKeystoreKmsProvider` | 默认实现，从 `wallet.signer.kms.local-master-key-base64` 配置读 32B 主密钥 |
| `hd/Bip39MnemonicService` | `generateMnemonic` / `mnemonicToSeed` / `validate`，使用 web3j-crypto 的 MnemonicUtils |
| `hd/Bip32HdKeyDeriver` | `derive(seed, hdPath)` → `HdKey(privateKey, publicKey, path)`，使用 Bip32ECKeyPair |
| `KeyMaterialEntity` / `KeyMaterialMapper` | `key_material` 表 ORM |
| `KeyMaterialService` | `storeHdSeed(seed)` 加密落库；`loadSeed(keyId)` 解密返回（调用方负责 wipe） |
| `ChainSpecificSigner` | 内部 SPI：`Chain chain()` + `SignedTx sign(rawTx, byte[] privateKey)` |
| `SignerImpl` | 单例，按 `rawTx.chain` 路由到 `ChainSpecificSigner`，try/finally 清零 |

核心代码片段（`SignerImpl`）：
```java
try {
    seed = keyMaterialService.loadSeed(keyRef.getKeyId());
    hd = deriver.derive(seed, keyRef.getHdPath());
    priv = hd.getPrivateKey();
    return cs.sign(rawTx, priv);
} finally {
    AesGcmCipher.wipe(seed);
    if (hd != null) AesGcmCipher.wipe(hd.getPrivateKey());
    AesGcmCipher.wipe(priv);
}
```

### 与现有代码的核对结论

核对了 `wallet/src/main/java/com/exchange/wallet/signer/` 下全部文件：
- `Bip39MnemonicService` 当前生成 **128 bit 熵 = 12 词**，文档原描述 "24 词为行业默认"。**取舍**：12 词足够安全，且测试用例和现有代码已对齐 12 词。本文档已修正描述与代码一致。
- 其余 8 个类与 plan 中原描述完全一致，无需改动。
- 单测已落地：`AesGcmCipherTest` / `LocalKeystoreKmsProviderTest` / `Bip39MnemonicServiceTest` / `Bip32HdKeyDeriverTest` / `SignerImplTest`。

### Task 清单

- [x] **Task 23**: AesGcmCipher + 单测
- [x] **Task 24**: KmsProvider 接口 + LocalKeystoreKmsProvider + 单测
- [x] **Task 25**: Bip39MnemonicService + 单测
- [x] **Task 26**: Bip32HdKeyDeriver + 单测
- [x] **Task 27**: KeyMaterial 实体/Mapper/Service
- [x] **Task 28**: ChainSpecificSigner + SignerImpl + 单测（含路由 + 清零验证）

---

## Phase 6 — wallet.nonce：并发 nonce 分配 ⏳ 待实施

### 新增功能

对 EVM 类链（ETH/TRON）的同一出账地址，多线程并发提现时**严格连续递增**地发号 nonce，并提供启动校准与广播失败回收能力。

### 背景知识：nonce 是 EVM 世界的隐形一等公民（针对懂智能合约的同学）

你已经知道 EVM 的每笔 tx 必须带 nonce。从智能合约角度看 nonce 是 `account.nonce`（账户状态的一部分），由 EVM 在打包时校验：
- `tx.nonce < account.nonce` → "nonce too low"，节点直接丢弃
- `tx.nonce > account.nonce` → "nonce too high"，进 mempool 但不打包，等前面的 nonce 都被消耗
- 同一 nonce 两笔不同 gasPrice → mempool 优先选 gasPrice 高的，**这就是"同 nonce 替换"机制（加速/取消提现的实现基础）**

交易所并发问题：每秒可能并发处理几十笔提现，全部从同一个热钱包出账。所以多线程都要对同一地址发号 nonce，**不能撞、不能跳、不能丢**。

### 设计原理与决策

```
并发提现 → NonceAllocator.allocate(chain, address)
         ↓
         (DB 主路径) UPDATE nonce_register SET next_nonce=next_nonce+1, version+1 
                     WHERE chain=? AND address=? AND version=#{old}   -- CAS
         ↓ 0 row affected → 重试 5 次后抛 RetriableException
         ↓
         返回旧 next_nonce 值

启动 + 定时（5min）巡检：
  NonceReconciler:
    onChain = chainClient.getOnChainNonce(address)   -- pending 已用最大 + 1
    rectify nonce_register.next_nonce = onChain（首次）或检查空洞
```

**核心决策**：

1. **CAS 而非 SELECT FOR UPDATE** — 行锁会阻塞别的事务（高并发下退化为串行），CAS 不阻塞、自旋重试，吞吐高。

2. **`(chain, address)` 复合主键 + `next_nonce` + `version`** — DB 是真理之源；分配 = `UPDATE ... WHERE version=#{old}`，CAS 失败 = 并发冲突，重试。

3. **`NonceReconciler` 启动校准必须在 web server ready 之前** — 否则链上 nonce=10、DB 还是 0，第一笔提现拿到 nonce=0 被链拒。多实例部署用 ShedLock 互斥。

4. **`release` 难做完美** — 分配后别的线程已分配过更大 nonce，简单减回去会越界。本期 release 只在 `next_nonce == allocated + 1` 时成功；其他情况记 nonce 空洞由 Reconciler 用链上 pending nonce 修正。

5. **不用 Redis Lua 主路径** — 本期只走 DB 乐观锁；Plan 2 实施时按多实例并发压测结果决定是否补 Redis 兜底层，避免过度设计。

6. **`allocate` 必须独立短事务** — 不能嵌入业务事务：业务事务一长就持锁久 → 别的线程 CAS 失败重试不断；业务回滚不会回滚 nonce（不同事务）。

### 面试考点

- **如何在分布式环境下做并发安全的发号器** — DB 乐观锁 / Redis INCR / ZK / etcd 各自取舍
- **nonce 空洞如何处理** — 提现广播失败 vs 加速场景的区分
- **同 nonce 替换交易**（加速/取消）— 这是 EVM 上唯一的事务"撤销"机制
- **启动校准为什么是 readiness probe 的前置** — K8s 部署的工程问题

### 新增类清单

位于 `wallet/src/main/java/com/exchange/wallet/nonce/`：

| 类 | 职责 | 状态 |
|---|---|---|
| `NonceRegisterEntity` | `nonce_register` 表 ORM（chain+address 复合主键 + version 乐观锁） | 待 |
| `NonceRegisterMapper` | BaseMapper + `find / insertIfAbsent / casIncrement / reconcile` | 待 |
| `NonceAllocator` | 接口：`allocate(chain, address)` + `reconcile(chain, address, onChainPendingNonce)` | 待 |
| `DbOptimisticNonceAllocator` | 默认实现：5 次重试上限，CAS 失败抛 IllegalStateException | 待 |

关键 mapper SQL：
```java
@Update("""
    UPDATE nonce_register
       SET next_nonce = next_nonce + 1, version = version + 1
     WHERE chain = #{chain} AND address = #{address} AND version = #{version}
    """)
int casIncrement(...);
```

### Task 清单

- [ ] **Task 29**: NonceRegisterEntity + Mapper（含 CAS / reconcile SQL）
- [ ] **Task 30**: NonceAllocator 接口 + DbOptimisticNonceAllocator 实现 + 单测（成功/重试/未初始化三场景）

---

## Phase 7 — wallet.fee：手续费抽象 ⏳ 待实施

### 新增功能

业务层（withdraw / sweep）一行 `feeStrategyRegistry.quote(req)` 拿到 `FeeQuote`，不关心底下是 BTC sat/vB、ETH EIP-1559、还是 TRON energy。具体策略实现 **在各链 plan 里落地**（Plan 2 EthFeeStrategy / Plan 4 BtcFeeStrategy / Plan 5 TronFeeStrategy）；本 Phase 只搭骨架。

### 背景知识：三链手续费模型

| 链 | 单位 | 估算依赖 | 加速机制 |
|---|---|---|---|
| **BTC** | sat/vB | mempool feerate / `estimatesmartfee N` | RBF（Replace-By-Fee） |
| **ETH (EIP-1559)** | wei (gas) | `eth_feeHistory` 取最近 20 块 baseFee 中位数 + 50% 分位 priorityFee | 同 nonce + 高 gasPrice 替换 |
| **TRON** | sun (energy + bandwidth) | `triggerconstantcontract` 估算 + 主账户质押情况 | fee_payer 代付，不能加速 |

各链特化字段塞 `FeeQuote.chainSpecific: Map<String, Object>`：
- ETH: `maxFeePerGas / maxPriorityFeePerGas / gasLimit`
- BTC: `satPerVByte / vsize / utxoCount`
- TRON: `energy / bandwidth / feePayer`

业务层不读 map，由各链 `TxBuilder` 与 `FeeStrategy` 自己处理。

### 设计原理

```
业务 → FeeStrategyRegistry.quote(req)
         ↓ by req.chain
        FeeStrategy 实现（各链 Plan 落）
         ↓
        FeeQuote(feeAmount, feeCoinSymbol, chainSpecific)
```

**关键决策**：
- 接口在 Plan 1 落，实现在各链 plan 落 — 避免 Plan 1 引入 BTC/ETH/TRON 的 RPC 客户端依赖
- 不抽 `accelerate` 方法 — 加速完全是链特定的（EVM 调高 gasPrice / BTC RBF / TRON 不能加速），由各链 `wallet.withdraw` 状态机处理
- Registry 启动时填充 — 用 Spring `List<FeeStrategy>` 注入，按 `chain()` key 自动归类

### 面试考点

- **EIP-1559 vs Legacy gas 模型** — baseFee burn / priorityFee 给矿工
- **BTC RBF 为什么需要在 tx 上设 nSequence** — opt-in RBF 机制
- **TRON energy 经济模型** — 质押 TRX 获取 energy 远便宜于直接烧 TRX
- **极端拥堵保护**：`maxFeePerGas` 上限避免一笔提现把 fee 烧光（`chain_config.max_gas_price_gwei`）

### 新增类清单

位于 `wallet/src/main/java/com/exchange/wallet/fee/`：

| 类 | 职责 | 状态 |
|---|---|---|
| `FeeStrategy` | 接口：`Chain chain()` + `FeeQuote quote(FeeQuoteRequest req)` | 待 |
| `FeeStrategyRegistry` | 按 chain 路由的 Spring Component，注入 `List<FeeStrategy>` | 待 |

### Task 清单

- [ ] **Task 31**: FeeStrategy 接口 + FeeStrategyRegistry + 单测（fake EthFeeStrategy 覆盖路由 + unknown chain 抛错）

---

## Phase 8 — wallet.core：双账法账本 + 实体 + 地址池 ⏳ 待实施

### 新增功能

- 14 张表的 Entity + Mapper（含 `account` CAS 更新、`account_journal` 凑零汇总查询）
- `LedgerService` 6 个语义化操作（freeze / unfreeze / settle / credit / reverseCredit / transferAvailable）
- 系统账户常量（INFLOW=-1 / HOT_WALLET=-2 / FEE=-3 / FROZEN_BUFFER=-4）
- `AddressPoolService`：HD 路径不重复分配的地址生成
- `WalletAutoConfiguration` 让 wallet 子包被 bootstrap 装配

### 背景知识：双账法（复式记账）

会计学的复式记账法搬到交易所账本里。任何资金移动**必须**拆成"出"和"入"两条 journal 行：金额相等、direction 相反、`trace_id` 共享。

```
用户充值 1 ETH:
  systemInflow(-1) journal: direction=-1 amount=1   trace_id=t1
  user(1001)       journal: direction=+1 amount=1   trace_id=t1
  → SUM(direction × amount) = 0   ✓ 凑零不变量成立

用户提现 0.5 ETH 冻结阶段:
  user available  journal: direction=-1 amount=0.5  trace_id=t2
  user frozen     journal: direction=+1 amount=0.5  trace_id=t2
  → 同账户 available 列减、frozen 列加；同 user 但不同 "列"
```

**为什么必须双账**：
1. **可审计**：任意时刻 `SUM(direction × amount)` 必为 0，破坏 = bug 立刻暴露
2. **跨账户原子性**：A 转给 B，两个 account UPDATE + 两条 journal INSERT 在同一事务
3. **幂等保证**：`uk(trace_id, direction, account_id)` 让同一笔操作重试时第二次必被唯一约束挡掉
4. **支持反向冲账**：reorg / 风控驳回 / 误操作时反向 trace_id 写另一组双行；流水永不删除，只追加（append-only）

### 系统账户约定

用户 user_id 都是正数；系统账户 user_id < 0：

| user_id | 名称 | 用途 |
|---|---|---|
| -1 | INFLOW | 充值入金中间账户：链上识别充值后先 +1，再划转到用户账户 |
| -2 | HOT_WALLET | 主热钱包账户：对应链上热钱包地址，提现结算的资金来源 |
| -3 | FEE | 手续费账户：所有矿工费聚集 |
| -4 | FROZEN_BUFFER | 提现冻结暂存：用户冻结但还没广播的资金 |

系统账户在启动期由 `@PostConstruct` 保证插入（避免业务用到时 NULL）。

### LedgerService 6 个操作

| 方法 | 双账写入 | 用途 |
|---|---|---|
| `transferAvailable(cmd)` | `from.available -` ↔ `to.available +` | 用户间内部转账、归集 |
| `freeze(user, coin, amt, ...)` | `user.available -` ↔ `user.frozen +`（同账户两列） | 提现冻结、订单挂单 |
| `unfreeze(...)` | `user.frozen -` ↔ `user.available +` | 提现取消、挂单撤销 |
| `settle(user, ...)` | `user.frozen -` ↔ `HOT_WALLET.available +` | 提现广播确认后 |
| `credit(user, ...)` | `INFLOW.available -` ↔ `user.available +` | 充值入账 |
| `reverseCredit(user, ...)` | `user.available -` ↔ `INFLOW.available +` | reorg 反向冲账 |

### 设计原理与决策

**核心决策**：

1. **`account` 表只有 `available + frozen` 两列** — 不再细分"在途/不可用/受限"，复杂场景靠 journal 还原。约束越简，bug 越少。

2. **`account_journal` 是 append-only** — 永不 UPDATE 也不 DELETE。审计追溯靠 `WHERE biz_type=? AND biz_id=?`。

3. **CAS 更新 `account`** — `UPDATE account SET available=?, frozen=?, version=version+1 WHERE id=? AND version=#{old}`。零行影响 = 余额不足或并发冲突。
   - **更严格的版本**会带余额校验：`AND available>=?`，但本期实现先在 service 层 `if` 校验 + CAS。**面试可以讨论两种取舍**：service 校验更易读 / SQL 校验单 round-trip 更原子（推荐生产用 SQL 内校验）。

4. **journal `balance_after` 字段** — 写入时 `available - amount` 已知，可直接填入。便于 reconcile 时回放。

5. **`uk(trace_id, direction, account_id)` 唯一约束** — 第二次重入撞 `DuplicateKeyException`。**LedgerServiceImpl 把它当作幂等命中**——业务调用方第一次已经成功执行过，第二次什么也不做即可（不再回滚整个事务）。

6. **不在 LedgerServiceImpl 直接调 KafkaTemplate** — 账本变更事件由调用方 service 通过 `TransactionalEventPublisher` 写 outbox。Ledger 只管账，不管对外通知（单一职责）。

7. **失败时直接抛异常 + Spring 自动回滚** — 不要 try/catch 然后吞异常，会破坏事务边界。

### 面试考点（这是 Plan 1 最重要的面试点）

- **双账法 vs 单边记账** — 后者审计追溯困难，bug 难定位
- **`balance_after` 字段为什么有用** — reconcile 时不需要重新累加全部历史 journal
- **`uk(trace_id, direction, account_id)` 三字段设计** — 单 (trace_id) 不行（一笔业务两条 journal）；(trace_id, direction) 也不行（跨账户场景）
- **系统中间账户为什么必要** — 充值场景下"凭空多出来的钱"如果没有对手方就破坏凑零不变量
- **CAS 重试为什么不放在 LedgerService 内** — 业务期望"扣钱失败立刻知道"，不应隐式重试 5 次成了
- **`BigDecimal.equals` vs `compareTo`** — `new BigDecimal("1.0").equals(new BigDecimal("1.00"))` 是 false（scale 不同），必须用 compareTo

### 易踩的坑（一定要在测试里覆盖）

- 单边记账（只写一条 journal）→ 审计永远查不出对不上的根因
- 业务侧不传 trace_id 直接重入 → 第二次 insert 撞 uk 抛错（这就是设计意图）
- 跨账户转账没在同一事务 → A 扣了 B 没加 → 总额不平
- SystemAccount 不存在但被引用 → 用 `@PostConstruct` 在启动期保证插入这 4 行

### 新增类清单

位于 `wallet/src/main/java/com/exchange/wallet/core/`：

**`entity/`（14 个 Entity，Lombok @Data + @TableName）**

| Entity | 对应表 |
|---|---|
| `CoinEntity` | coin |
| `ChainConfigEntity` | chain_config |
| `WalletAddressEntity` | wallet_address |
| `HdPathEntity` | hd_path |
| `AccountEntity` | account（主力实体，含 version 乐观锁） |
| `AccountJournalEntity` | account_journal（append-only） |
| `ChainTxEntity` | chain_tx |
| `DepositOrderEntity` | deposit_order |
| `WithdrawOrderEntity` | withdraw_order |
| `SweepOrderEntity` | sweep_order |
| `TreasuryMovementEntity` | treasury_movement |
| `AddressBalanceEntity` | address_balance |
| `TreasuryPolicyEntity` | treasury_policy |
| `ReconcileReportEntity` | reconcile_report |

**`mapper/`（14 个 Mapper）**

大部分继承 `BaseMapper<T>` 即可。两个例外：
- `AccountMapper`: 加 `find(userId, coinId)` + `casUpdate(id, available, frozen, version)`
- `AccountJournalMapper`: 加 `sumDirectionalAmount(coinId)`（凑零不变量校验）+ `sumByAccount(accountId)`

**`ledger/`（双账法核心）**

| 类 | 职责 |
|---|---|
| `SystemAccountConstants` | 4 个 long 常量 + private 构造器 |
| `BizType` | enum（DEPOSIT/WITHDRAW_FREEZE/WITHDRAW_SETTLE/INTERNAL_TRANSFER/SWEEP_OUT/SWEEP_IN/FEE/REVERSE_DEPOSIT/TREASURY_MOVE） |
| `JournalDirection` | enum CREDIT(1) / DEBIT(-1) |
| `LedgerCommand` | DTO：traceId/from/to/coinId/amount/bizType/bizId/remark |
| `LedgerService` | 接口：6 个方法 |
| `LedgerServiceImpl` | 实现：每个方法 `@Transactional` + CAS update + 双 journal insert，提供 `ensureAccount(userId, coinId)` 自动建账 |

**`address/`（地址池）**

| 类 | 职责 |
|---|---|
| `AddressPoolService` | `allocate(userId, chain, hdSeedKeyId)`：派生唯一 hdPath、落 `hd_path` + `wallet_address`、签发 `KeyRef` |

**`WalletAutoConfiguration` + `META-INF/spring/...AutoConfiguration.imports`**

让 wallet 子包被 bootstrap 自动装配；`@ComponentScan("com.exchange.wallet")` + `@MapperScan(...)` 显式列子包。

核心代码片段（`LedgerServiceImpl.freeze`）：
```java
@Transactional
public void freeze(long userId, long coinId, BigDecimal amount, ...) {
    AccountEntity acc = ensureAccount(userId, coinId);
    require(acc.getAvailable().compareTo(amount) >= 0, "insufficient available");
    BigDecimal newAvailable = acc.getAvailable().subtract(amount);
    BigDecimal newFrozen = acc.getFrozen().add(amount);
    casOrThrow(accountMapper.casUpdate(acc.getId(), newAvailable, newFrozen, acc.getVersion()));
    insertJournal(traceId, acc.getId(), coinId, bizType, bizId, DEBIT, amount, newAvailable, ...);
    insertJournal(traceId, acc.getId(), coinId, bizType, bizId, CREDIT, amount, newFrozen, ...);
}
```

### Task 清单

- [ ] **Task 32**: 14 个 Entity（按 V2 SQL 字段映射，Lombok @Data + @TableName）
- [ ] **Task 33**: 14 个 Mapper（11 个空 BaseMapper + AccountMapper.casUpdate + AccountJournalMapper.sumDirectionalAmount）
- [ ] **Task 34**: SystemAccountConstants + BizType + JournalDirection 枚举
- [ ] **Task 35**: LedgerCommand DTO + LedgerService 接口（6 个方法）
- [ ] **Task 36**: LedgerServiceImpl（CAS update + 双 journal insert + ensureAccount）
- [ ] **Task 37**: LedgerServiceImplIT 集成测试（Testcontainers MySQL）
  - 凑零不变量：`SUM(direction × amount) GROUP BY coin_id == 0`
  - 同 traceId 重入：第二次 credit 抛 `DuplicateKeyException`
  - freeze → settle 余额轨迹正确
- [ ] **Task 38**: AddressPoolService（HD path 不重复分配）
- [ ] **Task 39**: WalletAutoConfiguration + AutoConfiguration.imports + bootstrap 起得来

---

## Phase 9 — 验收 ⏳ 待实施

### 验收三层次

```
1. 编译/测试层：mvn clean install 全绿
2. 启动/健康层：bootstrap 起得来，actuator/health UP，19 张表齐全
3. 关键不变量层：
     - LedgerServiceImplIT 凑零不变量 PASS
     - MqIntegrationTest outbox→Kafka→consumed 全链路 PASS
     - SignerImplTest 签名后私钥被清零 PASS
```

### 不在本期验收范围

- 链上端到端（充值入账 / 提现广播）→ Plan 2 落
- 归集 / 冷热分层 / 对账 → Plan 3 落
- BTC / TRON 链实现 → Plan 4 / 5 落

### Task 清单

- [x] **Task 40.1**: `mvn -q clean package` BUILD SUCCESS
- [x] **Task 40.2**: `mvn -q test` 全绿（8 common + 36 wallet 测试，含 4 LedgerServiceImplIT）
- [x] **Task 40.3**: 启动 bootstrap + curl actuator/health = UP（db / redis / ping 全 UP）
- [x] **Task 40.4**: drop database 后重启 → Flyway 重放 V2 → `show tables` 出 19 张业务表 + `flyway_schema_history`
- [x] **Task 40.5**: 沉淀面试题文档到 `question/`（已落地，按 CLAUDE.md "严禁同主题分散到多个文件" 规则归并）：
  - ✅ `question/wallet-double-entry-ledger.md`（**新建**）— 双账法 / `uk(trace_id, direction, account_id)` 三字段幂等闸 / 凑零不变量 / CAS / balance_after / append-only / @PostConstruct 系统账户 / 易踩坑表
  - ✅ `question/mq-outbox.md`（**已存在**，§1-§3 outbox 模式 / `Propagation.MANDATORY` / OutboxRelay 数值取舍；§4-§5 消费侧幂等 / 异常二分 + DLT；§6-§7 Producer 配置 / EventEnvelope）— 涵盖原计划 `wallet-mq-outbox.md` + `wallet-idempotent-consumer.md` 全部点
  - ✅ `question/wallet-signer.md`（**已存在**）— 私钥生命周期 / AES-256-GCM / KmsProvider / BIP39/32/44 / ChainSpecificSigner 路由 / byte[] vs String / SecureRandom 熵源；涵盖原计划 `wallet-key-management.md` 全部点
  - ✅ `question/wallet-nonce-allocation.md`（**新建**）— DB 乐观锁 CAS / `(chain, address)` + version / `REQUIRES_NEW` 短事务 / reconcile 只升不降 / nonce 空洞 + 加速取消 / 易踩坑表
- [x] **Task 40.6**: roadmap 标记 Plan 1 ✅ DONE

---

## 完成判据

- ✅ `mvn clean test` 全绿
- ✅ bootstrap 启动正常，actuator/health UP
- ✅ Flyway 在干净库中可重放，建出 19 张表
- ✅ 双账法跑过"凑零不变量"集成测试
- ✅ Plan 1 涉及的核心面试题已沉淀到 `question/`

**Plan 2（ETH 端到端）起跑前置**：本 Plan 全部 task 完成 + 验收通过。

---

## 附录 A：当前进度速查

| Phase | 状态 | 说明 |
|---|---|---|
| Phase 1 父 pom | ✅ 已完成 | 7 类依赖版本管理 |
| Phase 2 数据库 V2 | ✅ 已完成 | 19 张表全部 DDL |
| Phase 3 common.mq | ✅ 已完成 | outbox + idempotent consumer + 集成测试 |
| Phase 4 chain.api SPI | ✅ 已完成 | 5 SPI + Signer + 10 DTO |
| Phase 5 wallet.signer | ✅ 已完成 | AES-GCM + KMS + BIP39/32 + Signer 路由+清零 |
| Phase 6 wallet.nonce | ✅ 已完成 | Task 29-30 |
| Phase 7 wallet.fee | ✅ 已完成 | Task 31 |
| Phase 8 wallet.core | ✅ 已完成 | Task 32-39（14 entity + ledger + 地址池 + 自动装配） |
| Phase 9 验收 | ✅ 已完成 | Task 40（mvn 全绿 / bootstrap UP / Flyway 重放 / 面试题沉淀） |

## 附录 B：核心代码导航

如果在面试或 review 时需要快速定位关键代码：

- **outbox 模式核心**：`common/src/main/java/com/exchange/common/mq/outbox/TransactionalEventPublisherImpl.java`（`@Transactional(MANDATORY)`）+ `OutboxRelay.java`（ShedLock + 退避）
- **幂等消费核心**：`common/src/main/java/com/exchange/common/mq/IdempotentEventHandler.java`（exists → handle → markConsumed）
- **签名隔离核心**：`wallet/src/main/java/com/exchange/wallet/signer/SignerImpl.java`（try/finally 清零）
- **HD 派生**：`wallet/src/main/java/com/exchange/wallet/signer/hd/Bip32HdKeyDeriver.java`（`m/44'/60'/0'/0/index`）
- **链抽象边界**：`wallet/src/main/java/com/exchange/wallet/chain/api/`（5 SPI + Signer + 10 DTO）
- **数据库 schema**：`bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`
- **双账法（待实现）**：`wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerServiceImpl.java`
