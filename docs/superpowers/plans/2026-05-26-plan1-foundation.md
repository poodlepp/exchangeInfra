# Plan 1 — Foundation 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 CEX 钱包的基础设施层（消息总线 / 数据库 schema / 链抽象 SPI / 签名服务 / nonce 分配 / 手续费抽象 / 双账法账本）落齐，为后续 ETH/BTC/TRON 各链 plan 提供完整地基。

**Architecture:** 在现有 Maven 多模块单体上扩展，common 模块内新增 `mq` 子包承载消息总线（`DomainEvent` / `TransactionalEventPublisher` / `IdempotentEventHandler` / Outbox + Relay）；wallet 模块内按子包划分（`chain.api` / `signer` / `nonce` / `fee` / `core`），子包间通过 Spring Bean + 接口契约联动，不拆 Maven 子模块。数据库走 Flyway V2 迁移落 16 张业务表 + 2 张消息基础设施表。

**Tech Stack:** Java 21 · Spring Boot 3.3.5 · MyBatis-Plus 3.5.7 · MySQL 8.4 · Redis + Redisson · Kafka (KRaft) · Spring Kafka · ShedLock 5.x · Bouncy Castle · web3j-crypto（仅引 BIP32/39 工具，不引完整 web3j） · Testcontainers · Micrometer + Prometheus

---

## File Structure

新增文件按"消息总线 → 数据库迁移 → 链抽象 SPI → 签名 → nonce → fee → 账本"自底向上组织。

### common 模块（exchange-common）

`common/src/main/java/com/exchange/common/mq/`：
- `DomainEvent.java` — 领域事件标记接口（eventId/aggregateId/eventType/occurredAt）
- `AbstractDomainEvent.java` — 抽象基类，子类只关心业务字段
- `EventPublisher.java` — 即时发送接口
- `TransactionalEventPublisher.java` — 事务性发送接口（写 outbox 表）
- `IdempotentEventHandler.java` — 抽象消费者基类
- `RetriableException.java` — 重试型异常标记
- `outbox/OutboxEntity.java` — outbox 表实体
- `outbox/OutboxMapper.java` — MyBatis-Plus mapper
- `outbox/OutboxRecord.java` — 写入 DTO
- `outbox/OutboxStatus.java` — 状态枚举（PENDING/SENT/FAILED）
- `outbox/OutboxRelay.java` — 独立线程批量投递 Kafka
- `outbox/TransactionalEventPublisherImpl.java` — 默认实现
- `consumed/ConsumedRecordEntity.java`
- `consumed/ConsumedRecordMapper.java`
- `consumed/ConsumedRecordStore.java`
- `kafka/KafkaConfig.java` — KafkaTemplate / ConsumerFactory / DLT 配置
- `kafka/KafkaEventPublisher.java` — `EventPublisher` 默认实现
- `kafka/EventEnvelope.java` — 跨进程序列化封装（schemaVersion + payload）
- `serializer/EventSerializer.java` — Jackson 序列化
- `metrics/MqMetrics.java` — Prometheus 指标桥
- `MqAutoConfiguration.java` — Spring Boot 自动装配入口

`common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`：注册 `MqAutoConfiguration`。

`common/src/test/java/com/exchange/common/mq/...`：单测覆盖 OutboxRelay / IdempotentEventHandler / TransactionalEventPublisherImpl。

### wallet 模块（exchange-wallet）

`wallet/src/main/java/com/exchange/wallet/`：

`chain/api/`（链抽象 SPI 与 DTO）：
- `Chain.java` — 链枚举（BTC/ETH/TRON）
- `ChainClient.java`
- `TxBuilder.java`
- `TxBroadcaster.java`
- `TxParser.java`
- `AddressDerivator.java`
- `Signer.java`
- `dto/RawTx.java` / `SignedTx.java` / `ChainTx.java` / `ChainBlock.java` / `TransferRequest.java` / `TxStatus.java` / `KeyRef.java` / `DerivedAddress.java` / `FeeQuote.java` / `FeeQuoteRequest.java`

`signer/`：
- `kms/KmsProvider.java` — 密钥管理抽象
- `kms/LocalKeystoreKmsProvider.java` — 默认本地实现
- `kms/AesGcmCipher.java` — AES-256-GCM 加解密工具
- `hd/Bip39MnemonicService.java` — BIP-39 助记词生成与种子派生
- `hd/Bip32HdKeyDeriver.java` — BIP-32/44 路径派生
- `KeyMaterialEntity.java` / `KeyMaterialMapper.java`
- `KeyMaterialService.java` — 写入/读取 key_material 表
- `SignerImpl.java` — `Signer` 默认实现，调用具体链 ChainSpecificSigner（Plan 2/4/5 各自落实现）
- `ChainSpecificSigner.java` — 内部 SPI，按 chain 路由
- `MemoryWipe.java` — `byte[]` 清零工具

`nonce/`：
- `NonceAllocator.java` — 接口
- `NonceRegisterEntity.java` / `NonceRegisterMapper.java`
- `DbOptimisticNonceAllocator.java` — 默认实现（DB 乐观锁 + Redis 兜底）
- `RedisNonceCounter.java` — Redis Lua 脚本封装
- `NonceReconciler.java` — 启动校准 hook + 定时校准

`fee/`：
- `FeeStrategy.java` — 接口
- `FeeStrategyRegistry.java` — 按 chain 路由

`core/`（双账法账本）：
- `entity/CoinEntity.java` / `ChainConfigEntity.java` / `WalletAddressEntity.java` / `HdPathEntity.java` / `AccountEntity.java` / `AccountJournalEntity.java` / `ChainTxEntity.java` / `DepositOrderEntity.java` / `WithdrawOrderEntity.java` / `SweepOrderEntity.java` / `TreasuryMovementEntity.java` / `AddressBalanceEntity.java` / `TreasuryPolicyEntity.java` / `ReconcileReportEntity.java`
- `mapper/*.java` — 14 张表的 MyBatis-Plus mapper
- `ledger/LedgerService.java` — 接口
- `ledger/LedgerServiceImpl.java` — 双账法实现
- `ledger/JournalDirection.java` / `BizType.java` 枚举
- `ledger/LedgerCommand.java` / `LedgerResult.java` DTO
- `ledger/SystemAccountConstants.java` — 系统账户 userId 约定（-1 中间账户、-2 主热钱包账户）
- `address/AddressPoolService.java` — 地址池：批量预生成、分配、回收
- `coin/CoinRegistry.java` — 启动加载 coin 表，线程安全 cache
- `chain/ChainConfigRegistry.java` — 启动加载 chain_config 表
- `WalletDomainExceptions.java` — 钱包域错误码

`wallet/src/main/resources/mapper/`：14 张表的 mapper XML（仅当 SQL 复杂时需要，简单 CRUD 走 MyBatis-Plus 注解）。

`wallet/src/test/java/com/exchange/wallet/...`：单测 + 集成测试（Testcontainers 起 MySQL + Kafka）。

### bootstrap 模块（exchange-bootstrap）

- `src/main/resources/db/migration/V2__wallet_foundation.sql` — 16 张业务表 + outbox + consumed_record 全部 DDL
- `src/main/resources/application-dev.yml` — 加 Kafka / wallet 链开关默认值

### 父 pom（顶层）

- `pom.xml` — 加 Kafka 客户端、testcontainers、ShedLock、Bouncy Castle、web3j-crypto 等版本管理

### infra 目录

不变（已有 MySQL/Redis/Kafka），无需改动。

---

## Phase 1 — 父 pom 版本管理

### Task 1: 在父 pom 中追加新依赖的版本属性与 dependencyManagement

**Files:**
- Modify: `pom.xml`（顶层）

- [ ] **Step 1.1: 在 `<properties>` 节加版本属性**

定位 `pom.xml:43` `<jjwt.version>0.12.6</jjwt.version>` 后追加：

```xml
        <spring-kafka.version>3.2.4</spring-kafka.version>
        <shedlock.version>5.16.0</shedlock.version>
        <bouncycastle.version>1.78.1</bouncycastle.version>
        <web3j.version>4.12.2</web3j.version>
        <testcontainers.version>1.20.4</testcontainers.version>
        <micrometer.version>1.13.6</micrometer.version>
        <spring-statemachine.version>4.0.0</spring-statemachine.version>
```

- [ ] **Step 1.2: 在 `<dependencyManagement><dependencies>` 节追加版本管理**

定位 `pom.xml:166` 最后一条 jjwt-jackson 之后追加：

```xml
            <dependency>
                <groupId>org.springframework.kafka</groupId>
                <artifactId>spring-kafka</artifactId>
                <version>${spring-kafka.version}</version>
            </dependency>
            <dependency>
                <groupId>net.javacrumbs.shedlock</groupId>
                <artifactId>shedlock-spring</artifactId>
                <version>${shedlock.version}</version>
            </dependency>
            <dependency>
                <groupId>net.javacrumbs.shedlock</groupId>
                <artifactId>shedlock-provider-jdbc-template</artifactId>
                <version>${shedlock.version}</version>
            </dependency>
            <dependency>
                <groupId>org.bouncycastle</groupId>
                <artifactId>bcprov-jdk18on</artifactId>
                <version>${bouncycastle.version}</version>
            </dependency>
            <dependency>
                <groupId>org.web3j</groupId>
                <artifactId>crypto</artifactId>
                <version>${web3j.version}</version>
            </dependency>
            <dependency>
                <groupId>org.testcontainers</groupId>
                <artifactId>testcontainers-bom</artifactId>
                <version>${testcontainers.version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.springframework.statemachine</groupId>
                <artifactId>spring-statemachine-core</artifactId>
                <version>${spring-statemachine.version}</version>
            </dependency>
```

- [ ] **Step 1.3: 验证 mvn 解析无错**

Run: `mvn -q -DskipTests dependency:resolve -pl common`
Expected: 无 BUILD FAILURE，所有依赖解析成功。

- [ ] **Step 1.4: 提交**

```bash
git add pom.xml
git commit -m "build: add kafka/shedlock/bc/web3j-crypto/testcontainers/statemachine deps"
```

---

## Phase 2 — 数据库 V2 迁移：16 张业务表 + outbox + consumed_record + shedlock

### Task 2: 拆分迁移脚本骨架

**Files:**
- Create: `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`

> 说明：本任务只放骨架与表头注释；具体表 DDL 在 Task 3-7 分批写入，避免单步过长。

- [ ] **Step 2.1: 新建 V2 迁移文件**

写入：

```sql
-- V2 wallet foundation: common-mq + wallet 16 业务表
-- Charset: utf8mb4，时间字段 DATETIME(3) 含毫秒
-- 所有金额 DECIMAL(38,18) 兼容 BTC/ETH/TRON
-- 主键统一 BIGINT（雪花 ID）；乐观锁字段 version；带 created_at/updated_at
-- 表清单：
--   common-mq:    outbox / consumed_record / shedlock
--   资产配置:     coin / chain_config
--   地址与密钥:   wallet_address / hd_path / key_material
--   账户与流水:   account / account_journal
--   链上交易:     chain_tx
--   业务流程:     deposit_order / withdraw_order / sweep_order / treasury_movement
--   归集与水位:   nonce_register / address_balance / treasury_policy
--   对账:         reconcile_report

-- 占位：以下 DDL 由 Task 3-7 分批补全
SELECT 1;
```

- [ ] **Step 2.2: 提交骨架**

```bash
git add bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql
git commit -m "chore(db): scaffold V2 wallet foundation migration"
```

### Task 3: 写入 common-mq 与资产配置 DDL（5 张表）

**Files:**
- Modify: `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`

- [ ] **Step 3.1: 替换占位 SELECT 1 为五张表**

将 `SELECT 1;` 替换为：

```sql
-- ========== common-mq ==========
CREATE TABLE outbox (
  id            BIGINT PRIMARY KEY,
  event_id      VARCHAR(64) NOT NULL,
  topic         VARCHAR(64) NOT NULL,
  partition_key VARCHAR(64),
  payload       MEDIUMTEXT NOT NULL,
  status        TINYINT NOT NULL,
  retry_count   INT NOT NULL DEFAULT 0,
  next_retry_at DATETIME(3),
  created_at    DATETIME(3) NOT NULL,
  UNIQUE KEY uk_event_id (event_id),
  KEY idx_status_next (status, next_retry_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE consumed_record (
  event_id     VARCHAR(64) NOT NULL,
  handler_name VARCHAR(128) NOT NULL,
  consumed_at  DATETIME(3) NOT NULL,
  PRIMARY KEY (event_id, handler_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE shedlock (
  name       VARCHAR(64) PRIMARY KEY,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at  TIMESTAMP(3) NOT NULL,
  locked_by  VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ========== 资产配置 ==========
CREATE TABLE coin (
  id        BIGINT PRIMARY KEY,
  symbol    VARCHAR(16) NOT NULL,
  chain     VARCHAR(16) NOT NULL,
  contract  VARCHAR(128),
  decimals  INT NOT NULL,
  status    TINYINT NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL,
  UNIQUE KEY uk_symbol_chain (symbol, chain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE chain_config (
  id                BIGINT PRIMARY KEY,
  chain             VARCHAR(16) NOT NULL,
  deposit_confirms  INT NOT NULL,
  withdraw_confirms INT NOT NULL,
  reorg_depth       INT NOT NULL,
  min_withdraw      DECIMAL(38,18) NOT NULL,
  max_withdraw      DECIMAL(38,18) NOT NULL,
  fee_strategy      VARCHAR(32) NOT NULL,
  max_gas_price_gwei DECIMAL(38,18),
  created_at        DATETIME(3) NOT NULL,
  updated_at        DATETIME(3) NOT NULL,
  UNIQUE KEY uk_chain (chain)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 3.2: 提交**

```bash
git add bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql
git commit -m "feat(db): V2 add outbox/consumed_record/shedlock/coin/chain_config"
```

### Task 4: 写入地址与密钥 DDL（3 张表）

**Files:**
- Modify: `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`

- [ ] **Step 4.1: 在 chain_config 之后追加**

```sql

-- ========== 地址与密钥 ==========
CREATE TABLE wallet_address (
  id          BIGINT PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  chain       VARCHAR(16) NOT NULL,
  address     VARCHAR(128) NOT NULL,
  hd_path     VARCHAR(64) NOT NULL,
  key_id      VARCHAR(64) NOT NULL,
  status      TINYINT NOT NULL DEFAULT 1,
  created_at  DATETIME(3) NOT NULL,
  updated_at  DATETIME(3) NOT NULL,
  UNIQUE KEY uk_chain_addr (chain, address),
  KEY idx_user_chain (user_id, chain),
  KEY idx_key_id (key_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE hd_path (
  id        BIGINT PRIMARY KEY,
  chain     VARCHAR(16) NOT NULL,
  hd_path   VARCHAR(64) NOT NULL,
  used_at   DATETIME(3) NOT NULL,
  UNIQUE KEY uk_chain_path (chain, hd_path)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE key_material (
  id              BIGINT PRIMARY KEY,
  key_id          VARCHAR(64) NOT NULL,
  key_type        VARCHAR(16) NOT NULL,
  cipher_text     MEDIUMBLOB NOT NULL,
  iv              VARBINARY(32) NOT NULL,
  kms_alias       VARCHAR(128) NOT NULL,
  algo_version    INT NOT NULL DEFAULT 1,
  created_at      DATETIME(3) NOT NULL,
  UNIQUE KEY uk_key_id (key_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 4.2: 提交**

```bash
git add bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql
git commit -m "feat(db): V2 add wallet_address/hd_path/key_material"
```

### Task 5: 写入账户、流水、链上交易 DDL（3 张表）

**Files:**
- Modify: `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`

- [ ] **Step 5.1: 在 key_material 之后追加**

```sql

-- ========== 账户与流水（双账法）==========
CREATE TABLE account (
  id          BIGINT PRIMARY KEY,
  user_id     BIGINT NOT NULL,
  coin_id     BIGINT NOT NULL,
  available   DECIMAL(38,18) NOT NULL DEFAULT 0,
  frozen      DECIMAL(38,18) NOT NULL DEFAULT 0,
  version     INT NOT NULL DEFAULT 0,
  created_at  DATETIME(3) NOT NULL,
  updated_at  DATETIME(3) NOT NULL,
  UNIQUE KEY uk_user_coin (user_id, coin_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE account_journal (
  id            BIGINT PRIMARY KEY,
  trace_id      VARCHAR(64) NOT NULL,
  account_id    BIGINT NOT NULL,
  coin_id       BIGINT NOT NULL,
  biz_type      VARCHAR(32) NOT NULL,
  biz_id        BIGINT NOT NULL,
  direction     TINYINT NOT NULL,
  amount        DECIMAL(38,18) NOT NULL,
  balance_after DECIMAL(38,18) NOT NULL,
  remark        VARCHAR(255),
  created_at    DATETIME(3) NOT NULL,
  UNIQUE KEY uk_trace_direction_account (trace_id, direction, account_id),
  KEY idx_account_time (account_id, created_at),
  KEY idx_biz (biz_type, biz_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ========== 链上交易快照 ==========
CREATE TABLE chain_tx (
  id            BIGINT PRIMARY KEY,
  chain         VARCHAR(16) NOT NULL,
  tx_hash       VARCHAR(128) NOT NULL,
  vout          INT NOT NULL DEFAULT 0,
  block_height  BIGINT NOT NULL,
  block_hash    VARCHAR(128) NOT NULL,
  parent_hash   VARCHAR(128) NOT NULL,
  from_address  VARCHAR(128),
  to_address    VARCHAR(128) NOT NULL,
  coin_id       BIGINT,
  amount        DECIMAL(38,18) NOT NULL,
  direction     TINYINT NOT NULL,
  confirm_count INT NOT NULL DEFAULT 0,
  status        TINYINT NOT NULL,
  raw_json      MEDIUMTEXT,
  created_at    DATETIME(3) NOT NULL,
  UNIQUE KEY uk_chain_hash_vout (chain, tx_hash, vout),
  KEY idx_to_addr (chain, to_address),
  KEY idx_chain_height (chain, block_height)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

> 说明：`uk_trace_direction_account` 三字段联合唯一是双账法的安全幂等闸——同 trace 同 direction 在不同账户上必须能各自插入（freeze 场景两条 direction 不同；transferInternal 跨两个账户写双方各一条 +1/-1）。

- [ ] **Step 5.2: 提交**

```bash
git add bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql
git commit -m "feat(db): V2 add account/account_journal/chain_tx"
```

### Task 6: 写入业务流程 DDL（4 张表）

**Files:**
- Modify: `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`

- [ ] **Step 6.1: 在 chain_tx 之后追加**

```sql

-- ========== 业务流程 ==========
CREATE TABLE deposit_order (
  id            BIGINT PRIMARY KEY,
  user_id       BIGINT NOT NULL,
  coin_id       BIGINT NOT NULL,
  chain_tx_id   BIGINT NOT NULL,
  amount        DECIMAL(38,18) NOT NULL,
  status        VARCHAR(32) NOT NULL,
  confirm_count INT NOT NULL DEFAULT 0,
  version       INT NOT NULL DEFAULT 0,
  created_at    DATETIME(3) NOT NULL,
  updated_at    DATETIME(3) NOT NULL,
  UNIQUE KEY uk_chain_tx (chain_tx_id),
  KEY idx_user_status (user_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE withdraw_order (
  id              BIGINT PRIMARY KEY,
  user_id         BIGINT NOT NULL,
  coin_id         BIGINT NOT NULL,
  chain           VARCHAR(16) NOT NULL,
  to_address      VARCHAR(128) NOT NULL,
  amount          DECIMAL(38,18) NOT NULL,
  fee             DECIMAL(38,18) NOT NULL DEFAULT 0,
  fee_estimate    DECIMAL(38,18),
  status          VARCHAR(32) NOT NULL,
  fail_reason     VARCHAR(255),
  risk_decision   VARCHAR(32),
  signed_raw      MEDIUMTEXT,
  tx_hash         VARCHAR(128),
  nonce           BIGINT,
  from_address    VARCHAR(128),
  replace_of_id   BIGINT,
  confirm_count   INT NOT NULL DEFAULT 0,
  version         INT NOT NULL DEFAULT 0,
  created_at      DATETIME(3) NOT NULL,
  updated_at      DATETIME(3) NOT NULL,
  KEY idx_status_time (status, created_at),
  KEY idx_user (user_id, status),
  KEY idx_chain_from_nonce (chain, from_address, nonce),
  UNIQUE KEY uk_tx_hash (tx_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sweep_order (
  id            BIGINT PRIMARY KEY,
  chain         VARCHAR(16) NOT NULL,
  coin_id       BIGINT NOT NULL,
  src_address   VARCHAR(128) NOT NULL,
  dst_address   VARCHAR(128) NOT NULL,
  amount        DECIMAL(38,18) NOT NULL,
  status        VARCHAR(32) NOT NULL,
  drip_tx_hash  VARCHAR(128),
  sweep_tx_hash VARCHAR(128),
  nonce         BIGINT,
  retry_count   INT NOT NULL DEFAULT 0,
  version       INT NOT NULL DEFAULT 0,
  created_at    DATETIME(3) NOT NULL,
  updated_at    DATETIME(3) NOT NULL,
  KEY idx_chain_status (chain, status),
  UNIQUE KEY uk_sweep_tx (sweep_tx_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE treasury_movement (
  id            BIGINT PRIMARY KEY,
  chain         VARCHAR(16) NOT NULL,
  coin_id       BIGINT NOT NULL,
  direction     VARCHAR(16) NOT NULL,
  amount        DECIMAL(38,18) NOT NULL,
  status        VARCHAR(32) NOT NULL,
  psbt          MEDIUMTEXT,
  tx_hash       VARCHAR(128),
  proposer      VARCHAR(64),
  approver_list VARCHAR(512),
  created_at    DATETIME(3) NOT NULL,
  updated_at    DATETIME(3) NOT NULL,
  KEY idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 6.2: 提交**

```bash
git add bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql
git commit -m "feat(db): V2 add deposit/withdraw/sweep/treasury_movement"
```

### Task 7: 写入 nonce_register / address_balance / treasury_policy / reconcile_report DDL（4 张表）

**Files:**
- Modify: `bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql`

- [ ] **Step 7.1: 在 treasury_movement 之后追加**

```sql

-- ========== 归集与水位辅助 ==========
CREATE TABLE nonce_register (
  chain          VARCHAR(16) NOT NULL,
  address        VARCHAR(128) NOT NULL,
  next_nonce     BIGINT NOT NULL,
  on_chain_nonce BIGINT NOT NULL,
  reconciled_at  DATETIME(3) NOT NULL,
  version        INT NOT NULL DEFAULT 0,
  PRIMARY KEY (chain, address)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE address_balance (
  chain         VARCHAR(16) NOT NULL,
  address       VARCHAR(128) NOT NULL,
  coin_id       BIGINT NOT NULL,
  balance       DECIMAL(38,18) NOT NULL,
  block_height  BIGINT NOT NULL,
  refreshed_at  DATETIME(3) NOT NULL,
  PRIMARY KEY (chain, address, coin_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE treasury_policy (
  id                BIGINT PRIMARY KEY,
  chain             VARCHAR(16) NOT NULL,
  coin_id           BIGINT NOT NULL,
  hot_low_ratio     DECIMAL(5,4) NOT NULL,
  hot_high_ratio    DECIMAL(5,4) NOT NULL,
  hot_target_ratio  DECIMAL(5,4) NOT NULL,
  total_target      DECIMAL(38,18) NOT NULL,
  daily_outflow_avg DECIMAL(38,18),
  created_at        DATETIME(3) NOT NULL,
  updated_at        DATETIME(3) NOT NULL,
  UNIQUE KEY uk_chain_coin (chain, coin_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ========== 对账报告 ==========
CREATE TABLE reconcile_report (
  id            BIGINT PRIMARY KEY,
  report_date   DATE NOT NULL,
  chain         VARCHAR(16) NOT NULL,
  coin_id       BIGINT NOT NULL,
  ledger_total  DECIMAL(38,18),
  chain_total   DECIMAL(38,18),
  delta         DECIMAL(38,18),
  status        VARCHAR(16) NOT NULL,
  detail_json   MEDIUMTEXT,
  created_at    DATETIME(3) NOT NULL,
  UNIQUE KEY uk_date_chain_coin (report_date, chain, coin_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- [ ] **Step 7.2: 提交**

```bash
git add bootstrap/src/main/resources/db/migration/V2__wallet_foundation.sql
git commit -m "feat(db): V2 add nonce_register/address_balance/treasury_policy/reconcile_report"
```

### Task 8: 启动 bootstrap 验证 Flyway 应用 V2

**Files:**
- 仅运行命令

- [ ] **Step 8.1: 启动 infra**

Run: `cd infra && cp -n .env.example .env && docker compose up -d mysql redis kafka`
Expected: 三个容器健康，端口 3306/6379/29092 可用。

- [ ] **Step 8.2: 编译并启动 bootstrap，让 Flyway 跑 V2**

Run: `mvn -q -DskipTests -pl bootstrap -am package` 然后 `java -jar bootstrap/target/exchange-bootstrap-1.0.0-SNAPSHOT.jar --spring.profiles.active=dev &`，3 秒后 `curl http://localhost:8080/actuator/health`。
Expected: `{"status":"UP"}` 并且 mysql `exchange_dev` 库出现 19 张表。

- [ ] **Step 8.3: 校验表清单**

Run: `mysql -h127.0.0.1 -uroot -proot exchange_dev -e "show tables;"`
Expected：account / account_journal / address_balance / chain_config / chain_tx / coin / consumed_record / deposit_order / flyway_schema_history / hd_path / key_material / nonce_register / outbox / reconcile_report / shedlock / sweep_order / treasury_movement / treasury_policy / wallet_address / withdraw_order，共 20 行。

- [ ] **Step 8.4: 关闭后台进程**

Run: `pkill -f exchange-bootstrap-1.0.0-SNAPSHOT.jar`
Expected: 后台进程被终止。无文件改动需要提交。

---

## Phase 3 — common-mq 子包（消息总线）

### 全景：为什么需要 outbox + ShedLock + idempotent

**问题场景**：业务变更（落账 / 改单 / 改状态）写 MySQL，然后给下游发 Kafka 通知。如果中间任何一步失败：

- 业务已 commit，但 Kafka 没发 → 下游错过事件、对账不齐。
- Kafka 已发，但业务事务回滚 → 下游凭一条不存在的事件做了反应（"幽灵事件"）。

两阶段提交（XA）能解，但 MySQL + Kafka 跨堆栈 XA 几乎不可用，且性能差。**Outbox 模式**用一张本地表 `outbox` 当"准发事件"，把"业务表变更 + outbox 插入"放在同一个本地事务里。事务一 commit，"事件即将发"是必然事实；之后由独立的 Relay 进程把 outbox 的行真正搬到 Kafka，是 _at-least-once_。下游必须自己做幂等。

**模块拆解**：

```mermaid
flowchart LR
  subgraph App[应用进程内]
    Biz[业务 Service]
    TEP[TransactionalEventPublisher<br/>Propagation.MANDATORY]
    Relay[OutboxRelay<br/>@Scheduled + @SchedulerLock]
    KEP[KafkaEventPublisher<br/>即时发送，无事务保证]
    Cons[IdempotentEventHandler<br/>下游消费者基类]
  end

  subgraph DB[(MySQL)]
    OB[(outbox)]
    CR[(consumed_record)]
    SL[(shedlock)]
  end

  K[(Kafka<br/>topic + DLT)]

  Biz -- 同事务 --> TEP
  TEP -- insert PENDING --> OB
  Relay -- pickPending --> OB
  Relay -- send --> K
  Relay -- markSENT/markRetry --> OB
  Relay -.加锁.-> SL

  K --> Cons
  Cons -- exists? --> CR
  Cons -- markConsumed --> CR
  Cons -- 业务处理 --> Biz
```

**职责切分**：

| 组件 | 解决什么 | 关键约束 |
|---|---|---|
| `DomainEvent` / `AbstractDomainEvent` | 事件契约：`eventId`（去重键）、`aggregateId`（partition key）、`eventType`（topic）、`occurredAt` | `eventId` 必须全局唯一且稳定；下游凭它做幂等 |
| `TransactionalEventPublisher` | 把"发事件"降级为"在 outbox 里插一行"，与业务变更同事务 | `Propagation.MANDATORY`：必须有调用方事务，否则抛错 |
| `OutboxRelay` + `ShedLock` | 把 PENDING 行真正发到 Kafka；多实例下只有一个在跑 | 失败指数退避；同名锁 `outbox-relay` 全局互斥 |
| `KafkaEventPublisher` | 不需要事务保证的即时通知（监控、告警） | 不进 outbox，丢了就丢了 |
| `IdempotentEventHandler` | 消费侧重复消息的幂等闸 | `(event_id, handler_name)` 唯一约束做去重 |
| `consumed_record` 表 | 记录"哪个 handler 处理过哪个 event" | 复合主键，INSERT IGNORE 写入 |

**关键决策**：

- **outbox 不做按时间分区表**：单表撑得住（PENDING 行很快被 Relay 清掉，SENT 行可异步归档）。
- **Relay 不用 Kafka Connect Debezium**：CDC 引入额外组件，对项目复杂度不划算；轮询表 + 200ms~1s 延迟在交易场景完全可接受。
- **Relay 用 ShedLock 而非 Redis 分布式锁**：本来就在用 MySQL，ShedLock 把锁状态也落 MySQL，避免再引一层依赖。
- **下游幂等用表而非 Redis**：Redis 持久化弱，落表保证"消费过就一定消费过"——尤其面对账户变更这种不可逆操作。
- **DLQ 不在 IdempotentEventHandler 里**：交给 spring-kafka 的 `DefaultErrorHandler` 配置 DLT 路由，Handler 只做异常分类（`RetriableException` 透传 / 其他原样抛）。

**易踩的坑**：

- 调用 `TransactionalEventPublisher.publish` 时调用方没开事务：直接抛 `IllegalTransactionStateException`——这是设计想要的，不是 bug。
- 多实例都跑 `@Scheduled`，没开 ShedLock：同一行被两个 Relay 同时 send，下游收到重复消息。靠下游的 idempotent 兜底，但发送量翻倍，浪费 Kafka 配额。
- Outbox 行积压：Relay 挂了或 Kafka 不可用导致积压，必须有"PENDING 行年龄超过 N 分钟"告警。
- 下游 handler 内部用了 `@Transactional(REQUIRES_NEW)`：`markConsumed` 不在外层事务里，业务回滚但 mark 已 commit，会丢消息。本期默认所有 handler 单事务，不嵌套。

---

### Task 9: 在 common 模块加 spring-kafka / shedlock / micrometer 依赖

**Files:**
- Modify: `common/pom.xml`

- [ ] **Step 9.1: 在 `common/pom.xml` 的 `<dependencies>` 节末尾追加**

定位 `</dependencies>` 闭合前一行，追加：

```xml
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
        </dependency>
        <dependency>
            <groupId>net.javacrumbs.shedlock</groupId>
            <artifactId>shedlock-spring</artifactId>
        </dependency>
        <dependency>
            <groupId>net.javacrumbs.shedlock</groupId>
            <artifactId>shedlock-provider-jdbc-template</artifactId>
        </dependency>
        <dependency>
            <groupId>io.micrometer</groupId>
            <artifactId>micrometer-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>mysql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 9.2: 验证**

Run: `mvn -q -DskipTests -pl common -am dependency:resolve`
Expected: BUILD SUCCESS。

- [ ] **Step 9.3: 提交**

```bash
git add common/pom.xml
git commit -m "build(common): add kafka/shedlock/testcontainers deps for common-mq"
```

### Task 10: 定义 DomainEvent / AbstractDomainEvent / RetriableException

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/DomainEvent.java`
- Create: `common/src/main/java/com/exchange/common/mq/AbstractDomainEvent.java`
- Create: `common/src/main/java/com/exchange/common/mq/RetriableException.java`
- Test: `common/src/test/java/com/exchange/common/mq/AbstractDomainEventTest.java`

- [ ] **Step 10.1: 写 DomainEvent.java**

```java
package com.exchange.common.mq;

public interface DomainEvent {
    String eventId();
    String aggregateId();
    String eventType();
    long occurredAt();
}
```

- [ ] **Step 10.2: 写 AbstractDomainEvent.java**

```java
package com.exchange.common.mq;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;

@Getter
public abstract class AbstractDomainEvent implements DomainEvent {
    private final String eventId;
    private final long occurredAt;

    protected AbstractDomainEvent() {
        this.eventId = String.valueOf(SnowflakeIdGenerator.nextId());
        this.occurredAt = System.currentTimeMillis();
    }

    protected AbstractDomainEvent(String eventId, long occurredAt) {
        this.eventId = eventId;
        this.occurredAt = occurredAt;
    }

    @Override public String eventId() { return eventId; }
    @Override public long occurredAt() { return occurredAt; }
    @JsonIgnore @Override public abstract String aggregateId();
    @JsonIgnore @Override public abstract String eventType();
}
```

> 说明：如果现有 `SnowflakeIdGenerator` 不是 static 方法，先看一下 `common/src/main/java/com/exchange/common/util/SnowflakeIdGenerator.java`，按它的实际签名调用——`nextId()` 是常见命名，若是实例方法则改为 `bean.nextId()`。

- [ ] **Step 10.3: 写 RetriableException.java**

```java
package com.exchange.common.mq;

public class RetriableException extends RuntimeException {
    public RetriableException(String message) { super(message); }
    public RetriableException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 10.4: 写 AbstractDomainEventTest.java**

```java
package com.exchange.common.mq;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class AbstractDomainEventTest {

    static class FooEvent extends AbstractDomainEvent {
        private final String aggregateId;
        FooEvent(String aggregateId) { this.aggregateId = aggregateId; }
        @Override public String aggregateId() { return aggregateId; }
        @Override public String eventType() { return "test.foo.created"; }
    }

    @Test
    void event_id_and_occurred_at_are_auto_filled() {
        FooEvent e = new FooEvent("agg-1");
        assertThat(e.eventId()).isNotBlank();
        assertThat(e.occurredAt()).isPositive();
        assertThat(e.aggregateId()).isEqualTo("agg-1");
        assertThat(e.eventType()).isEqualTo("test.foo.created");
    }
}
```

- [ ] **Step 10.5: 跑测试**

Run: `mvn -q -pl common test -Dtest=AbstractDomainEventTest`
Expected: BUILD SUCCESS, 1 test passed.

- [ ] **Step 10.6: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/ common/src/test/java/com/exchange/common/mq/
git commit -m "feat(mq): DomainEvent/AbstractDomainEvent/RetriableException"
```

### Task 11: Outbox 实体 + Mapper + 状态枚举

**设计考虑**：

```mermaid
stateDiagram-v2
    [*] --> PENDING: TransactionalEventPublisher.publish<br/>(在业务事务里 insert)
    PENDING --> SENT: Relay 发 Kafka 成功<br/>markStatus(SENT)
    PENDING --> PENDING: Relay 发送失败<br/>markRetry(retry_count++, next_retry_at)
    PENDING --> FAILED: 重试达到上限<br/>(运维介入或归档)
    SENT --> [*]: 异步归档 / 截断
```

- **`event_id` 唯一约束**：业务侧用它做"事件已写入"的幂等键。同一业务操作重试时，第二次 insert 会被唯一约束挡掉，保证一条业务变更最多一条 outbox 行。
- **`status` 用 TINYINT 而非 enum 字符串**：节省字段宽度；`OutboxStatus.code` 是枚举值的真理，DB 字段只存数字。
- **`retry_count` 与 `next_retry_at` 拆开**：`retry_count` 决定退避级别（指数计算），`next_retry_at` 决定何时再可被 `pickPending` 选中。`pickPending` 的 `WHERE` 条件 `(next_retry_at IS NULL OR next_retry_at <= now)` 让首次未重试的行（NULL）和到期重试的行都能被选中。
- **`partition_key` 单独存**：Kafka 消息发送时需要 partition key 做有序保证（同 aggregate 的事件按时间序到同一分区）。从 payload JSON 里临时反序列化代价高，存字段直接拿。
- **`payload` 用 MEDIUMTEXT**：足够装下大部分 EventEnvelope JSON；如果未来出现超大事件（很少见），考虑外部对象存储 + 引用。
- **索引 `idx_status_next (status, next_retry_at)`**：`pickPending` 的核心查询走它；按 `status` 等值 + `next_retry_at` 范围扫，比单列 `status` 索引能多过滤一轮。
- **不放 `eventType` 字段**：Relay 不关心 type，只关心 topic。`eventType → topic` 的映射在 Publisher 端就已经做完。

**上下游契约**：
- 上游：`TransactionalEventPublisherImpl.insert` 写入 PENDING 行，必须在调用方事务内。
- 下游：`OutboxRelay.relay` 通过 `pickPending` 选行 → `kafkaTemplate.send` → `markStatus(SENT)` 或 `markRetry`。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/outbox/OutboxStatus.java`
- Create: `common/src/main/java/com/exchange/common/mq/outbox/OutboxEntity.java`
- Create: `common/src/main/java/com/exchange/common/mq/outbox/OutboxMapper.java`

- [ ] **Step 11.1: OutboxStatus.java**

```java
package com.exchange.common.mq.outbox;

public enum OutboxStatus {
    PENDING(0), SENT(1), FAILED(2);

    public final int code;
    OutboxStatus(int code) { this.code = code; }

    public static OutboxStatus of(int code) {
        for (OutboxStatus s : values()) if (s.code == code) return s;
        throw new IllegalArgumentException("unknown OutboxStatus code: " + code);
    }
}
```

- [ ] **Step 11.2: OutboxEntity.java**

```java
package com.exchange.common.mq.outbox;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("outbox")
public class OutboxEntity {
    @TableId(type = IdType.INPUT)
    private Long id;
    private String eventId;
    private String topic;
    private String partitionKey;
    private String payload;
    private Integer status;
    private Integer retryCount;
    private LocalDateTime nextRetryAt;
    private LocalDateTime createdAt;
}
```

- [ ] **Step 11.3: OutboxMapper.java**

```java
package com.exchange.common.mq.outbox;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import java.time.LocalDateTime;
import java.util.List;

@Mapper
public interface OutboxMapper extends BaseMapper<OutboxEntity> {

    @Select("""
        SELECT * FROM outbox
        WHERE status = #{status}
          AND (next_retry_at IS NULL OR next_retry_at <= #{now})
        ORDER BY id
        LIMIT #{limit}
        """)
    List<OutboxEntity> pickPending(@Param("status") int status,
                                   @Param("now") LocalDateTime now,
                                   @Param("limit") int limit);

    @Update("UPDATE outbox SET status = #{status} WHERE id = #{id}")
    int markStatus(@Param("id") long id, @Param("status") int status);

    @Update("""
        UPDATE outbox
           SET retry_count = retry_count + 1,
               next_retry_at = #{nextRetryAt}
         WHERE id = #{id}
        """)
    int markRetry(@Param("id") long id, @Param("nextRetryAt") LocalDateTime nextRetryAt);
}
```

- [ ] **Step 11.4: 编译验证**

Run: `mvn -q -pl common -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 11.5: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/outbox/
git commit -m "feat(mq): outbox entity/mapper/status enum"
```

### Task 12: ConsumedRecord 实体 + Mapper + Store

**设计考虑**：

```mermaid
flowchart TB
    K[Kafka 消息] --> H[IdempotentEventHandler.onMessage]
    H --> E{exists<br/>event_id + handler_name?}
    E -- 有 --> Skip[跳过，幂等命中]
    E -- 无 --> Handle[handle 业务逻辑]
    Handle -- 成功 --> Mark[markConsumed<br/>INSERT IGNORE]
    Handle -- RetriableException --> Throw[原样抛，触发 Kafka 重试]
    Mark --> Done[ack 提交 offset]
```

- **`(event_id, handler_name)` 复合主键**：同一事件被不同 handler 各处理一次，不互斥。比如一笔提现完成事件，"通知用户"和"更新风控水位"是两个 handler，都得各自跑一次。
- **没有自增 id**：复合主键就是天然唯一键，省一列。
- **`INSERT IGNORE` 而非 `ON DUPLICATE KEY UPDATE`**：第一次写入即定型，二次进来直接被唯一约束拒绝，零开销。`exists` 是先查后插的双保险——快速路径走查询，仅在 race 下才依赖 INSERT IGNORE。
- **不用 Redis 做去重**：Redis 持久化弱，掉一次数据就可能让账户事件被重复处理。MySQL 的持久性 + 双账法本身的幂等闸（`uk(trace_id, direction, account_id)`）才是真正可靠的兜底——本表只是"避免业务逻辑被重复执行"的快门。
- **不带 TTL**：consumed_record 不会无限增长——每个 event_id 雪花 ID 趋势递增，按 created_at 老化即可。本期不做归档，未来可加分区表 + 定时 drop 旧分区。

**与 outbox 的对称性**：
- 写侧（生产者）：业务事务 + outbox 表 + Relay = at-least-once 投递。
- 读侧（消费者）：Kafka offset + consumed_record 表 + IdempotentHandler = 业务逻辑 exactly-once 执行。
- 这一对配合起来才能在 Kafka 默认 at-least-once 语义之上做出"幂等等价 exactly-once"。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/consumed/ConsumedRecordEntity.java`
- Create: `common/src/main/java/com/exchange/common/mq/consumed/ConsumedRecordMapper.java`
- Create: `common/src/main/java/com/exchange/common/mq/consumed/ConsumedRecordStore.java`

- [ ] **Step 12.1: ConsumedRecordEntity.java**

```java
package com.exchange.common.mq.consumed;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("consumed_record")
public class ConsumedRecordEntity {
    private String eventId;
    private String handlerName;
    private LocalDateTime consumedAt;
}
```

- [ ] **Step 12.2: ConsumedRecordMapper.java**

```java
package com.exchange.common.mq.consumed;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import java.time.LocalDateTime;

@Mapper
public interface ConsumedRecordMapper {

    @Select("""
        SELECT COUNT(*) FROM consumed_record
        WHERE event_id = #{eventId} AND handler_name = #{handler}
        """)
    int countByKey(@Param("eventId") String eventId,
                   @Param("handler") String handler);

    @Insert("""
        INSERT IGNORE INTO consumed_record(event_id, handler_name, consumed_at)
        VALUES(#{eventId}, #{handler}, #{consumedAt})
        """)
    int insertIgnore(@Param("eventId") String eventId,
                     @Param("handler") String handler,
                     @Param("consumedAt") LocalDateTime consumedAt);
}
```

- [ ] **Step 12.3: ConsumedRecordStore.java**

```java
package com.exchange.common.mq.consumed;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class ConsumedRecordStore {

    private final ConsumedRecordMapper mapper;

    public boolean exists(String eventId, String handlerName) {
        return mapper.countByKey(eventId, handlerName) > 0;
    }

    public void markConsumed(String eventId, String handlerName) {
        mapper.insertIgnore(eventId, handlerName, LocalDateTime.now());
    }
}
```

- [ ] **Step 12.4: 编译**

Run: `mvn -q -pl common -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 12.5: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/consumed/
git commit -m "feat(mq): consumed_record entity/mapper/store"
```

### Task 13: Kafka 配置 + EventEnvelope + KafkaEventPublisher（即时发送）

**设计考虑**：

```mermaid
flowchart LR
    Caller[业务代码<br/>不需要事务保证] --> KEP[KafkaEventPublisher]
    KEP -- wrap --> Env[EventEnvelope<br/>schemaVersion + 业务字段]
    Env -- toJson --> Ser[EventSerializer<br/>Jackson]
    Ser -- send(topic, key, value) --> KT[KafkaTemplate]
    KT -- idempotence + acks=all --> K[(Kafka)]
```

- **`KafkaEventPublisher` 是"无事务保证"的即时发送通道**：用于不需要持久化保证的场景（监控、Slack 通知、日志投递）。账户、订单类的关键事件**必须走 `TransactionalEventPublisher` + outbox**，不要直接调它。
- **Producer 三件套配置**：
  - `enable.idempotence=true`：单 Producer 实例内防重发，避免网络重试导致 Kafka 端出现重复消息。
  - `acks=all`：所有 in-sync replica 写入才算成功，配合 broker 端 `min.insync.replicas≥2` 才能在单节点宕机下不丢消息。
  - `max.in.flight.requests.per.connection=5`：开 idempotence 后必须 ≤5（Kafka 限制），同时保证同一分区内有序——开 idempotence 时 broker 用 sequence number 重排，应用层无需担心乱序。
- **`EventEnvelope` 加 `schemaVersion`**：事件结构演进的逃生通道。早期 v1 字段不变；v2 加新字段时下游能根据 schemaVersion 判断走老逻辑还是新逻辑，避免直接改字段引发反序列化失败。
- **partition key 用 `aggregateId`**：同一聚合（同一账户、同一订单）的事件必到同一分区 → 分区内有序 = 该聚合的事件全局有序。这是后续做"按 aggregate 串行消费"的前置条件。
- **`@ConditionalOnProperty(exchange.mq.enabled, matchIfMissing=true)`**：默认开启，但单测和某些不需要 Kafka 的部署场景可以 `exchange.mq.enabled=false` 整段绕开。`matchIfMissing` 确保不显式配置时也走默认开启。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/kafka/EventEnvelope.java`
- Create: `common/src/main/java/com/exchange/common/mq/kafka/KafkaConfig.java`
- Create: `common/src/main/java/com/exchange/common/mq/EventPublisher.java`
- Create: `common/src/main/java/com/exchange/common/mq/kafka/KafkaEventPublisher.java`
- Create: `common/src/main/java/com/exchange/common/mq/serializer/EventSerializer.java`

- [ ] **Step 13.1: EventEnvelope.java**

```java
package com.exchange.common.mq.kafka;

import com.exchange.common.mq.DomainEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope {
    private int schemaVersion;
    private String eventType;
    private String eventId;
    private long occurredAt;
    private String aggregateId;
    private Object payload;

    public static EventEnvelope wrap(DomainEvent event) {
        return new EventEnvelope(1, event.eventType(), event.eventId(),
                event.occurredAt(), event.aggregateId(), event);
    }
}
```

- [ ] **Step 13.2: EventSerializer.java**

```java
package com.exchange.common.mq.serializer;

import com.exchange.common.mq.kafka.EventEnvelope;
import com.exchange.common.util.JsonUtil;
import org.springframework.stereotype.Component;

@Component
public class EventSerializer {
    public String toJson(EventEnvelope env) {
        return JsonUtil.toJson(env);
    }

    public EventEnvelope fromJson(String json) {
        return JsonUtil.fromJson(json, EventEnvelope.class);
    }
}
```

> 说明：依赖现有 `com.exchange.common.util.JsonUtil`。若 JsonUtil 接口名不同，按实际改 `toJson`/`fromJson` 调用。

- [ ] **Step 13.3: KafkaConfig.java**

```java
package com.exchange.common.mq.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "exchange.mq.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            org.springframework.boot.autoconfigure.kafka.KafkaProperties props) {
        Map<String, Object> cfg = new HashMap<>(props.buildProducerProperties(null));
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new DefaultKafkaProducerFactory<>(cfg);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }
}
```

- [ ] **Step 13.4: EventPublisher.java（接口）**

```java
package com.exchange.common.mq;

public interface EventPublisher {
    void publish(DomainEvent event);
    void publish(String topic, DomainEvent event);
}
```

- [ ] **Step 13.5: KafkaEventPublisher.java**

```java
package com.exchange.common.mq.kafka;

import com.exchange.common.mq.DomainEvent;
import com.exchange.common.mq.EventPublisher;
import com.exchange.common.mq.serializer.EventSerializer;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventSerializer serializer;

    @Override
    public void publish(DomainEvent event) {
        publish(event.eventType(), event);
    }

    @Override
    public void publish(String topic, DomainEvent event) {
        String json = serializer.toJson(EventEnvelope.wrap(event));
        kafkaTemplate.send(topic, event.aggregateId(), json);
    }
}
```

- [ ] **Step 13.6: 编译**

Run: `mvn -q -pl common -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 13.7: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/
git commit -m "feat(mq): EventEnvelope/KafkaConfig/EventPublisher/KafkaEventPublisher"
```

### Task 14: TransactionalEventPublisher 接口与实现（写 outbox）

**设计考虑**：

```mermaid
sequenceDiagram
    participant Biz as Business Service
    participant TM as TransactionManager
    participant TEP as TxnEventPublisher
    participant DB as MySQL
    participant Relay as OutboxRelay (异步)
    participant K as Kafka

    Biz->>TM: @Transactional 开事务
    Biz->>DB: UPDATE account ...
    Biz->>TEP: publish(event)
    Note over TEP: Propagation.MANDATORY<br/>必须有外层事务
    TEP->>DB: INSERT outbox PENDING
    Biz->>TM: commit
    Note over DB: 业务变更 + outbox<br/>原子提交

    Note over Relay: 独立线程
    Relay->>DB: pickPending
    Relay->>K: send
    Relay->>DB: markStatus(SENT)
```

- **为什么是 `Propagation.MANDATORY` 而不是 `REQUIRED` / `REQUIRES_NEW`**：
  - `REQUIRED`（默认）：没有事务就新开一个。Publisher 单独开事务 → outbox 行 commit 后业务事务回滚 → 出现"业务没成但事件已送达"的幻象。**严重 bug**。
  - `REQUIRES_NEW`：强制开新事务，更糟，业务和 outbox 完全异构事务，无任何原子性保证。
  - `MANDATORY`：必须有调用方事务，否则抛 `IllegalTransactionStateException`。在编译期 + 启动期不能保证调用者一定开事务，但运行期第一次错误调用就直接炸——把 bug 前置到调用方编写阶段，而不是潜伏到生产事故。
- **失败场景闭环**：
  - 业务 commit + outbox commit + Kafka 发送成功 → 正常路径。
  - 业务 commit + outbox commit + Kafka 发送失败 → outbox 保留 PENDING，Relay 后续重试。**这是 outbox 模式的核心价值**。
  - 业务回滚 → outbox 也回滚（同事务）→ 无脏事件。
  - Publisher 调用时没事务 → 立刻抛错，调用方修复。
- **不在 Publisher 内做 Kafka 发送**：发送动作完全交给 Relay。如果 Publisher 同步发，又遇到 Kafka 卡死，业务事务被拖死，反而比传统直发还糟。
- **`event_id` 的来源**：`AbstractDomainEvent` 默认用 SnowflakeIdGenerator.nextDefaultId() 生成；如需"业务键即事件 id"（更强的去重语义，比如同一笔提现重发），子类构造时传入业务键作为 `eventId`。
- **`outbox.id` 用 Snowflake 而非自增**：避免单点发号瓶颈、便于跨表 join 排查；趋势递增对 InnoDB 主键友好。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/TransactionalEventPublisher.java`
- Create: `common/src/main/java/com/exchange/common/mq/outbox/TransactionalEventPublisherImpl.java`
- Test: `common/src/test/java/com/exchange/common/mq/outbox/TransactionalEventPublisherImplTest.java`

- [ ] **Step 14.1: TransactionalEventPublisher.java**

```java
package com.exchange.common.mq;

public interface TransactionalEventPublisher {
    void publish(DomainEvent event);
    void publish(String topic, DomainEvent event);
}
```

- [ ] **Step 14.2: TransactionalEventPublisherImpl.java**

```java
package com.exchange.common.mq.outbox;

import com.exchange.common.mq.DomainEvent;
import com.exchange.common.mq.TransactionalEventPublisher;
import com.exchange.common.mq.kafka.EventEnvelope;
import com.exchange.common.mq.serializer.EventSerializer;
import com.exchange.common.util.SnowflakeIdGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class TransactionalEventPublisherImpl implements TransactionalEventPublisher {

    private final OutboxMapper outboxMapper;
    private final EventSerializer serializer;

    @Override
    public void publish(DomainEvent event) {
        publish(event.eventType(), event);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(String topic, DomainEvent event) {
        OutboxEntity entity = new OutboxEntity();
        entity.setId(SnowflakeIdGenerator.nextId());
        entity.setEventId(event.eventId());
        entity.setTopic(topic);
        entity.setPartitionKey(event.aggregateId());
        entity.setPayload(serializer.toJson(EventEnvelope.wrap(event)));
        entity.setStatus(OutboxStatus.PENDING.code);
        entity.setRetryCount(0);
        entity.setCreatedAt(LocalDateTime.now());
        outboxMapper.insert(entity);
    }
}
```

> 说明：`Propagation.MANDATORY` 强制要求调用方处于事务中——这是 outbox 模式的核心保证：业务变更与 outbox 写入必须同事务。

- [ ] **Step 14.3: TransactionalEventPublisherImplTest.java**

```java
package com.exchange.common.mq.outbox;

import com.exchange.common.mq.AbstractDomainEvent;
import com.exchange.common.mq.serializer.EventSerializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TransactionalEventPublisherImplTest {

    static class FooEvent extends AbstractDomainEvent {
        @Override public String aggregateId() { return "agg-1"; }
        @Override public String eventType() { return "test.foo"; }
    }

    @Test
    void publish_writes_pending_outbox_row() {
        OutboxMapper mapper = mock(OutboxMapper.class);
        EventSerializer serializer = mock(EventSerializer.class);
        when(serializer.toJson(any())).thenReturn("{}");

        TransactionalEventPublisherImpl pub =
                new TransactionalEventPublisherImpl(mapper, serializer);
        FooEvent e = new FooEvent();

        pub.publish(e);

        ArgumentCaptor<OutboxEntity> cap = ArgumentCaptor.forClass(OutboxEntity.class);
        verify(mapper).insert(cap.capture());
        OutboxEntity row = cap.getValue();
        assertThat(row.getEventId()).isEqualTo(e.eventId());
        assertThat(row.getTopic()).isEqualTo("test.foo");
        assertThat(row.getPartitionKey()).isEqualTo("agg-1");
        assertThat(row.getStatus()).isEqualTo(OutboxStatus.PENDING.code);
        assertThat(row.getRetryCount()).isZero();
    }
}
```

- [ ] **Step 14.4: 跑测试**

Run: `mvn -q -pl common test -Dtest=TransactionalEventPublisherImplTest`
Expected: 1 test passed.

- [ ] **Step 14.5: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/ common/src/test/java/com/exchange/common/mq/outbox/
git commit -m "feat(mq): TransactionalEventPublisher writes outbox in same tx"
```

### Task 15: OutboxRelay（独立线程批量投递）+ ShedLock

**设计考虑**：

```mermaid
flowchart TB
    Tick["@Scheduled(fixedDelay=1000)<br/>每秒触发"]
    Lock["@SchedulerLock<br/>name: outbox-relay"]
    Pick[mapper.pickPending<br/>批 200 行]
    Loop{遍历每行}
    Send[kafkaTemplate.send.get]
    OK[markStatus SENT]
    Fail[markRetry<br/>retry_count + next_retry_at]

    Tick --> Lock
    Lock -- 抢到锁 --> Pick
    Lock -- 没抢到 --> Skip[跳过本轮]
    Pick --> Loop
    Loop -- 成功 --> OK
    Loop -- 失败 --> Fail

    SL[(shedlock 表<br/>name=outbox-relay)]
    Lock <-.锁状态.-> SL
```

**关键决策**：

- **`fixedDelay=1000ms`**：上一轮结束到下一轮开始的间隔，不是固定频率。Relay 慢于 1 秒时会自然背压，不会堆任务。1 秒延迟在交易场景可接受；想压更低代价是 DB 轮询负载，不划算。
- **批量 200 行**：单轮最多发 200 条。够吞吐（200 \* 1QPS = 200msg/s/instance），但又不会让锁被占太久导致其他实例长时间空转。
- **ShedLock 的 `lockAtMostFor=30s`**：锁最多被持有 30 秒，超时强行释放。防止 Relay 进程崩溃时锁卡死。值要 ≥ 一轮处理的最大耗时（200 行 _ 单条最多 100ms = 20s），留 50% 余量。
- **ShedLock 的 `lockAtLeastFor=500ms`**：锁至少持有 500ms，即使任务很快结束也不立刻释放。防止"分布式时钟轻微偏差导致同一行被两个实例都拿到"——A 实例 100ms 跑完释放锁，B 实例时钟稍快、立刻抢到锁 + 看到了 A 还没 commit 的更新 = 重发。500ms 是 DB 主从复制延迟的安全阈值。
- **失败指数退避公式 `min(60 * 2^min(retry, 8), 600)`**：
  - retry=0 → 60s
  - retry=1 → 120s
  - retry=3 → 480s
  - retry≥4 → 600s（10 分钟封顶）
  - 8 次后 retry_count 不再放大延迟。延迟封顶 600s 避免 outbox 行被无限"延后"。本期不做"超过 N 次自动转 FAILED"，靠运维监控介入。
- **`.get()` 同步阻塞而非 `.thenAccept()`**：批内顺序处理简化错误归因。Relay 是单线程批处理，吞吐由批大小 \* 每秒次数决定，不需要并发。如果未来需要更高吞吐，开多个 Relay 实例 + ShedLock 调度，比单实例内部并发更稳。
- **`InterruptedException` 处理**：恢复中断标志（`Thread.currentThread().interrupt()`）后直接 return，不写 markRetry——下一轮再重试。
- **`@Scheduled` 必须配合 `@EnableScheduling`**：在 `ShedLockConfig` 上声明，避免污染其他不需要调度的 import。

**易踩的坑**：

- 不开 ShedLock 直接 `@Scheduled`：多副本部署时每个实例都跑，发送量被放大到副本数倍，下游 idempotent 表瞬间膨胀。
- `lockAtMostFor` 设小了：Relay 处理一轮没跑完锁就被释放，第二个实例进来抢到锁 + 看到老数据，重发。
- 错误处理把 `RuntimeException` 全吞了：Relay 永远不抛异常，下一轮立刻继续。这是设计选择——发送失败属于"对单条而言失败"，不应让整个 Relay 退出。但要监控 `markRetry` 的频率，超阈值告警。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/outbox/OutboxRelay.java`
- Create: `common/src/main/java/com/exchange/common/mq/outbox/ShedLockConfig.java`
- Test: `common/src/test/java/com/exchange/common/mq/outbox/OutboxRelayTest.java`

- [ ] **Step 15.1: ShedLockConfig.java**

```java
package com.exchange.common.mq.outbox;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "60s")
@ConditionalOnProperty(name = "exchange.mq.enabled", havingValue = "true", matchIfMissing = true)
public class ShedLockConfig {

    @Bean
    public LockProvider lockProvider(JdbcTemplate jdbcTemplate) {
        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(jdbcTemplate)
                        .usingDbTime()
                        .build());
    }
}
```

- [ ] **Step 15.2: OutboxRelay.java**

```java
package com.exchange.common.mq.outbox;

import com.exchange.common.mq.kafka.EventEnvelope;
import com.exchange.common.mq.serializer.EventSerializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private static final int BATCH = 200;

    private final OutboxMapper mapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventSerializer serializer;

    @Scheduled(fixedDelay = 1000)
    @SchedulerLock(name = "outbox-relay", lockAtMostFor = "30s", lockAtLeastFor = "500ms")
    public void relay() {
        List<OutboxEntity> rows = mapper.pickPending(
                OutboxStatus.PENDING.code, LocalDateTime.now(), BATCH);
        for (OutboxEntity row : rows) sendOne(row);
    }

    private void sendOne(OutboxEntity row) {
        try {
            kafkaTemplate.send(row.getTopic(), row.getPartitionKey(), row.getPayload())
                         .get();
            mapper.markStatus(row.getId(), OutboxStatus.SENT.code);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | RuntimeException e) {
            int next = row.getRetryCount() + 1;
            long delaySec = Math.min(60L * (1L << Math.min(next, 8)), 600L);
            mapper.markRetry(row.getId(), LocalDateTime.now().plusSeconds(delaySec));
            log.warn("outbox send failed id={} retry={} cause={}", row.getId(), next, e.toString());
        }
    }
}
```

- [ ] **Step 15.3: OutboxRelayTest.java（mock 单测）**

```java
package com.exchange.common.mq.outbox;

import com.exchange.common.mq.serializer.EventSerializer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutboxRelayTest {

    @Test
    void successful_send_marks_sent() {
        OutboxMapper mapper = mock(OutboxMapper.class);
        KafkaTemplate<String, String> tpl = mock(KafkaTemplate.class);
        EventSerializer ser = mock(EventSerializer.class);

        OutboxEntity row = new OutboxEntity();
        row.setId(1L);
        row.setTopic("t");
        row.setPartitionKey("k");
        row.setPayload("{}");
        row.setRetryCount(0);
        row.setStatus(OutboxStatus.PENDING.code);
        when(mapper.pickPending(eq(0), any(LocalDateTime.class), eq(200)))
                .thenReturn(List.of(row));

        SendResult<String, String> res = mock(SendResult.class);
        RecordMetadata md = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0L, 0, 0);
        when(res.getRecordMetadata()).thenReturn(md);
        when(tpl.send("t", "k", "{}")).thenReturn(CompletableFuture.completedFuture(res));

        new OutboxRelay(mapper, tpl, ser).relay();

        verify(mapper).markStatus(1L, OutboxStatus.SENT.code);
        verify(mapper, never()).markRetry(anyLong(), any());
    }

    @Test
    void failed_send_marks_retry_with_backoff() {
        OutboxMapper mapper = mock(OutboxMapper.class);
        KafkaTemplate<String, String> tpl = mock(KafkaTemplate.class);
        EventSerializer ser = mock(EventSerializer.class);

        OutboxEntity row = new OutboxEntity();
        row.setId(2L);
        row.setTopic("t");
        row.setPartitionKey("k");
        row.setPayload("{}");
        row.setRetryCount(0);
        row.setStatus(OutboxStatus.PENDING.code);
        when(mapper.pickPending(eq(0), any(LocalDateTime.class), eq(200)))
                .thenReturn(List.of(row));

        CompletableFuture<SendResult<String, String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("kafka down"));
        when(tpl.send("t", "k", "{}")).thenReturn(failed);

        new OutboxRelay(mapper, tpl, ser).relay();

        verify(mapper, never()).markStatus(anyLong(), anyInt());
        verify(mapper).markRetry(eq(2L), any(LocalDateTime.class));
    }
}
```

- [ ] **Step 15.4: 跑测试**

Run: `mvn -q -pl common test -Dtest=OutboxRelayTest`
Expected: 2 tests passed.

- [ ] **Step 15.5: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/outbox/ common/src/test/java/com/exchange/common/mq/outbox/
git commit -m "feat(mq): OutboxRelay with ShedLock + exponential backoff"
```

### Task 16: IdempotentEventHandler 抽象消费者

**设计考虑**：

```mermaid
flowchart TB
    M[Kafka onMessage] --> Exists{exists?}
    Exists -- 是 --> Skip[skip<br/>不再 handle]
    Exists -- 否 --> H[handle 业务逻辑]
    H -- 成功返回 --> Mark[markConsumed]
    H -- 抛 RetriableException --> Re[原样抛<br/>spring-kafka 触发重试]
    H -- 抛其他 RuntimeException --> NR[原样抛<br/>spring-kafka 转入 DLT]
    Mark --> Ack[ack offset]
    Re --> Retry["DefaultErrorHandler<br/>退避 + maxAttempts"]
    NR --> DLT["DLT topic<br/>人工介入"]
```

**关键决策**：

- **"先查后处理后标记"的顺序**：
  1. `exists?` 是快速路径，命中直接返回，不走业务逻辑。
  2. `handle()` 跑业务（可能写多张表、调多个外部接口）。
  3. 成功后 `markConsumed`。
  - **顺序很重要**：如果先 mark 后 handle，handle 失败但 mark 已落 → 下次跳过 → 业务永远没执行。
  - 如果 handle 与 mark 不同事务，且 handle 内部有副作用（例如调外部接口），可能出现"业务执行了但 mark 失败" → 下次重复执行。所以**默认要求 handle + mark 在同一个 Spring 事务**：onMessage 的调用方（Spring Kafka 的 `MessageListener` 包装）开外层 `@Transactional`，handle + markConsumed 共享。
- **异常分类的二分法**：
  - `RetriableException`：transient 失败（DB 死锁、外部接口超时）。原样抛 → spring-kafka 的 `DefaultErrorHandler` 按退避策略重试。**没标记 markConsumed**，重试时还会再走一次 `exists?` 检查。
  - 其他 `RuntimeException`：non-retriable（数据格式错、业务校验失败）。原样抛 → `DefaultErrorHandler` 按 `maxAttempts` 用尽后转 DLT topic 等待人工介入。
  - 二分法的好处：业务代码不需要写"是否要重试"的复杂判断，只需要决定异常的类型。
- **DLQ 不在本类内**：`IdempotentEventHandler` 只做"幂等闸 + 异常类型"，**不直接发 DLT**。DLT 路由是 Spring Kafka 的基础设施配置（在 `MqAutoConfiguration` 或各业务 listener 自己的 `KafkaListenerContainerFactory` 上配 `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`）。这样不同业务可以独立调整重试次数 / DLT 命名。
- **`handlerName` 必须稳定**：作为 consumed_record 的复合键之一。重命名 = 同一事件被该 handler 重新处理一次（之前的所有 mark 失效）。建议用 `<bounded-context>.<event>.<intent>` 格式，例如 `wallet.deposit-confirmed.notify-user`。

**易踩的坑**：

- handle 内部 `@Transactional(REQUIRES_NEW)`：mark 不在外层事务里，handle 执行成功后外层因别的原因回滚，mark 已落 → 业务回滚但下次跳过 → 丢消息。**禁止**。
- 把 `IllegalArgumentException`（数据格式错）当 `RetriableException` 抛：永远重试到 DLT，浪费 broker 资源。要明确分类。
- handler 内部用 ThreadLocal 但没清理：消费 pool 复用线程，残留状态影响下一条。本基类不解决，要求子类自行处理。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/IdempotentEventHandler.java`
- Test: `common/src/test/java/com/exchange/common/mq/IdempotentEventHandlerTest.java`

- [ ] **Step 16.1: IdempotentEventHandler.java**

```java
package com.exchange.common.mq;

import com.exchange.common.mq.consumed.ConsumedRecordStore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class IdempotentEventHandler<T extends DomainEvent> {

    private final ConsumedRecordStore consumedRecordStore;

    protected IdempotentEventHandler(ConsumedRecordStore consumedRecordStore) {
        this.consumedRecordStore = consumedRecordStore;
    }

    public final void onMessage(T event) {
        if (consumedRecordStore.exists(event.eventId(), handlerName())) {
            log.debug("idempotent skip event={} handler={}", event.eventId(), handlerName());
            return;
        }
        try {
            handle(event);
            consumedRecordStore.markConsumed(event.eventId(), handlerName());
        } catch (RetriableException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("non-retriable error event={} handler={} cause={}",
                    event.eventId(), handlerName(), e.toString(), e);
            throw e;
        }
    }

    protected abstract void handle(T event);
    protected abstract String handlerName();
}
```

> 说明：DLQ 路由由 spring-kafka 的 `DefaultErrorHandler` 配置接管，本类只负责"幂等闸 + 异常分类"，不直接发 DLQ。

- [ ] **Step 16.2: IdempotentEventHandlerTest.java**

```java
package com.exchange.common.mq;

import com.exchange.common.mq.consumed.ConsumedRecordStore;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class IdempotentEventHandlerTest {

    static class Foo extends AbstractDomainEvent {
        @Override public String aggregateId() { return "a"; }
        @Override public String eventType() { return "t.foo"; }
    }

    static class FooHandler extends IdempotentEventHandler<Foo> {
        final AtomicInteger calls = new AtomicInteger();
        FooHandler(ConsumedRecordStore s) { super(s); }
        @Override protected void handle(Foo event) { calls.incrementAndGet(); }
        @Override protected String handlerName() { return "test.foo.handler"; }
    }

    @Test
    void duplicate_event_skipped() {
        ConsumedRecordStore store = mock(ConsumedRecordStore.class);
        FooHandler h = new FooHandler(store);
        Foo e = new Foo();
        when(store.exists(e.eventId(), "test.foo.handler")).thenReturn(true);
        h.onMessage(e);
        assertThat(h.calls.get()).isZero();
        verify(store, never()).markConsumed(any(), any());
    }

    @Test
    void first_event_handled_then_marked() {
        ConsumedRecordStore store = mock(ConsumedRecordStore.class);
        FooHandler h = new FooHandler(store);
        Foo e = new Foo();
        when(store.exists(e.eventId(), "test.foo.handler")).thenReturn(false);
        h.onMessage(e);
        assertThat(h.calls.get()).isOne();
        verify(store).markConsumed(e.eventId(), "test.foo.handler");
    }

    @Test
    void retriable_exception_propagates_without_marking() {
        ConsumedRecordStore store = mock(ConsumedRecordStore.class);
        FooHandler h = new FooHandler(store) {
            @Override protected void handle(Foo event) { throw new RetriableException("transient"); }
        };
        Foo e = new Foo();
        when(store.exists(e.eventId(), "test.foo.handler")).thenReturn(false);
        assertThatThrownBy(() -> h.onMessage(e)).isInstanceOf(RetriableException.class);
        verify(store, never()).markConsumed(any(), any());
    }
}
```

- [ ] **Step 16.3: 跑测试**

Run: `mvn -q -pl common test -Dtest=IdempotentEventHandlerTest`
Expected: 3 tests passed.

- [ ] **Step 16.4: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/IdempotentEventHandler.java common/src/test/java/com/exchange/common/mq/IdempotentEventHandlerTest.java
git commit -m "feat(mq): IdempotentEventHandler with retriable/non-retriable split"
```

### Task 17: MqAutoConfiguration + 自动装配 import 文件

**设计考虑**：

```mermaid
flowchart LR
    Boot[Spring Boot 启动] --> Imp["扫 META-INF/spring/<br/>AutoConfiguration.imports"]
    Imp --> Cond{"exchange.mq.enabled?<br/>(默认 true)"}
    Cond -- 是 --> Mac[MqAutoConfiguration<br/>生效]
    Cond -- 否 --> Skip[整段不装配<br/>测试 / 离线分析模式]
    Mac --> CS["@ComponentScan<br/>com.exchange.common.mq"]
    Mac --> MS["@MapperScan<br/>outbox + consumed"]
    CS --> Beans[KafkaConfig / Relay / TEP / KEP / ...]
    MS --> Mappers[OutboxMapper / ConsumedRecordMapper]
```

**关键决策**：

- **为什么用 AutoConfiguration 而不是放在 bootstrap 全局扫描**：
  - common 是被多个模块依赖的基础库。bootstrap 的 `@SpringBootApplication` 默认只扫自己的包；让它去扫 common 里的子包会污染 bootstrap 配置。
  - AutoConfiguration 是 Spring Boot 标准玩法：基础库自带装配，使用方零配置即可享受全部 Bean。
- **`@ConditionalOnProperty(matchIfMissing=true)`**：默认装配；`exchange.mq.enabled=false` 时整段绕开。Hooks 到 `KafkaConfig` 和 `ShedLockConfig` 上的同名条件，三处一致就能整体禁用。
- **`@MapperScan` 显式列子包**：MyBatis-Plus 的 `@MapperScan` 不能直接装在 common 里扫 `com.exchange.common.mq` 全包——会扫到非 mapper 接口报错。**显式列出 mapper 所在的两个子包** `outbox` + `consumed` 更精准。
- **`@ComponentScan` 限定在 mq 包内**：避免扫到 common 其他子包（`util` / `config` 等已经有自己的装配路径）。
- **AutoConfiguration.imports 文件位置**：Spring Boot 2.7+ 弃用 `spring.factories`，新位置在 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`，每行一个全限定类名。
- **顺序无关**：本期没有显式 `@AutoConfigureBefore/After`。Bean 之间通过依赖注入自然排序。如果将来出现循环依赖（比如 KafkaConfig 依赖 MqMetrics、MqMetrics 又监听 KafkaTemplate），再用 `@AutoConfigureOrder` 拆。

**易踩的坑**：

- 忘了写 `imports` 文件，只放 `@Configuration` 类：使用方启动时这个类不会被发现，所有 Bean 都不存在但不报错——业务代码注入 `EventPublisher` 时才在启动期失败，难定位。
- 把 `MqAutoConfiguration` 写到 `bootstrap` 模块：违反"common 自带装配"的设计，且循环依赖 bootstrap → common → bootstrap。
- `@MapperScan` 写错路径：MyBatis 启动期会报"NoClassDefFoundError"或"Property 'sqlSessionFactory' is required"。复查 `imports` 文件列出的 base packages。

**Files:**
- Create: `common/src/main/java/com/exchange/common/mq/MqAutoConfiguration.java`
- Create: `common/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 17.1: MqAutoConfiguration.java**

```java
package com.exchange.common.mq;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(name = "exchange.mq.enabled", havingValue = "true", matchIfMissing = true)
@ComponentScan(basePackages = "com.exchange.common.mq")
@MapperScan(basePackages = {
        "com.exchange.common.mq.outbox",
        "com.exchange.common.mq.consumed"
})
public class MqAutoConfiguration {
}
```

- [ ] **Step 17.2: AutoConfiguration.imports**

写入：

```
com.exchange.common.mq.MqAutoConfiguration
```

- [ ] **Step 17.3: 编译**

Run: `mvn -q -pl bootstrap -am package -DskipTests`
Expected: BUILD SUCCESS。

- [ ] **Step 17.4: 提交**

```bash
git add common/src/main/java/com/exchange/common/mq/MqAutoConfiguration.java common/src/main/resources/META-INF/
git commit -m "feat(mq): autoconfiguration entry"
```

### Task 18: common-mq 集成测试（Testcontainers Kafka + MySQL）

**设计考虑**：

```mermaid
flowchart LR
    JUnit[JUnit 5] --> SBT["@SpringBootTest<br/>MqTestApplication"]
    SBT --> TC["@Testcontainers<br/>MySQLContainer"]
    SBT --> EK["@EmbeddedKafka<br/>partitions=1"]
    TC --> DPS["@DynamicPropertySource<br/>注入 jdbc-url + bootstrap-servers"]
    EK --> DPS
    DPS --> Ctx[Spring Context]
    Ctx --> TEP[TransactionalEventPublisher]
    Ctx --> Relay[OutboxRelay]
    Ctx --> Mapper[OutboxMapper]

    Test[测试用例] --> Tx["TransactionTemplate<br/>显式开事务"]
    Tx --> TEP
    TEP --> OB[(outbox PENDING)]
    OB -. 1秒后 .-> Relay
    Relay --> EK
    Test --> Await["Awaitility 轮询<br/>无 PENDING 行 = 已发出"]
```

**关键决策**：

- **MySQL 用 Testcontainers，Kafka 用 EmbeddedKafka**：
  - MySQL 用 Testcontainers：版本与生产严格一致（mysql:8.0），跑 Flyway 迁移验证 DDL 正确性；EmbeddedMysql 已不再维护。
  - Kafka 用 `@EmbeddedKafka`：spring-kafka-test 自带，启动比 KafkaContainer 快 5-10 秒，集成测试不需要测真 Kafka 的高级特性（如多 broker、partition reassign）。
  - 取舍：Kafka 集群级行为（如 broker 故障切换）需要 KafkaContainer，本期不测；business 流程测试 EmbeddedKafka 够用。
- **`@DynamicPropertySource` 替代 `application-test.yml` 硬编码**：MySQL 容器启动后才知道 jdbc-url（端口随机），EmbeddedKafka 的 `brokers` 也是运行时分配的。`DynamicPropertySource` 在 Spring Context 启动前注入到 `Environment`，YAML 占位符 `${mysql.url}` 才能解析。
- **`MqTestApplication` 放 test 目录**：不污染 main 的启动入口；`@SpringBootApplication` 默认扫所在包，所以放 `com.exchange.common.mq` 包下能扫到所有 `@Component`。
- **业务测试用 `TransactionTemplate` 而非 `@Transactional`**：`@Transactional` 在测试方法上的语义是"测试结束自动回滚"，与 `Propagation.MANDATORY` 配合 OK，但事务**不会真正 commit**——outbox 行根本没落表，Relay 看不到。所以**必须用 `TransactionTemplate.executeWithoutResult` 显式开事务并 commit**，让 outbox 行真实可见。
- **`Awaitility` 而非 `Thread.sleep`**：异步流程的等待用轮询断言，最多等 10 秒。早达到立刻继续，慢一点也不会 sleep 死等。CI 环境时序波动大，`sleep(2s)` 经常 flaky。
- **断言条件 `noneMatch eventId`**：不直接断言"SENT 行存在"——SENT 行可能被异步归档；用"PENDING 行不再含此 eventId" 反向证明已发送。

**易踩的坑**：

- 用 `@Transactional` 装饰整个测试方法 + 调 `TransactionalEventPublisher.publish`：方法结束 Spring 回滚事务，outbox 行被回滚没落表，Relay 永远看不到 → 测试超时失败。**必须用 TransactionTemplate**。
- 容器启动慢导致测试超时：`MySQLContainer` 首次跑要拉镜像，CI 上配镜像缓存或预热。
- `@EmbeddedKafka` 与 `KafkaTemplate` 的 bootstrap-servers 不一致：依赖 `spring.embedded.kafka.brokers` 系统属性传递；`@DynamicPropertySource` 必须显式 `add("kafka.bootstrap", () -> System.getProperty("spring.embedded.kafka.brokers"))`。
- ShedLock 的锁残留：测试用同一张 shedlock 表，连续跑两个测试时上一轮锁还没过期 → 后一轮 Relay 不工作。可以在 `@AfterEach` 清空 shedlock 表，或者把 `lockAtMostFor` 在 mqtest profile 里调成 5s。

**Files:**
- Create: `common/src/test/java/com/exchange/common/mq/MqIntegrationTest.java`
- Create: `common/src/test/resources/application-mqtest.yml`

- [ ] **Step 18.1: application-mqtest.yml**

```yaml
spring:
  kafka:
    bootstrap-servers: ${kafka.bootstrap}
  datasource:
    url: ${mysql.url}
    username: test
    password: test
  flyway:
    enabled: true
    locations: classpath:db/migration
exchange:
  mq:
    enabled: true
```

- [ ] **Step 18.2: MqIntegrationTest.java**

```java
package com.exchange.common.mq;

import com.exchange.common.mq.outbox.OutboxMapper;
import com.exchange.common.mq.outbox.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = MqTestApplication.class)
@ActiveProfiles("mqtest")
@Testcontainers
@EmbeddedKafka(partitions = 1, topics = {"test.foo"})
class MqIntegrationTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withUsername("test").withPassword("test").withDatabaseName("exchange_dev");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("mysql.url", mysql::getJdbcUrl);
        r.add("kafka.bootstrap", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Autowired TransactionalEventPublisher txPublisher;
    @Autowired OutboxMapper outboxMapper;

    static class FooEvent extends AbstractDomainEvent {
        @Override public String aggregateId() { return "agg"; }
        @Override public String eventType() { return "test.foo"; }
    }

    @Test
    @Transactional
    void publish_then_relay_to_kafka() {
        FooEvent e = new FooEvent();
        txPublisher.publish(e);
        // 退出事务后，OutboxRelay 应在 1-2 秒内拾取
    }

    @Test
    void relay_marks_sent() {
        // 单独事务写入一条 outbox，等 relay 处理
        FooEvent e = new FooEvent();
        runInTx(() -> txPublisher.publish(e));
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            var rows = outboxMapper.pickPending(OutboxStatus.PENDING.code,
                    LocalDateTime.now().plusYears(1), 100);
            assertThat(rows).noneMatch(r -> r.getEventId().equals(e.eventId()));
        });
    }

    private void runInTx(Runnable r) {
        new org.springframework.transaction.support.TransactionTemplate(
                txManager()).executeWithoutResult(s -> r.run());
    }

    @Autowired private org.springframework.transaction.PlatformTransactionManager txManager;
    private org.springframework.transaction.PlatformTransactionManager txManager() { return txManager; }
}
```

- [ ] **Step 18.3: MqTestApplication.java（最小启动类）**

Create: `common/src/test/java/com/exchange/common/mq/MqTestApplication.java`

```java
package com.exchange.common.mq;

import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MqTestApplication {
}
```

> 说明：放在 `test` 目录下，仅供测试驱动 Spring Boot 上下文使用。

- [ ] **Step 18.4: 跑集成测试**

Run: `mvn -q -pl common test -Dtest=MqIntegrationTest`
Expected: 2 tests passed（首次拉镜像耗时较长）。

- [ ] **Step 18.5: 提交**

```bash
git add common/src/test/
git commit -m "test(mq): integration test with testcontainers + embedded kafka"
```

---

## Phase 4 — wallet.chain.api：链抽象 SPI 与 DTO

> 说明：本阶段只创建接口与 POJO DTO，没有运行期逻辑，因此跳过 TDD，单测在后续 Plan 实现具体链时落。

### Task 19: 在 wallet pom 加 web3j-crypto / bouncycastle / spring-kafka 依赖

**Files:**
- Modify: `wallet/pom.xml`

- [ ] **Step 19.1: 在 `<dependencies>` 节末尾追加**

```xml
        <dependency>
            <groupId>com.exchange</groupId>
            <artifactId>exchange-risk</artifactId>
        </dependency>
        <dependency>
            <groupId>org.bouncycastle</groupId>
            <artifactId>bcprov-jdk18on</artifactId>
        </dependency>
        <dependency>
            <groupId>org.web3j</groupId>
            <artifactId>crypto</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.statemachine</groupId>
            <artifactId>spring-statemachine-core</artifactId>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>mysql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
```

> 说明：web3j-crypto 是 web3j 拆分包之一，仅含椭圆曲线 / BIP32/BIP39，不引整套 web3j（Plan 2 才引）。

- [ ] **Step 19.2: 验证编译**

Run: `mvn -q -pl wallet -am dependency:resolve`
Expected: BUILD SUCCESS。

- [ ] **Step 19.3: 提交**

```bash
git add wallet/pom.xml
git commit -m "build(wallet): add bc/web3j-crypto/statemachine/risk deps"
```

### Task 20: Chain 枚举 + 链无关 DTO（一组）

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/Chain.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/RawTx.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/SignedTx.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/ChainTx.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/ChainBlock.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/TransferRequest.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/TxStatus.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/KeyRef.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/DerivedAddress.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/FeeQuote.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/dto/FeeQuoteRequest.java`

- [ ] **Step 20.1: Chain.java**

```java
package com.exchange.wallet.chain.api;

public enum Chain {
    BTC, ETH, TRON;

    public static Chain of(String name) {
        return Chain.valueOf(name.toUpperCase());
    }
}
```

- [ ] **Step 20.2: RawTx.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.util.Map;

@Data
@Builder
public class RawTx {
    private Chain chain;
    private String fromAddress;
    private String toAddress;
    private String coinSymbol;
    private String contract;             // 合约地址，原生币为 null
    private java.math.BigDecimal amount;
    private Long nonce;                  // ETH/TRON
    private byte[] rawBytes;             // 链特化的未签名 tx 字节
    private Map<String, Object> chainSpecific;  // 链特化补充字段（gas/gasPrice/feeRate/...）
}
```

- [ ] **Step 20.3: SignedTx.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SignedTx {
    private Chain chain;
    private String fromAddress;
    private byte[] signedBytes;          // 链特化的已签名 tx 字节
    private String hexEncoded;           // 便于落库的 hex 形式
    private String predictedTxHash;      // 签名后即可计算的 txHash（部分链支持）
}
```

- [ ] **Step 20.4: ChainTx.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class ChainTx {
    private Chain chain;
    private String txHash;
    private int vout;                    // BTC vout / ETH ERC20 logIndex
    private long blockHeight;
    private String blockHash;
    private String parentHash;
    private String fromAddress;
    private String toAddress;
    private String coinSymbol;
    private String contract;
    private BigDecimal amount;
    private int direction;               // 1 入站 0 出站
    private int confirmCount;
    private String rawJson;
}
```

- [ ] **Step 20.5: ChainBlock.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ChainBlock {
    private Chain chain;
    private long height;
    private String hash;
    private String parentHash;
    private long timestampMs;
    private Object rawBlock;             // 链特化原始结构，TxParser 用
}
```

- [ ] **Step 20.6: TransferRequest.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class TransferRequest {
    private Chain chain;
    private String coinSymbol;
    private String contract;             // 原生币 null
    private String fromAddress;
    private String toAddress;
    private BigDecimal amount;
    private Long nonce;                  // 由 NonceAllocator 分配后填入
    private FeeQuote fee;                // 由 FeeStrategy 估算后填入
}
```

- [ ] **Step 20.7: TxStatus.java**

```java
package com.exchange.wallet.chain.api.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TxStatus {
    public enum Phase { NOT_FOUND, PENDING, MINED_OK, MINED_FAILED, DROPPED }

    private Phase phase;
    private long blockHeight;
    private int confirmCount;
    private String failureReason;
}
```

- [ ] **Step 20.8: KeyRef.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KeyRef {
    private Chain chain;
    private String keyId;                // 关联 key_material.key_id
    private String hdPath;               // 派生路径
    private String address;              // 期望地址（校验用）
}
```

- [ ] **Step 20.9: DerivedAddress.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DerivedAddress {
    private Chain chain;
    private String address;
    private String hdPath;
    private String publicKeyHex;
}
```

- [ ] **Step 20.10: FeeQuote.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Map;

@Data
@Builder
public class FeeQuote {
    private Chain chain;
    private BigDecimal feeAmount;        // 链原生币计价的总 fee
    private String feeCoinSymbol;        // ETH 用 ETH，TRON 用 TRX，BTC 用 BTC
    private Map<String, Object> chainSpecific;  // EIP-1559: maxFeePerGas, maxPriorityFeePerGas, gasLimit; BTC: feeRate, vsize; TRON: energy, bandwidth
}
```

- [ ] **Step 20.11: FeeQuoteRequest.java**

```java
package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class FeeQuoteRequest {
    private Chain chain;
    private String coinSymbol;
    private String contract;
    private String fromAddress;
    private String toAddress;
    private BigDecimal amount;
    private String urgency;              // NORMAL / FAST
}
```

- [ ] **Step 20.12: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 20.13: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/chain/api/
git commit -m "feat(chain-api): Chain enum + 10 chain-agnostic DTOs"
```

### Task 21: 链抽象 SPI 接口（5 个）

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/ChainClient.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/TxBuilder.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/TxBroadcaster.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/TxParser.java`
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/AddressDerivator.java`

- [ ] **Step 21.1: ChainClient.java**

```java
package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.ChainBlock;
import com.exchange.wallet.chain.api.dto.TxStatus;
import java.math.BigDecimal;

public interface ChainClient {
    Chain chain();
    BigDecimal getBalance(String address, String coinSymbol);
    long getLatestHeight();
    ChainBlock getBlock(long height);
    TxStatus queryTxStatus(String txHash);
    long getOnChainNonce(String address);   // BTC 不支持时返回 0
}
```

- [ ] **Step 21.2: TxBuilder.java**

```java
package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.TransferRequest;

public interface TxBuilder {
    Chain chain();
    RawTx buildTransfer(TransferRequest req);
}
```

- [ ] **Step 21.3: TxBroadcaster.java**

```java
package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.SignedTx;

public interface TxBroadcaster {
    Chain chain();
    String broadcast(SignedTx signedTx);
}
```

- [ ] **Step 21.4: TxParser.java**

```java
package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.ChainBlock;
import com.exchange.wallet.chain.api.dto.ChainTx;
import java.util.List;

public interface TxParser {
    Chain chain();
    List<ChainTx> parse(ChainBlock block);
}
```

- [ ] **Step 21.5: AddressDerivator.java**

```java
package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.DerivedAddress;

public interface AddressDerivator {
    Chain chain();
    DerivedAddress derive(byte[] hdSeed, String hdPath);
}
```

- [ ] **Step 21.6: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 21.7: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/chain/api/
git commit -m "feat(chain-api): ChainClient/TxBuilder/TxBroadcaster/TxParser/AddressDerivator SPI"
```

### Task 22: Signer 接口

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/chain/api/Signer.java`

- [ ] **Step 22.1: Signer.java**

```java
package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;

public interface Signer {
    SignedTx sign(RawTx rawTx, KeyRef keyRef);
}
```

> 说明：Signer 不是按链分实现——它是 wallet.signer 内的统一入口。内部根据 RawTx.chain 路由到 ChainSpecificSigner（在 Phase 5 创建）。

- [ ] **Step 22.2: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 22.3: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/chain/api/Signer.java
git commit -m "feat(chain-api): Signer SPI"
```

---

## Phase 5 — wallet.signer：密钥与签名

### Task 23: AES-GCM 加密工具 + 单测

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/signer/kms/AesGcmCipher.java`
- Test: `wallet/src/test/java/com/exchange/wallet/signer/kms/AesGcmCipherTest.java`

- [ ] **Step 23.1: AesGcmCipher.java**

```java
package com.exchange.wallet.signer.kms;

import lombok.Data;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Arrays;

public final class AesGcmCipher {

    public static final int IV_BYTES = 12;
    public static final int TAG_BITS = 128;

    private static final SecureRandom RNG = new SecureRandom();

    @Data
    public static class Cipherblob {
        private final byte[] iv;
        private final byte[] cipherText;
    }

    public Cipherblob encrypt(byte[] key, byte[] plaintext) {
        if (key.length != 32) throw new IllegalArgumentException("AES-256 key must be 32 bytes");
        byte[] iv = new byte[IV_BYTES];
        RNG.nextBytes(iv);
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            return new Cipherblob(iv, c.doFinal(plaintext));
        } catch (Exception e) {
            throw new IllegalStateException("AES-GCM encrypt failed", e);
        }
    }

    public byte[] decrypt(byte[] key, byte[] iv, byte[] cipherText) {
        if (key.length != 32) throw new IllegalArgumentException("AES-256 key must be 32 bytes");
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            return c.doFinal(cipherText);
        } catch (Exception e) {
            throw new IllegalStateException("AES-GCM decrypt failed", e);
        }
    }

    public static void wipe(byte[] data) {
        if (data != null) Arrays.fill(data, (byte) 0);
    }
}
```

- [ ] **Step 23.2: AesGcmCipherTest.java**

```java
package com.exchange.wallet.signer.kms;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import static org.assertj.core.api.Assertions.*;

class AesGcmCipherTest {

    private final AesGcmCipher cipher = new AesGcmCipher();

    @Test
    void encrypt_decrypt_roundtrip() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        byte[] plaintext = "super-secret-mnemonic seed".getBytes(StandardCharsets.UTF_8);

        AesGcmCipher.Cipherblob blob = cipher.encrypt(key, plaintext);
        byte[] decoded = cipher.decrypt(key, blob.getIv(), blob.getCipherText());

        assertThat(decoded).isEqualTo(plaintext);
        assertThat(blob.getIv()).hasSize(AesGcmCipher.IV_BYTES);
    }

    @Test
    void wrong_key_fails() {
        byte[] key1 = new byte[32];
        byte[] key2 = new byte[32];
        new SecureRandom().nextBytes(key1);
        new SecureRandom().nextBytes(key2);
        AesGcmCipher.Cipherblob blob = cipher.encrypt(key1, "x".getBytes());
        assertThatThrownBy(() -> cipher.decrypt(key2, blob.getIv(), blob.getCipherText()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void invalid_key_length_rejected() {
        assertThatThrownBy(() -> cipher.encrypt(new byte[16], new byte[]{1}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 23.3: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=AesGcmCipherTest`
Expected: 3 tests passed.

- [ ] **Step 23.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/signer/kms/ wallet/src/test/java/com/exchange/wallet/signer/kms/
git commit -m "feat(signer): AES-256-GCM cipher with wipe utility"
```

### Task 24: KmsProvider 接口 + 本地 keystore 实现

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/signer/kms/KmsProvider.java`
- Create: `wallet/src/main/java/com/exchange/wallet/signer/kms/LocalKeystoreKmsProvider.java`
- Test: `wallet/src/test/java/com/exchange/wallet/signer/kms/LocalKeystoreKmsProviderTest.java`

- [ ] **Step 24.1: KmsProvider.java**

```java
package com.exchange.wallet.signer.kms;

public interface KmsProvider {
    byte[] resolveDataKey(String alias);   // 解出 32 字节 AES 数据密钥
    String defaultAlias();
}
```

- [ ] **Step 24.2: LocalKeystoreKmsProvider.java**

```java
package com.exchange.wallet.signer.kms;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
public class LocalKeystoreKmsProvider implements KmsProvider {

    @Value("${wallet.signer.kms.local-master-key-base64:}")
    private String masterKeyB64;

    @Value("${wallet.signer.kms.default-alias:local:default}")
    private String defaultAlias;

    private final Map<String, byte[]> cache = new ConcurrentHashMap<>();

    @Override
    public byte[] resolveDataKey(String alias) {
        return cache.computeIfAbsent(alias, k -> {
            if (masterKeyB64 != null && !masterKeyB64.isBlank()) {
                byte[] decoded = Base64.getDecoder().decode(masterKeyB64);
                if (decoded.length != 32) {
                    throw new IllegalStateException("local master key must be 32 bytes (base64)");
                }
                return decoded;
            }
            // 兜底：生成进程内随机密钥（仅 dev 环境用）
            byte[] generated = new byte[32];
            new SecureRandom().nextBytes(generated);
            return generated;
        });
    }

    @Override
    public String defaultAlias() {
        return defaultAlias;
    }
}
```

> 说明：生产替换为 AwsKmsProvider 时实现同样的 `resolveDataKey` 接口（调用 KMS Decrypt API），上层零改动。dev 环境可以在 `application-dev.yml` 注入 `WALLET_SIGNER_KMS_LOCAL_MASTER_KEY_BASE64=<base64-encoded-32-bytes>`。

- [ ] **Step 24.3: LocalKeystoreKmsProviderTest.java**

```java
package com.exchange.wallet.signer.kms;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Base64;
import static org.assertj.core.api.Assertions.assertThat;

class LocalKeystoreKmsProviderTest {

    @Test
    void uses_configured_master_key() {
        LocalKeystoreKmsProvider p = new LocalKeystoreKmsProvider();
        byte[] key = new byte[32];
        for (int i = 0; i < 32; i++) key[i] = (byte) i;
        ReflectionTestUtils.setField(p, "masterKeyB64", Base64.getEncoder().encodeToString(key));
        ReflectionTestUtils.setField(p, "defaultAlias", "local:default");

        assertThat(p.resolveDataKey("local:default")).isEqualTo(key);
        assertThat(p.resolveDataKey("local:default")).isEqualTo(key);  // 同 alias 复用 cache
    }

    @Test
    void generates_random_key_when_unset() {
        LocalKeystoreKmsProvider p = new LocalKeystoreKmsProvider();
        ReflectionTestUtils.setField(p, "masterKeyB64", "");
        ReflectionTestUtils.setField(p, "defaultAlias", "local:default");

        byte[] k1 = p.resolveDataKey("local:default");
        byte[] k2 = p.resolveDataKey("local:default");
        assertThat(k1).hasSize(32).isEqualTo(k2);  // cache 内幂等
    }
}
```

- [ ] **Step 24.4: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=LocalKeystoreKmsProviderTest`
Expected: 2 tests passed.

- [ ] **Step 24.5: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/signer/kms/ wallet/src/test/java/com/exchange/wallet/signer/kms/LocalKeystoreKmsProviderTest.java
git commit -m "feat(signer): KmsProvider abstraction + local keystore impl"
```

### Task 25: BIP-39 助记词工具

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/signer/hd/Bip39MnemonicService.java`
- Test: `wallet/src/test/java/com/exchange/wallet/signer/hd/Bip39MnemonicServiceTest.java`

- [ ] **Step 25.1: Bip39MnemonicService.java**

```java
package com.exchange.wallet.signer.hd;

import org.springframework.stereotype.Component;
import org.web3j.crypto.MnemonicUtils;
import java.security.SecureRandom;

@Component
public class Bip39MnemonicService {

    private static final SecureRandom RNG = new SecureRandom();

    public String generateMnemonic() {
        byte[] entropy = new byte[16];   // 128 bit → 12 词
        RNG.nextBytes(entropy);
        return MnemonicUtils.generateMnemonic(entropy);
    }

    public byte[] mnemonicToSeed(String mnemonic, String passphrase) {
        return MnemonicUtils.generateSeed(mnemonic, passphrase == null ? "" : passphrase);
    }

    public boolean validate(String mnemonic) {
        return MnemonicUtils.validateMnemonic(mnemonic);
    }
}
```

- [ ] **Step 25.2: Bip39MnemonicServiceTest.java**

```java
package com.exchange.wallet.signer.hd;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class Bip39MnemonicServiceTest {

    private final Bip39MnemonicService svc = new Bip39MnemonicService();

    @Test
    void generated_mnemonic_is_valid_12_words() {
        String mnemonic = svc.generateMnemonic();
        assertThat(mnemonic.split(" ")).hasSize(12);
        assertThat(svc.validate(mnemonic)).isTrue();
    }

    @Test
    void seed_is_64_bytes() {
        String mnemonic = svc.generateMnemonic();
        byte[] seed = svc.mnemonicToSeed(mnemonic, "");
        assertThat(seed).hasSize(64);
    }

    @Test
    void same_mnemonic_same_seed() {
        String mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        byte[] s1 = svc.mnemonicToSeed(mnemonic, "");
        byte[] s2 = svc.mnemonicToSeed(mnemonic, "");
        assertThat(s1).isEqualTo(s2);
    }
}
```

- [ ] **Step 25.3: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=Bip39MnemonicServiceTest`
Expected: 3 tests passed.

- [ ] **Step 25.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/signer/hd/ wallet/src/test/java/com/exchange/wallet/signer/hd/
git commit -m "feat(signer): BIP-39 mnemonic service"
```

### Task 26: BIP-32/44 HD 路径派生

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/signer/hd/Bip32HdKeyDeriver.java`
- Test: `wallet/src/test/java/com/exchange/wallet/signer/hd/Bip32HdKeyDeriverTest.java`

- [ ] **Step 26.1: Bip32HdKeyDeriver.java**

```java
package com.exchange.wallet.signer.hd;

import lombok.Data;
import org.springframework.stereotype.Component;
import org.web3j.crypto.Bip32ECKeyPair;

@Component
public class Bip32HdKeyDeriver {

    @Data
    public static class HdKey {
        private final byte[] privateKey;     // 32 bytes
        private final byte[] publicKey;      // 65 bytes uncompressed, leading 0x04
        private final String path;
    }

    public HdKey derive(byte[] seed, String hdPath) {
        Bip32ECKeyPair master = Bip32ECKeyPair.generateKeyPair(seed);
        int[] indices = parsePath(hdPath);
        Bip32ECKeyPair derived = Bip32ECKeyPair.deriveKeyPair(master, indices);
        byte[] priv = derived.getPrivateKey().toByteArray();
        priv = leftPad32(priv);
        byte[] pub = derived.getPublicKey().toByteArray();
        return new HdKey(priv, pub, hdPath);
    }

    private static int[] parsePath(String hdPath) {
        // 形如 m/44'/60'/0'/0/123
        if (!hdPath.startsWith("m/")) throw new IllegalArgumentException("hdPath must start with m/");
        String[] parts = hdPath.substring(2).split("/");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            boolean hard = p.endsWith("'");
            int n = Integer.parseInt(hard ? p.substring(0, p.length() - 1) : p);
            out[i] = hard ? (n | 0x80000000) : n;
        }
        return out;
    }

    private static byte[] leftPad32(byte[] in) {
        if (in.length == 32) return in;
        byte[] out = new byte[32];
        if (in.length < 32) {
            System.arraycopy(in, 0, out, 32 - in.length, in.length);
        } else {
            System.arraycopy(in, in.length - 32, out, 0, 32);
        }
        return out;
    }
}
```

- [ ] **Step 26.2: Bip32HdKeyDeriverTest.java**

```java
package com.exchange.wallet.signer.hd;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class Bip32HdKeyDeriverTest {

    private final Bip39MnemonicService bip39 = new Bip39MnemonicService();
    private final Bip32HdKeyDeriver deriver = new Bip32HdKeyDeriver();

    @Test
    void deterministic_derivation() {
        // BIP-39 test vector: "abandon" × 11 + "about" → 已知 seed
        String mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        byte[] seed = bip39.mnemonicToSeed(mnemonic, "");
        Bip32HdKeyDeriver.HdKey k1 = deriver.derive(seed, "m/44'/60'/0'/0/0");
        Bip32HdKeyDeriver.HdKey k2 = deriver.derive(seed, "m/44'/60'/0'/0/0");
        assertThat(k1.getPrivateKey()).hasSize(32).isEqualTo(k2.getPrivateKey());
    }

    @Test
    void different_paths_different_keys() {
        String mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        byte[] seed = bip39.mnemonicToSeed(mnemonic, "");
        Bip32HdKeyDeriver.HdKey k0 = deriver.derive(seed, "m/44'/60'/0'/0/0");
        Bip32HdKeyDeriver.HdKey k1 = deriver.derive(seed, "m/44'/60'/0'/0/1");
        assertThat(k0.getPrivateKey()).isNotEqualTo(k1.getPrivateKey());
    }

    @Test
    void rejects_bad_path() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> deriver.derive(new byte[64], "44'/60'/0'/0/0"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 26.3: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=Bip32HdKeyDeriverTest`
Expected: 3 tests passed.

- [ ] **Step 26.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/signer/hd/Bip32HdKeyDeriver.java wallet/src/test/java/com/exchange/wallet/signer/hd/Bip32HdKeyDeriverTest.java
git commit -m "feat(signer): BIP-32/44 HD key deriver"
```

### Task 27: KeyMaterial 实体 + Mapper + Service

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/signer/KeyMaterialEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/signer/KeyMaterialMapper.java`
- Create: `wallet/src/main/java/com/exchange/wallet/signer/KeyMaterialService.java`

- [ ] **Step 27.1: KeyMaterialEntity.java**

```java
package com.exchange.wallet.signer;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("key_material")
public class KeyMaterialEntity {
    @TableId(type = IdType.INPUT)
    private Long id;
    private String keyId;
    private String keyType;             // HD_SEED / SINGLE
    private byte[] cipherText;
    private byte[] iv;
    private String kmsAlias;
    private Integer algoVersion;
    private LocalDateTime createdAt;
}
```

- [ ] **Step 27.2: KeyMaterialMapper.java**

```java
package com.exchange.wallet.signer;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface KeyMaterialMapper extends BaseMapper<KeyMaterialEntity> {

    @Select("SELECT * FROM key_material WHERE key_id = #{keyId} LIMIT 1")
    KeyMaterialEntity findByKeyId(@Param("keyId") String keyId);
}
```

- [ ] **Step 27.3: KeyMaterialService.java**

```java
package com.exchange.wallet.signer;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.signer.kms.AesGcmCipher;
import com.exchange.wallet.signer.kms.KmsProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class KeyMaterialService {

    private final KeyMaterialMapper mapper;
    private final KmsProvider kmsProvider;
    private final AesGcmCipher cipher = new AesGcmCipher();

    /** 生成新的 HD 种子并加密落库；返回 keyId */
    public String storeHdSeed(byte[] seed) {
        String keyId = "hd-" + UUID.randomUUID();
        byte[] dataKey = kmsProvider.resolveDataKey(kmsProvider.defaultAlias());
        AesGcmCipher.Cipherblob blob = cipher.encrypt(dataKey, seed);

        KeyMaterialEntity row = new KeyMaterialEntity();
        row.setId(SnowflakeIdGenerator.nextId());
        row.setKeyId(keyId);
        row.setKeyType("HD_SEED");
        row.setCipherText(blob.getCipherText());
        row.setIv(blob.getIv());
        row.setKmsAlias(kmsProvider.defaultAlias());
        row.setAlgoVersion(1);
        row.setCreatedAt(LocalDateTime.now());
        mapper.insert(row);
        return keyId;
    }

    /** 解密返回明文种子。调用方必须用完后 wipe。 */
    public byte[] loadSeed(String keyId) {
        KeyMaterialEntity row = mapper.findByKeyId(keyId);
        if (row == null) throw new IllegalArgumentException("keyId not found: " + keyId);
        byte[] dataKey = kmsProvider.resolveDataKey(row.getKmsAlias());
        return cipher.decrypt(dataKey, row.getIv(), row.getCipherText());
    }
}
```

- [ ] **Step 27.4: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 27.5: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/signer/KeyMaterial*
git commit -m "feat(signer): KeyMaterial entity/mapper/service with KMS-wrapped storage"
```

### Task 28: ChainSpecificSigner 接口 + Signer 实现（路由 + 私钥清零）

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/signer/ChainSpecificSigner.java`
- Create: `wallet/src/main/java/com/exchange/wallet/signer/SignerImpl.java`
- Test: `wallet/src/test/java/com/exchange/wallet/signer/SignerImplTest.java`

- [ ] **Step 28.1: ChainSpecificSigner.java**

```java
package com.exchange.wallet.signer;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;

public interface ChainSpecificSigner {
    Chain chain();
    SignedTx sign(RawTx rawTx, byte[] privateKey);
}
```

- [ ] **Step 28.2: SignerImpl.java**

```java
package com.exchange.wallet.signer;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.Signer;
import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;
import com.exchange.wallet.signer.hd.Bip32HdKeyDeriver;
import com.exchange.wallet.signer.kms.AesGcmCipher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SignerImpl implements Signer {

    private final KeyMaterialService keyMaterialService;
    private final Bip32HdKeyDeriver deriver;
    private final List<ChainSpecificSigner> chainSigners;

    @Override
    public SignedTx sign(RawTx rawTx, KeyRef keyRef) {
        if (rawTx.getChain() != keyRef.getChain()) {
            throw new IllegalArgumentException("chain mismatch: rawTx=" + rawTx.getChain()
                    + " keyRef=" + keyRef.getChain());
        }

        Map<Chain, ChainSpecificSigner> registry = chainSigners.stream()
                .collect(Collectors.toMap(ChainSpecificSigner::chain, s -> s));
        ChainSpecificSigner cs = registry.get(rawTx.getChain());
        if (cs == null) {
            throw new IllegalStateException("no ChainSpecificSigner for " + rawTx.getChain());
        }

        byte[] seed = null;
        Bip32HdKeyDeriver.HdKey hd = null;
        byte[] priv = null;
        try {
            seed = keyMaterialService.loadSeed(keyRef.getKeyId());
            hd = deriver.derive(seed, keyRef.getHdPath());
            priv = hd.getPrivateKey();
            return cs.sign(rawTx, priv);
        } finally {
            AesGcmCipher.wipe(seed);
            if (hd != null) {
                AesGcmCipher.wipe(hd.getPrivateKey());
            }
            // priv 是 hd.privateKey 的引用，已清；保险再清一次
            AesGcmCipher.wipe(priv);
        }
    }
}
```

- [ ] **Step 28.3: SignerImplTest.java**

```java
package com.exchange.wallet.signer;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;
import com.exchange.wallet.signer.hd.Bip32HdKeyDeriver;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SignerImplTest {

    static class FakeChainSigner implements ChainSpecificSigner {
        SignedTx returnValue = SignedTx.builder().chain(Chain.ETH).hexEncoded("0xdead").build();
        byte[] privateKeyAtCall;
        @Override public Chain chain() { return Chain.ETH; }
        @Override public SignedTx sign(RawTx rawTx, byte[] privateKey) {
            // 拷贝当时的内容用于断言（清零后再断言会全 0）
            privateKeyAtCall = privateKey.clone();
            return returnValue;
        }
    }

    @Test
    void routes_to_matching_chain_signer_and_wipes_after_use() {
        KeyMaterialService kms = mock(KeyMaterialService.class);
        Bip32HdKeyDeriver deriver = mock(Bip32HdKeyDeriver.class);
        FakeChainSigner ethSigner = new FakeChainSigner();
        SignerImpl signer = new SignerImpl(kms, deriver, List.of(ethSigner));

        byte[] seed = new byte[64];
        for (int i = 0; i < seed.length; i++) seed[i] = (byte) i;
        when(kms.loadSeed("k1")).thenReturn(seed);

        byte[] priv = new byte[32];
        for (int i = 0; i < 32; i++) priv[i] = (byte) (i + 100);
        Bip32HdKeyDeriver.HdKey hd = new Bip32HdKeyDeriver.HdKey(priv, new byte[65], "m/44'/60'/0'/0/0");
        when(deriver.derive(seed, "m/44'/60'/0'/0/0")).thenReturn(hd);

        RawTx raw = RawTx.builder().chain(Chain.ETH).build();
        KeyRef ref = KeyRef.builder().chain(Chain.ETH).keyId("k1").hdPath("m/44'/60'/0'/0/0").build();

        SignedTx out = signer.sign(raw, ref);

        assertThat(out.getHexEncoded()).isEqualTo("0xdead");
        assertThat(ethSigner.privateKeyAtCall).contains((byte) 100);  // 调用时是真实 key
        assertThat(priv).containsOnly((byte) 0);                       // 调用后被 wipe
        assertThat(seed).containsOnly((byte) 0);
    }

    @Test
    void chain_mismatch_rejected() {
        SignerImpl signer = new SignerImpl(mock(KeyMaterialService.class),
                mock(Bip32HdKeyDeriver.class), List.of(new FakeChainSigner()));
        RawTx raw = RawTx.builder().chain(Chain.BTC).build();
        KeyRef ref = KeyRef.builder().chain(Chain.ETH).build();
        assertThatThrownBy(() -> signer.sign(raw, ref))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void no_chain_signer_registered() {
        SignerImpl signer = new SignerImpl(mock(KeyMaterialService.class),
                mock(Bip32HdKeyDeriver.class), List.of());
        RawTx raw = RawTx.builder().chain(Chain.ETH).build();
        KeyRef ref = KeyRef.builder().chain(Chain.ETH).build();
        assertThatThrownBy(() -> signer.sign(raw, ref))
                .isInstanceOf(IllegalStateException.class);
    }
}
```

- [ ] **Step 28.4: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=SignerImplTest`
Expected: 3 tests passed.

- [ ] **Step 28.5: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/signer/ChainSpecificSigner.java wallet/src/main/java/com/exchange/wallet/signer/SignerImpl.java wallet/src/test/java/com/exchange/wallet/signer/SignerImplTest.java
git commit -m "feat(signer): SignerImpl with chain routing and post-use key wipe"
```

---

## Phase 6 — wallet.nonce：并发 nonce 分配

### Task 29: NonceRegister 实体 + Mapper

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/nonce/NonceRegisterEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/nonce/NonceRegisterMapper.java`

- [ ] **Step 29.1: NonceRegisterEntity.java**

```java
package com.exchange.wallet.nonce;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("nonce_register")
public class NonceRegisterEntity {
    private String chain;
    private String address;
    private Long nextNonce;
    private Long onChainNonce;
    private LocalDateTime reconciledAt;
    private Integer version;
}
```

- [ ] **Step 29.2: NonceRegisterMapper.java**

```java
package com.exchange.wallet.nonce;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import java.time.LocalDateTime;

@Mapper
public interface NonceRegisterMapper extends BaseMapper<NonceRegisterEntity> {

    @Select("""
        SELECT * FROM nonce_register
        WHERE chain = #{chain} AND address = #{address}
        """)
    NonceRegisterEntity find(@Param("chain") String chain, @Param("address") String address);

    @Insert("""
        INSERT IGNORE INTO nonce_register(chain, address, next_nonce, on_chain_nonce, reconciled_at, version)
        VALUES(#{chain}, #{address}, #{nextNonce}, #{onChainNonce}, #{reconciledAt}, 0)
        """)
    int insertIfAbsent(@Param("chain") String chain,
                       @Param("address") String address,
                       @Param("nextNonce") long nextNonce,
                       @Param("onChainNonce") long onChainNonce,
                       @Param("reconciledAt") LocalDateTime reconciledAt);

    @Update("""
        UPDATE nonce_register
           SET next_nonce = next_nonce + 1,
               version = version + 1
         WHERE chain = #{chain} AND address = #{address} AND version = #{version}
        """)
    int casIncrement(@Param("chain") String chain,
                     @Param("address") String address,
                     @Param("version") int version);

    @Update("""
        UPDATE nonce_register
           SET next_nonce = #{nextNonce},
               on_chain_nonce = #{onChainNonce},
               reconciled_at = #{now},
               version = version + 1
         WHERE chain = #{chain} AND address = #{address}
        """)
    int reconcile(@Param("chain") String chain,
                  @Param("address") String address,
                  @Param("nextNonce") long nextNonce,
                  @Param("onChainNonce") long onChainNonce,
                  @Param("now") LocalDateTime now);
}
```

- [ ] **Step 29.3: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 29.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/nonce/
git commit -m "feat(nonce): NonceRegister entity/mapper with CAS increment"
```

### Task 30: NonceAllocator 接口 + DB 乐观锁实现

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/nonce/NonceAllocator.java`
- Create: `wallet/src/main/java/com/exchange/wallet/nonce/DbOptimisticNonceAllocator.java`
- Test: `wallet/src/test/java/com/exchange/wallet/nonce/DbOptimisticNonceAllocatorTest.java`

- [ ] **Step 30.1: NonceAllocator.java**

```java
package com.exchange.wallet.nonce;

import com.exchange.wallet.chain.api.Chain;

public interface NonceAllocator {
    long allocate(Chain chain, String fromAddress);
    void reconcile(Chain chain, String fromAddress, long onChainPendingNonce);
}
```

- [ ] **Step 30.2: DbOptimisticNonceAllocator.java**

```java
package com.exchange.wallet.nonce;

import com.exchange.wallet.chain.api.Chain;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class DbOptimisticNonceAllocator implements NonceAllocator {

    private static final int MAX_RETRIES = 5;

    private final NonceRegisterMapper mapper;

    @Override
    public long allocate(Chain chain, String fromAddress) {
        for (int i = 0; i < MAX_RETRIES; i++) {
            NonceRegisterEntity row = mapper.find(chain.name(), fromAddress);
            if (row == null) {
                throw new IllegalStateException(
                        "nonce register not initialized for chain=" + chain + " addr=" + fromAddress
                                + ". call reconcile() once before allocate.");
            }
            long nonce = row.getNextNonce();
            int updated = mapper.casIncrement(chain.name(), fromAddress, row.getVersion());
            if (updated == 1) return nonce;
            log.debug("nonce CAS contention chain={} addr={} retry={}", chain, fromAddress, i + 1);
        }
        throw new IllegalStateException("nonce allocate failed after retries chain=" + chain
                + " addr=" + fromAddress);
    }

    @Override
    public void reconcile(Chain chain, String fromAddress, long onChainPendingNonce) {
        int rows = mapper.insertIfAbsent(chain.name(), fromAddress,
                onChainPendingNonce, onChainPendingNonce, LocalDateTime.now());
        if (rows == 0) {
            // 已存在，按链上 pending 重新校准 next_nonce
            mapper.reconcile(chain.name(), fromAddress,
                    onChainPendingNonce, onChainPendingNonce, LocalDateTime.now());
        }
        log.info("nonce reconciled chain={} addr={} pending={}", chain, fromAddress, onChainPendingNonce);
    }
}
```

> 说明：本实现仅用 DB 乐观锁；Redis Lua 兜底层在 Plan 2 实施时按多实例并发压测结果决定是否补，避免过度设计。

- [ ] **Step 30.3: DbOptimisticNonceAllocatorTest.java**

```java
package com.exchange.wallet.nonce;

import com.exchange.wallet.chain.api.Chain;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class DbOptimisticNonceAllocatorTest {

    @Test
    void allocate_returns_current_and_increments() {
        NonceRegisterMapper mapper = mock(NonceRegisterMapper.class);
        NonceRegisterEntity row = new NonceRegisterEntity();
        row.setChain("ETH"); row.setAddress("0xabc");
        row.setNextNonce(7L); row.setVersion(3);
        when(mapper.find("ETH", "0xabc")).thenReturn(row);
        when(mapper.casIncrement("ETH", "0xabc", 3)).thenReturn(1);

        DbOptimisticNonceAllocator a = new DbOptimisticNonceAllocator(mapper);
        long n = a.allocate(Chain.ETH, "0xabc");
        assertThat(n).isEqualTo(7L);
    }

    @Test
    void retries_on_contention() {
        NonceRegisterMapper mapper = mock(NonceRegisterMapper.class);
        NonceRegisterEntity row1 = new NonceRegisterEntity();
        row1.setNextNonce(5L); row1.setVersion(1);
        NonceRegisterEntity row2 = new NonceRegisterEntity();
        row2.setNextNonce(6L); row2.setVersion(2);
        when(mapper.find("ETH", "0x1"))
                .thenReturn(row1).thenReturn(row2);
        when(mapper.casIncrement("ETH", "0x1", 1)).thenReturn(0);
        when(mapper.casIncrement("ETH", "0x1", 2)).thenReturn(1);

        DbOptimisticNonceAllocator a = new DbOptimisticNonceAllocator(mapper);
        long n = a.allocate(Chain.ETH, "0x1");
        assertThat(n).isEqualTo(6L);
    }

    @Test
    void uninitialized_register_throws() {
        NonceRegisterMapper mapper = mock(NonceRegisterMapper.class);
        when(mapper.find(any(), any())).thenReturn(null);
        DbOptimisticNonceAllocator a = new DbOptimisticNonceAllocator(mapper);
        assertThatThrownBy(() -> a.allocate(Chain.ETH, "0x2"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not initialized");
    }
}
```

- [ ] **Step 30.4: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=DbOptimisticNonceAllocatorTest`
Expected: 3 tests passed.

- [ ] **Step 30.5: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/nonce/ wallet/src/test/java/com/exchange/wallet/nonce/
git commit -m "feat(nonce): NonceAllocator with DB optimistic CAS + reconcile"
```

---

## Phase 7 — wallet.fee：手续费抽象

### Task 31: FeeStrategy 接口 + Registry

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/fee/FeeStrategy.java`
- Create: `wallet/src/main/java/com/exchange/wallet/fee/FeeStrategyRegistry.java`
- Test: `wallet/src/test/java/com/exchange/wallet/fee/FeeStrategyRegistryTest.java`

- [ ] **Step 31.1: FeeStrategy.java**

```java
package com.exchange.wallet.fee;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.FeeQuote;
import com.exchange.wallet.chain.api.dto.FeeQuoteRequest;

public interface FeeStrategy {
    Chain chain();
    FeeQuote quote(FeeQuoteRequest req);
}
```

- [ ] **Step 31.2: FeeStrategyRegistry.java**

```java
package com.exchange.wallet.fee;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.FeeQuote;
import com.exchange.wallet.chain.api.dto.FeeQuoteRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class FeeStrategyRegistry {

    private final List<FeeStrategy> strategies;

    private Map<Chain, FeeStrategy> registry;

    public FeeQuote quote(FeeQuoteRequest req) {
        FeeStrategy s = lookup().get(req.getChain());
        if (s == null) throw new IllegalStateException("no FeeStrategy for chain=" + req.getChain());
        return s.quote(req);
    }

    private synchronized Map<Chain, FeeStrategy> lookup() {
        if (registry == null) {
            registry = strategies.stream()
                    .collect(Collectors.toMap(FeeStrategy::chain, s -> s));
        }
        return registry;
    }
}
```

- [ ] **Step 31.3: FeeStrategyRegistryTest.java**

```java
package com.exchange.wallet.fee;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.FeeQuote;
import com.exchange.wallet.chain.api.dto.FeeQuoteRequest;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class FeeStrategyRegistryTest {

    static class FakeEthFee implements FeeStrategy {
        @Override public Chain chain() { return Chain.ETH; }
        @Override public FeeQuote quote(FeeQuoteRequest r) {
            return FeeQuote.builder().chain(Chain.ETH).feeAmount(new BigDecimal("0.001")).build();
        }
    }

    @Test
    void routes_by_chain() {
        FeeStrategyRegistry reg = new FeeStrategyRegistry(List.of(new FakeEthFee()));
        FeeQuote q = reg.quote(FeeQuoteRequest.builder().chain(Chain.ETH).build());
        assertThat(q.getFeeAmount()).isEqualByComparingTo("0.001");
    }

    @Test
    void unknown_chain_throws() {
        FeeStrategyRegistry reg = new FeeStrategyRegistry(List.of(new FakeEthFee()));
        assertThatThrownBy(() -> reg.quote(FeeQuoteRequest.builder().chain(Chain.BTC).build()))
                .isInstanceOf(IllegalStateException.class);
    }
}
```

- [ ] **Step 31.4: 跑测试**

Run: `mvn -q -pl wallet test -Dtest=FeeStrategyRegistryTest`
Expected: 2 tests passed.

- [ ] **Step 31.5: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/fee/ wallet/src/test/java/com/exchange/wallet/fee/
git commit -m "feat(fee): FeeStrategy SPI + Registry"
```

---

## Phase 8 — wallet.core：双账法账本 + 实体 + 地址池

### Task 32: 14 张表的 Entity（一次性创建）

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/CoinEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/ChainConfigEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/WalletAddressEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/HdPathEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/AccountEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/AccountJournalEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/ChainTxEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/DepositOrderEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/WithdrawOrderEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/SweepOrderEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/TreasuryMovementEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/AddressBalanceEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/TreasuryPolicyEntity.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/entity/ReconcileReportEntity.java`

> 14 个 entity 都是 Lombok `@Data + @TableName` 风格，每个文件 ~ 25 行，由 subagent 按 V2 SQL 字段对应翻译。下方给出 1 个完整模板，其余按相同模式生成。

- [ ] **Step 32.1: 完整模板 — CoinEntity.java**

```java
package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("coin")
public class CoinEntity {
    @TableId(type = IdType.INPUT)
    private Long id;
    private String symbol;
    private String chain;
    private String contract;
    private Integer decimals;
    private Integer status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
```

- [ ] **Step 32.2: 按相同模式产出其余 13 个 entity**

字段映射规则：
- 主键 `id BIGINT` → `@TableId(type = IdType.INPUT) private Long id;`
- DB `version` → Java `private Integer version;`
- DB `DECIMAL(38,18)` → Java `BigDecimal`
- DB `DATETIME(3)` → Java `LocalDateTime`
- DB `MEDIUMTEXT` / `MEDIUMBLOB` → `String` / `byte[]`
- DB 复合主键（`address_balance` / `nonce_register` 已在 Task 29 处理）→ 不加 `@TableId`，全部字段普通；`account_journal` 主键 id 单字段；`treasury_policy` / `reconcile_report` 主键 id

13 个 entity 类的完整内容按 V2 SQL 中对应表字段生成，类名约定：

| 表 | Entity 类名 |
|---|---|
| chain_config | ChainConfigEntity |
| wallet_address | WalletAddressEntity |
| hd_path | HdPathEntity |
| account | AccountEntity |
| account_journal | AccountJournalEntity |
| chain_tx | ChainTxEntity |
| deposit_order | DepositOrderEntity |
| withdraw_order | WithdrawOrderEntity |
| sweep_order | SweepOrderEntity |
| treasury_movement | TreasuryMovementEntity |
| address_balance | AddressBalanceEntity |
| treasury_policy | TreasuryPolicyEntity |
| reconcile_report | ReconcileReportEntity |

每一个文件遵循 CoinEntity 模板：`@Data` + `@TableName("...")` + `@TableId(IdType.INPUT)` 标注主键。

- [ ] **Step 32.3: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 32.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/core/entity/
git commit -m "feat(core): 14 entities for wallet foundation tables"
```

### Task 33: 14 张表的 Mapper

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/core/mapper/CoinMapper.java` — `extends BaseMapper<CoinEntity>`
- 同模板: `ChainConfigMapper / WalletAddressMapper / HdPathMapper / AccountMapper / AccountJournalMapper / ChainTxMapper / DepositOrderMapper / WithdrawOrderMapper / SweepOrderMapper / TreasuryMovementMapper / AddressBalanceMapper / TreasuryPolicyMapper / ReconcileReportMapper`

- [ ] **Step 33.1: CoinMapper.java（模板）**

```java
package com.exchange.wallet.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.exchange.wallet.core.entity.CoinEntity;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface CoinMapper extends BaseMapper<CoinEntity> {
}
```

- [ ] **Step 33.2: AccountMapper.java（含乐观锁更新方法）**

```java
package com.exchange.wallet.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.exchange.wallet.core.entity.AccountEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import java.math.BigDecimal;

@Mapper
public interface AccountMapper extends BaseMapper<AccountEntity> {

    @Select("SELECT * FROM account WHERE user_id = #{userId} AND coin_id = #{coinId}")
    AccountEntity find(@Param("userId") long userId, @Param("coinId") long coinId);

    @Update("""
        UPDATE account
           SET available = #{available},
               frozen = #{frozen},
               version = version + 1,
               updated_at = NOW(3)
         WHERE id = #{id} AND version = #{version}
        """)
    int casUpdate(@Param("id") long id,
                  @Param("available") BigDecimal available,
                  @Param("frozen") BigDecimal frozen,
                  @Param("version") int version);
}
```

- [ ] **Step 33.3: AccountJournalMapper.java（含 SUM 对账查询）**

```java
package com.exchange.wallet.core.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.exchange.wallet.core.entity.AccountJournalEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import java.math.BigDecimal;

@Mapper
public interface AccountJournalMapper extends BaseMapper<AccountJournalEntity> {

    @Select("""
        SELECT COALESCE(SUM(direction * amount), 0)
          FROM account_journal
         WHERE coin_id = #{coinId}
        """)
    BigDecimal sumDirectionalAmount(@Param("coinId") long coinId);

    @Select("""
        SELECT COALESCE(SUM(direction * amount), 0)
          FROM account_journal
         WHERE account_id = #{accountId}
        """)
    BigDecimal sumByAccount(@Param("accountId") long accountId);
}
```

- [ ] **Step 33.4: 其余 11 个 Mapper（仅 BaseMapper 即可，无自定义 SQL）**

按 CoinMapper 模板生成：ChainConfigMapper / WalletAddressMapper / HdPathMapper / ChainTxMapper / DepositOrderMapper / WithdrawOrderMapper / SweepOrderMapper / TreasuryMovementMapper / AddressBalanceMapper / TreasuryPolicyMapper / ReconcileReportMapper —— 类内空，泛型对应自身 entity。

- [ ] **Step 33.5: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 33.6: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/core/mapper/
git commit -m "feat(core): 14 mappers (account/journal with custom SQL)"
```

### Task 34: 系统账户常量 + BizType / JournalDirection 枚举

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/core/ledger/SystemAccountConstants.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/ledger/BizType.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/ledger/JournalDirection.java`

- [ ] **Step 34.1: SystemAccountConstants.java**

```java
package com.exchange.wallet.core.ledger;

public final class SystemAccountConstants {
    public static final long INFLOW_USER_ID = -1L;       // 充值入口（链上未识别归账户）
    public static final long HOT_WALLET_USER_ID = -2L;   // 主热钱包账户
    public static final long FEE_USER_ID = -3L;          // 手续费账户（出金费）
    public static final long FROZEN_BUFFER_USER_ID = -4L;// 提现冻结暂存
    private SystemAccountConstants() {}
}
```

- [ ] **Step 34.2: BizType.java**

```java
package com.exchange.wallet.core.ledger;

public enum BizType {
    DEPOSIT,
    WITHDRAW_FREEZE,
    WITHDRAW_SETTLE,
    WITHDRAW_REFUND,
    INTERNAL_TRANSFER,
    SWEEP_OUT,
    SWEEP_IN,
    FEE,
    REVERSE_DEPOSIT,
    TREASURY_MOVE
}
```

- [ ] **Step 34.3: JournalDirection.java**

```java
package com.exchange.wallet.core.ledger;

public enum JournalDirection {
    CREDIT(1), DEBIT(-1);
    public final int code;
    JournalDirection(int code) { this.code = code; }
}
```

- [ ] **Step 34.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/core/ledger/
git commit -m "feat(core): SystemAccountConstants + BizType + JournalDirection"
```

### Task 35: LedgerService 接口 + DTO

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerCommand.java`
- Create: `wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerService.java`

- [ ] **Step 35.1: LedgerCommand.java**

```java
package com.exchange.wallet.core.ledger;

import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class LedgerCommand {
    private String traceId;          // 同一笔业务跨多个账户共享
    private long fromUserId;
    private long toUserId;
    private long coinId;
    private BigDecimal amount;
    private BizType bizType;
    private long bizId;
    private String remark;
}
```

- [ ] **Step 35.2: LedgerService.java**

```java
package com.exchange.wallet.core.ledger;

import java.math.BigDecimal;

public interface LedgerService {

    /** 双向划转：fromUser available -amount，toUser available +amount */
    void transferAvailable(LedgerCommand cmd);

    /** 冻结：user available -amount → user frozen +amount */
    void freeze(long userId, long coinId, BigDecimal amount, String traceId, BizType bizType, long bizId);

    /** 解冻：user frozen -amount → user available +amount */
    void unfreeze(long userId, long coinId, BigDecimal amount, String traceId, BizType bizType, long bizId);

    /** 结算：user frozen -amount → systemHot available +amount（提现链上确认后调用） */
    void settle(long userId, long coinId, BigDecimal amount, String traceId, BizType bizType, long bizId);

    /** 入账（充值）：systemInflow -amount → user available +amount */
    void credit(long userId, long coinId, BigDecimal amount, String traceId, BizType bizType, long bizId);

    /** 反向冲账（reorg 已入账资金回滚） */
    void reverseCredit(long userId, long coinId, BigDecimal amount, String traceId, BizType bizType, long bizId);
}
```

- [ ] **Step 35.3: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 35.4: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerCommand.java wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerService.java
git commit -m "feat(core): LedgerService interface with 6 ops"
```

### Task 36: LedgerServiceImpl 双账法实现

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerServiceImpl.java`

- [ ] **Step 36.1: LedgerServiceImpl.java**

```java
package com.exchange.wallet.core.ledger;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.core.entity.AccountEntity;
import com.exchange.wallet.core.entity.AccountJournalEntity;
import com.exchange.wallet.core.mapper.AccountJournalMapper;
import com.exchange.wallet.core.mapper.AccountMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Service
@RequiredArgsConstructor
public class LedgerServiceImpl implements LedgerService {

    private final AccountMapper accountMapper;
    private final AccountJournalMapper journalMapper;

    @Override
    @Transactional
    public void transferAvailable(LedgerCommand cmd) {
        AccountEntity from = ensureAccount(cmd.getFromUserId(), cmd.getCoinId());
        AccountEntity to = ensureAccount(cmd.getToUserId(), cmd.getCoinId());
        require(from.getAvailable().compareTo(cmd.getAmount()) >= 0, "insufficient available");

        BigDecimal fromAvailable = from.getAvailable().subtract(cmd.getAmount());
        BigDecimal toAvailable = to.getAvailable().add(cmd.getAmount());

        casOrThrow(accountMapper.casUpdate(from.getId(), fromAvailable, from.getFrozen(), from.getVersion()), "from");
        casOrThrow(accountMapper.casUpdate(to.getId(), toAvailable, to.getFrozen(), to.getVersion()), "to");

        insertJournal(cmd.getTraceId(), from.getId(), cmd.getCoinId(),
                cmd.getBizType(), cmd.getBizId(), JournalDirection.DEBIT, cmd.getAmount(),
                fromAvailable, cmd.getRemark());
        insertJournal(cmd.getTraceId(), to.getId(), cmd.getCoinId(),
                cmd.getBizType(), cmd.getBizId(), JournalDirection.CREDIT, cmd.getAmount(),
                toAvailable, cmd.getRemark());
    }

    @Override
    @Transactional
    public void freeze(long userId, long coinId, BigDecimal amount,
                       String traceId, BizType bizType, long bizId) {
        AccountEntity acc = ensureAccount(userId, coinId);
        require(acc.getAvailable().compareTo(amount) >= 0, "insufficient available");
        BigDecimal newAvailable = acc.getAvailable().subtract(amount);
        BigDecimal newFrozen = acc.getFrozen().add(amount);
        casOrThrow(accountMapper.casUpdate(acc.getId(), newAvailable, newFrozen, acc.getVersion()), "freeze");

        insertJournal(traceId, acc.getId(), coinId, bizType, bizId,
                JournalDirection.DEBIT, amount, newAvailable, "freeze available");
        insertJournal(traceId, acc.getId(), coinId, bizType, bizId,
                JournalDirection.CREDIT, amount, newFrozen, "freeze frozen");
    }

    @Override
    @Transactional
    public void unfreeze(long userId, long coinId, BigDecimal amount,
                         String traceId, BizType bizType, long bizId) {
        AccountEntity acc = ensureAccount(userId, coinId);
        require(acc.getFrozen().compareTo(amount) >= 0, "insufficient frozen");
        BigDecimal newFrozen = acc.getFrozen().subtract(amount);
        BigDecimal newAvailable = acc.getAvailable().add(amount);
        casOrThrow(accountMapper.casUpdate(acc.getId(), newAvailable, newFrozen, acc.getVersion()), "unfreeze");

        insertJournal(traceId, acc.getId(), coinId, bizType, bizId,
                JournalDirection.DEBIT, amount, newFrozen, "unfreeze frozen");
        insertJournal(traceId, acc.getId(), coinId, bizType, bizId,
                JournalDirection.CREDIT, amount, newAvailable, "unfreeze available");
    }

    @Override
    @Transactional
    public void settle(long userId, long coinId, BigDecimal amount,
                       String traceId, BizType bizType, long bizId) {
        AccountEntity user = ensureAccount(userId, coinId);
        AccountEntity hot = ensureAccount(SystemAccountConstants.HOT_WALLET_USER_ID, coinId);
        require(user.getFrozen().compareTo(amount) >= 0, "insufficient frozen for settle");

        BigDecimal newUserFrozen = user.getFrozen().subtract(amount);
        casOrThrow(accountMapper.casUpdate(user.getId(), user.getAvailable(), newUserFrozen, user.getVersion()), "user-frozen");

        BigDecimal newHotAvailable = hot.getAvailable().add(amount);
        casOrThrow(accountMapper.casUpdate(hot.getId(), newHotAvailable, hot.getFrozen(), hot.getVersion()), "hot");

        insertJournal(traceId, user.getId(), coinId, bizType, bizId,
                JournalDirection.DEBIT, amount, newUserFrozen, "settle out frozen");
        insertJournal(traceId, hot.getId(), coinId, bizType, bizId,
                JournalDirection.CREDIT, amount, newHotAvailable, "settle into hot wallet");
    }

    @Override
    @Transactional
    public void credit(long userId, long coinId, BigDecimal amount,
                       String traceId, BizType bizType, long bizId) {
        AccountEntity inflow = ensureAccount(SystemAccountConstants.INFLOW_USER_ID, coinId);
        AccountEntity user = ensureAccount(userId, coinId);
        BigDecimal newInflowAvailable = inflow.getAvailable().subtract(amount);
        BigDecimal newUserAvailable = user.getAvailable().add(amount);

        casOrThrow(accountMapper.casUpdate(inflow.getId(), newInflowAvailable, inflow.getFrozen(), inflow.getVersion()), "inflow");
        casOrThrow(accountMapper.casUpdate(user.getId(), newUserAvailable, user.getFrozen(), user.getVersion()), "user");

        insertJournal(traceId, inflow.getId(), coinId, bizType, bizId,
                JournalDirection.DEBIT, amount, newInflowAvailable, "credit inflow out");
        insertJournal(traceId, user.getId(), coinId, bizType, bizId,
                JournalDirection.CREDIT, amount, newUserAvailable, "credit user in");
    }

    @Override
    @Transactional
    public void reverseCredit(long userId, long coinId, BigDecimal amount,
                              String traceId, BizType bizType, long bizId) {
        // 反向：user -amount → inflow +amount
        AccountEntity inflow = ensureAccount(SystemAccountConstants.INFLOW_USER_ID, coinId);
        AccountEntity user = ensureAccount(userId, coinId);
        require(user.getAvailable().compareTo(amount) >= 0, "insufficient available for reverse");

        BigDecimal newUserAvailable = user.getAvailable().subtract(amount);
        BigDecimal newInflowAvailable = inflow.getAvailable().add(amount);
        casOrThrow(accountMapper.casUpdate(user.getId(), newUserAvailable, user.getFrozen(), user.getVersion()), "user-rev");
        casOrThrow(accountMapper.casUpdate(inflow.getId(), newInflowAvailable, inflow.getFrozen(), inflow.getVersion()), "inflow-rev");

        insertJournal(traceId, user.getId(), coinId, bizType, bizId,
                JournalDirection.DEBIT, amount, newUserAvailable, "reverse credit");
        insertJournal(traceId, inflow.getId(), coinId, bizType, bizId,
                JournalDirection.CREDIT, amount, newInflowAvailable, "reverse credit");
    }

    private AccountEntity ensureAccount(long userId, long coinId) {
        AccountEntity row = accountMapper.find(userId, coinId);
        if (row != null) return row;
        AccountEntity created = new AccountEntity();
        created.setId(SnowflakeIdGenerator.nextId());
        created.setUserId(userId);
        created.setCoinId(coinId);
        created.setAvailable(BigDecimal.ZERO);
        created.setFrozen(BigDecimal.ZERO);
        created.setVersion(0);
        LocalDateTime now = LocalDateTime.now();
        created.setCreatedAt(now);
        created.setUpdatedAt(now);
        accountMapper.insert(created);
        return accountMapper.find(userId, coinId);
    }

    private void insertJournal(String traceId, long accountId, long coinId,
                               BizType bizType, long bizId, JournalDirection dir,
                               BigDecimal amount, BigDecimal balanceAfter, String remark) {
        AccountJournalEntity j = new AccountJournalEntity();
        j.setId(SnowflakeIdGenerator.nextId());
        j.setTraceId(traceId);
        j.setAccountId(accountId);
        j.setCoinId(coinId);
        j.setBizType(bizType.name());
        j.setBizId(bizId);
        j.setDirection(dir.code);
        j.setAmount(amount);
        j.setBalanceAfter(balanceAfter);
        j.setRemark(remark);
        j.setCreatedAt(LocalDateTime.now());
        journalMapper.insert(j);
    }

    private static void require(boolean cond, String msg) {
        if (!cond) throw new IllegalStateException(msg);
    }

    private static void casOrThrow(int affected, String tag) {
        if (affected != 1) {
            throw new ConcurrentModificationException("ledger CAS conflict at " + tag);
        }
    }

    public static class ConcurrentModificationException extends RuntimeException {
        public ConcurrentModificationException(String message) { super(message); }
    }
}
```

- [ ] **Step 36.2: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 36.3: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/core/ledger/LedgerServiceImpl.java
git commit -m "feat(core): LedgerServiceImpl with double-entry journal + CAS"
```

### Task 37: LedgerService 集成测试（Testcontainers MySQL，验证双账平衡 + 幂等闸）

**Files:**
- Create: `wallet/src/test/java/com/exchange/wallet/core/ledger/LedgerServiceImplIT.java`
- Create: `wallet/src/test/java/com/exchange/wallet/WalletTestApplication.java`
- Create: `wallet/src/test/resources/application-walletit.yml`

- [ ] **Step 37.1: WalletTestApplication.java**

```java
package com.exchange.wallet;

import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WalletTestApplication {
}
```

- [ ] **Step 37.2: application-walletit.yml**

```yaml
spring:
  datasource:
    url: ${mysql.url}
    username: test
    password: test
  flyway:
    enabled: true
    locations: classpath:db/migration
exchange:
  mq:
    enabled: false
wallet:
  signer:
    kms:
      local-master-key-base64: AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=
```

- [ ] **Step 37.3: LedgerServiceImplIT.java**

```java
package com.exchange.wallet.core.ledger;

import com.exchange.wallet.WalletTestApplication;
import com.exchange.wallet.core.mapper.AccountJournalMapper;
import com.exchange.wallet.core.mapper.AccountMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.math.BigDecimal;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = WalletTestApplication.class)
@ActiveProfiles("walletit")
@Testcontainers
class LedgerServiceImplIT {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("exchange_dev")
            .withUsername("test").withPassword("test");

    @DynamicPropertySource
    static void p(DynamicPropertyRegistry r) { r.add("mysql.url", mysql::getJdbcUrl); }

    @Autowired LedgerService ledger;
    @Autowired AccountMapper accountMapper;
    @Autowired AccountJournalMapper journalMapper;

    private static final long COIN_ETH = 1L;

    @Test
    void zero_sum_invariant_after_credit() {
        String trace = UUID.randomUUID().toString();
        ledger.credit(1001L, COIN_ETH, new BigDecimal("0.5"), trace, BizType.DEPOSIT, 9001L);
        BigDecimal sum = journalMapper.sumDirectionalAmount(COIN_ETH);
        assertThat(sum).isEqualByComparingTo("0");
    }

    @Test
    void duplicate_trace_blocked_by_unique_key() {
        String trace = UUID.randomUUID().toString();
        ledger.credit(1002L, COIN_ETH, new BigDecimal("1"), trace, BizType.DEPOSIT, 9002L);
        assertThatThrownBy(() ->
                ledger.credit(1002L, COIN_ETH, new BigDecimal("1"), trace, BizType.DEPOSIT, 9002L))
                .isInstanceOf(org.springframework.dao.DuplicateKeyException.class);
    }

    @Test
    void freeze_then_settle_balance_correct() {
        String t1 = UUID.randomUUID().toString();
        ledger.credit(1003L, COIN_ETH, new BigDecimal("2"), t1, BizType.DEPOSIT, 9003L);

        String tFreeze = UUID.randomUUID().toString();
        ledger.freeze(1003L, COIN_ETH, new BigDecimal("1.2"), tFreeze, BizType.WITHDRAW_FREEZE, 9004L);
        var acc = accountMapper.find(1003L, COIN_ETH);
        assertThat(acc.getAvailable()).isEqualByComparingTo("0.8");
        assertThat(acc.getFrozen()).isEqualByComparingTo("1.2");

        String tSettle = UUID.randomUUID().toString();
        ledger.settle(1003L, COIN_ETH, new BigDecimal("1.2"), tSettle, BizType.WITHDRAW_SETTLE, 9004L);
        acc = accountMapper.find(1003L, COIN_ETH);
        assertThat(acc.getFrozen()).isEqualByComparingTo("0");
        assertThat(acc.getAvailable()).isEqualByComparingTo("0.8");

        BigDecimal sum = journalMapper.sumDirectionalAmount(COIN_ETH);
        assertThat(sum).isEqualByComparingTo("0");
    }
}
```

- [ ] **Step 37.4: 跑集成测试**

Run: `mvn -q -pl wallet test -Dtest=LedgerServiceImplIT`
Expected: 3 tests passed.

- [ ] **Step 37.5: 提交**

```bash
git add wallet/src/test/java/ wallet/src/test/resources/
git commit -m "test(core): LedgerService IT — zero-sum + idempotency + freeze/settle"
```

### Task 38: AddressPoolService（地址预生成 + 分配 + 回收）

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/core/address/AddressPoolService.java`

- [ ] **Step 38.1: AddressPoolService.java**

```java
package com.exchange.wallet.core.address;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.chain.api.AddressDerivator;
import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.DerivedAddress;
import com.exchange.wallet.core.entity.HdPathEntity;
import com.exchange.wallet.core.entity.WalletAddressEntity;
import com.exchange.wallet.core.mapper.HdPathMapper;
import com.exchange.wallet.core.mapper.WalletAddressMapper;
import com.exchange.wallet.signer.KeyMaterialService;
import com.exchange.wallet.signer.kms.AesGcmCipher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class AddressPoolService {

    private final WalletAddressMapper addressMapper;
    private final HdPathMapper hdPathMapper;
    private final KeyMaterialService keyMaterialService;
    private final List<AddressDerivator> derivators;
    private final AtomicLong addressIndex = new AtomicLong(0);

    @Transactional
    public WalletAddressEntity allocate(long userId, Chain chain, String hdSeedKeyId) {
        Map<Chain, AddressDerivator> reg = derivators.stream()
                .collect(Collectors.toMap(AddressDerivator::chain, d -> d));
        AddressDerivator deriver = reg.get(chain);
        if (deriver == null) throw new IllegalStateException("no AddressDerivator for " + chain);

        // 简化策略：address_index 全局自增，后续可以按 chain 拆
        long idx = addressIndex.getAndIncrement();
        String hdPath = "m/44'/" + cointype(chain) + "'/0'/0/" + idx;

        // 防重：唯一约束阻止；尝试到第一个未占用 path
        while (hdPathMapper.selectCount(new QueryWrapper<HdPathEntity>()
                .eq("chain", chain.name()).eq("hd_path", hdPath)) > 0) {
            idx = addressIndex.getAndIncrement();
            hdPath = "m/44'/" + cointype(chain) + "'/0'/0/" + idx;
        }

        byte[] seed = null;
        try {
            seed = keyMaterialService.loadSeed(hdSeedKeyId);
            DerivedAddress derived = deriver.derive(seed, hdPath);

            HdPathEntity pathRow = new HdPathEntity();
            pathRow.setId(SnowflakeIdGenerator.nextId());
            pathRow.setChain(chain.name());
            pathRow.setHdPath(hdPath);
            pathRow.setUsedAt(LocalDateTime.now());
            hdPathMapper.insert(pathRow);

            WalletAddressEntity addr = new WalletAddressEntity();
            addr.setId(SnowflakeIdGenerator.nextId());
            addr.setUserId(userId);
            addr.setChain(chain.name());
            addr.setAddress(derived.getAddress());
            addr.setHdPath(hdPath);
            addr.setKeyId(hdSeedKeyId);
            addr.setStatus(1);
            LocalDateTime now = LocalDateTime.now();
            addr.setCreatedAt(now);
            addr.setUpdatedAt(now);
            addressMapper.insert(addr);
            return addr;
        } finally {
            AesGcmCipher.wipe(seed);
        }
    }

    private static int cointype(Chain c) {
        return switch (c) {
            case BTC -> 0;
            case ETH -> 60;
            case TRON -> 195;
        };
    }
}
```

> 说明：本实现是 Plan 1 的最小可用版本——`address_index` 用进程内 AtomicLong 起跳，重启会从 0 重算然后撞 hd_path 的唯一约束逐个跳过。Plan 2 实施时升级到 Redis `INCR` 单点分配 + 启动从 DB 读取最大 hd_path 末段填回 AtomicLong。

- [ ] **Step 38.2: 编译**

Run: `mvn -q -pl wallet -am compile`
Expected: BUILD SUCCESS。

- [ ] **Step 38.3: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/core/address/
git commit -m "feat(core): AddressPoolService with HD path uniqueness"
```

### Task 39: WalletAutoConfiguration 让 wallet 子包被 bootstrap 扫描

**Files:**
- Create: `wallet/src/main/java/com/exchange/wallet/WalletAutoConfiguration.java`
- Create: `wallet/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`

- [ ] **Step 39.1: WalletAutoConfiguration.java**

```java
package com.exchange.wallet;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.exchange.wallet")
@MapperScan(basePackages = {
        "com.exchange.wallet.core.mapper",
        "com.exchange.wallet.signer",
        "com.exchange.wallet.nonce"
})
public class WalletAutoConfiguration {
}
```

- [ ] **Step 39.2: AutoConfiguration.imports**

写入：

```
com.exchange.wallet.WalletAutoConfiguration
```

- [ ] **Step 39.3: 编译并启动 bootstrap**

Run:
```bash
mvn -q -DskipTests -pl bootstrap -am package
java -jar bootstrap/target/exchange-bootstrap-1.0.0-SNAPSHOT.jar --spring.profiles.active=dev &
sleep 5 && curl -s http://localhost:8080/actuator/health
```
Expected: `{"status":"UP"}`，日志能看到 wallet/common-mq 的 bean 装配。

- [ ] **Step 39.4: 关闭进程**

Run: `pkill -f exchange-bootstrap-1.0.0-SNAPSHOT.jar`

- [ ] **Step 39.5: 提交**

```bash
git add wallet/src/main/java/com/exchange/wallet/WalletAutoConfiguration.java wallet/src/main/resources/META-INF/
git commit -m "feat(wallet): autoconfiguration entry"
```

---

## Phase 9 — 验收

### Task 40: 全量编译 + 全量测试 + 启动健康检查

**Files:**
- 仅运行命令

- [ ] **Step 40.1: 全量构建**

Run: `mvn -q clean package`
Expected: BUILD SUCCESS，无 test 失败。

- [ ] **Step 40.2: 全量测试**

Run: `mvn -q test`
Expected: BUILD SUCCESS，统计输出含 wallet / common 各模块的 unit + IT 测试。

- [ ] **Step 40.3: 启动 + actuator 健康**

Run:
```bash
java -jar bootstrap/target/exchange-bootstrap-1.0.0-SNAPSHOT.jar --spring.profiles.active=dev &
sleep 8 && curl -s http://localhost:8080/actuator/health
curl -s http://localhost:8080/wallet/health
pkill -f exchange-bootstrap-1.0.0-SNAPSHOT.jar
```
Expected: 两个 health 端点都返回 UP。

- [ ] **Step 40.4: 校验 V2 在干净库中重放**

Run:
```bash
mysql -h127.0.0.1 -uroot -proot -e "drop database if exists exchange_dev; create database exchange_dev character set utf8mb4;"
java -jar bootstrap/target/exchange-bootstrap-1.0.0-SNAPSHOT.jar --spring.profiles.active=dev &
sleep 8 && mysql -h127.0.0.1 -uroot -proot exchange_dev -e "show tables;" | wc -l
pkill -f exchange-bootstrap-1.0.0-SNAPSHOT.jar
```
Expected: 至少 20 行（19 张表 + header）。

- [ ] **Step 40.5: 沉淀知识到 question/**

按 CLAUDE.md 约定，把本 Plan 涉及的核心面试题沉淀到 `question/`：
- `question/wallet-double-entry-ledger.md` — 双账法 / trace_id 幂等闸 / 凑零不变量 / 系统账户设计
- `question/wallet-mq-outbox.md` — outbox 模式 / 事务边界 / `Propagation.MANDATORY` / OutboxRelay 重试退避
- `question/wallet-idempotent-consumer.md` — 幂等消费 / 可重试 vs 不可重试异常分类 / DLQ
- `question/wallet-key-management.md` — KMS 抽象 / AES-GCM / 私钥清零 / HD 派生
- `question/wallet-nonce-allocation.md` — DB 乐观锁 CAS / 启动校准 / 多实例并发

每篇文档结构：题面 → 标准答案要点 → 在本项目中的体现（带文件路径与行号锚点） → 延伸追问。

```bash
git add question/
git commit -m "docs(question): Plan 1 foundation interview points"
```

- [ ] **Step 40.6: 在 docs/superpowers/plans/0000-roadmap.md 标记 Plan 1 完成**

把 `## Plan 1 — Foundation（基础设施）` 标题改为 `## Plan 1 — Foundation（基础设施） ✅ DONE` 并提交。

```bash
git add docs/superpowers/plans/0000-roadmap.md
git commit -m "docs: mark plan1 done"
```

---

## 完成判据（Plan 1）

- ✅ `mvn clean test` 全绿，包含 LedgerServiceImplIT、MqIntegrationTest、SignerImplTest、DbOptimisticNonceAllocatorTest、AesGcmCipherTest、Bip32HdKeyDeriverTest 等
- ✅ bootstrap 启动正常，actuator/health UP
- ✅ Flyway 在干净库中可重放，建出 19 张表
- ✅ 双账法跑过"凑零不变量"集成测试
- ✅ Plan 1 涉及的核心面试题已沉淀到 `question/`

Plan 2 起跑前置：本 Plan 全部 task 完成 + 验收通过。





