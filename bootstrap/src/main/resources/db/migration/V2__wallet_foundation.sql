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
