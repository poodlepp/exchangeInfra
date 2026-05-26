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
