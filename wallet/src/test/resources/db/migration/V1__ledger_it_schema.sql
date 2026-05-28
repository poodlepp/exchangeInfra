-- Ledger 集成测试用最小 schema：仅包含 account + account_journal 两表，
-- 与 bootstrap V2__wallet_foundation.sql 中对应表的 DDL 严格一致。
-- 其他 12 张表不在本测试涉及范围内，避免 IT 启动时的无关 DDL 噪音。

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
