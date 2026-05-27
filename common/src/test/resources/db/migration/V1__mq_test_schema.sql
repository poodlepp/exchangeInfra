-- 仅用于 common 模块集成测试。生产 V2 在 bootstrap 模块。
-- common 模块不能依赖 bootstrap，故复制 outbox/consumed_record/shedlock 三张表 DDL。

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
