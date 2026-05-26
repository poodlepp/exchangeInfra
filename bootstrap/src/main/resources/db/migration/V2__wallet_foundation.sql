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
