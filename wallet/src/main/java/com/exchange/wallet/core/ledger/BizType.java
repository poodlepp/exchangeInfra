package com.exchange.wallet.core.ledger;

/**
 * 业务类型，写入 account_journal.biz_type，与 biz_id 共同定位一笔业务。
 * 落库为字符串，便于扩展不破坏历史数据。
 */
public enum BizType {

    /** 充值入账：INFLOW(-1) → user(+1) */
    DEPOSIT,

    /** 提现冻结：user.available(-1) → user.frozen(+1) */
    WITHDRAW_FREEZE,

    /** 提现结算（链上确认）：user.frozen(-1) → HOT_WALLET(+1) */
    WITHDRAW_SETTLE,

    /** 用户内部转账：fromUser.available(-1) → toUser.available(+1) */
    INTERNAL_TRANSFER,

    /** 归集出账：用户存款地址(-1) → HOT_WALLET(+1) */
    SWEEP_OUT,

    /** 归集入账：与 SWEEP_OUT 配对（双链路场景） */
    SWEEP_IN,

    /** 矿工费扣减：HOT_WALLET(-1) → FEE(+1) */
    FEE,

    /** 充值反向冲账：reorg / 风控驳回时反向 */
    REVERSE_DEPOSIT,

    /** 冷热分层划转：HOT_WALLET ↔ 冷钱包链上账户 */
    TREASURY_MOVE
}
