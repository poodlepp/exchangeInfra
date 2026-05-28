package com.exchange.wallet.core.ledger;

/**
 * 双账法账本服务。
 *
 * <p>所有方法都遵循"双账法"——任一资金移动都拆成两条 journal（金额相等、direction 相反、
 * 共享 traceId）+ 对应 account 表的 CAS 余额更新，全部在同一 Spring 事务内。
 *
 * <p>幂等性：所有方法以 {@link LedgerCommand#getTraceId()} 为闸。同一 traceId 重入会撞
 * {@code uk(trace_id, direction, account_id)} 抛 DuplicateKeyException——业务调用方应将其视为
 * "第一次已经成功"的幂等命中，不再重试整个动作。
 *
 * <p>失败语义：余额不足、账户不存在、CAS 冲突等异常一律抛出 RuntimeException，由 Spring 自动回滚。
 */
public interface LedgerService {

    /**
     * 用户内部转账：fromUser.available - ↔ toUser.available +
     * <p>用途：用户对用户转账、归集源 → HOT_WALLET 账面划转
     */
    void transferAvailable(LedgerCommand cmd);

    /**
     * 提现冻结：user.available - ↔ user.frozen +（同账户跨列）
     * <p>cmd.fromUserId == cmd.toUserId，amount 在 available 减、frozen 加
     */
    void freeze(LedgerCommand cmd);

    /**
     * 解冻（提现取消 / 挂单撤销）：user.frozen - ↔ user.available +
     * <p>cmd.fromUserId == cmd.toUserId
     */
    void unfreeze(LedgerCommand cmd);

    /**
     * 提现结算：user.frozen - ↔ HOT_WALLET.available +
     * <p>cmd.fromUserId == 用户、cmd.toUserId == HOT_WALLET
     */
    void settle(LedgerCommand cmd);

    /**
     * 充值入账：INFLOW.available - ↔ user.available +
     * <p>cmd.fromUserId == INFLOW、cmd.toUserId == 用户
     */
    void credit(LedgerCommand cmd);

    /**
     * 充值反向冲账：user.available - ↔ INFLOW.available +
     * <p>用于 reorg / 风控驳回，cmd.fromUserId == 用户、cmd.toUserId == INFLOW
     */
    void reverseCredit(LedgerCommand cmd);
}
