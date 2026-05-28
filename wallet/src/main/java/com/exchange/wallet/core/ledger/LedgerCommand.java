package com.exchange.wallet.core.ledger;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

/**
 * 双账法记账命令。所有 LedgerService 方法都接收同一个 DTO，避免方法签名爆炸。
 *
 * <p>关键不变量：
 * <ul>
 *   <li>{@code traceId} 必填，全局唯一，作为幂等闸：同 traceId 第二次调用 → DuplicateKeyException 命中</li>
 *   <li>{@code amount} 必须 &gt; 0；不接受零和负数（反向操作通过 reverseCredit 表达）</li>
 *   <li>{@code fromUserId} / {@code toUserId} 任一可为系统账户（负数 user_id）</li>
 *   <li>{@code bizType} + {@code bizId} 用于审计追溯（"哪笔提现订单触发了这条 journal"）</li>
 * </ul>
 *
 * 不可变（{@code @Value}），构造期校验由 LedgerServiceImpl 完成。
 */
@Value
@Builder
public class LedgerCommand {

    /** 全局唯一追踪 ID，幂等闸字段 */
    String traceId;

    /** 出账方账户的 user_id（可为系统账户负数） */
    long fromUserId;

    /** 入账方账户的 user_id（可为系统账户负数） */
    long toUserId;

    /** 币种 ID */
    long coinId;

    /** 金额；必须 > 0 */
    BigDecimal amount;

    /** 业务类型 */
    BizType bizType;

    /** 业务主键（如 withdraw_order.id / deposit_order.id） */
    long bizId;

    /** 备注（可选） */
    String remark;
}
