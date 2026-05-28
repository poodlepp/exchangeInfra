package com.exchange.wallet.core.ledger;

/**
 * 系统账户常量。
 * 用户账户 user_id 为正数；系统账户 user_id 为负数，避免与用户域冲突。
 * 这 4 个账户在启动期由 LedgerServiceImpl#@PostConstruct 保证存在，
 * 业务调用时直接引用，避免 NPE。
 */
public final class SystemAccountConstants {

    /** 充值入金中间账户：链上识别充值后先 +1，再划转到用户账户 */
    public static final long INFLOW = -1L;

    /** 主热钱包账户：对应链上热钱包地址，提现结算的资金来源 */
    public static final long HOT_WALLET = -2L;

    /** 手续费账户：所有矿工费聚集 */
    public static final long FEE = -3L;

    /** 提现冻结暂存账户：用户冻结但还没广播的资金（可选中转） */
    public static final long FROZEN_BUFFER = -4L;

    private SystemAccountConstants() {
    }
}
