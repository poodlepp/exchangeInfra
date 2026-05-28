package com.exchange.wallet.core.ledger;

/**
 * journal 记账方向。落库为 TINYINT。
 * 凑零不变量：SUM(direction × amount) GROUP BY coin_id == 0
 */
public enum JournalDirection {

    /** 入账：+1 */
    CREDIT(1),

    /** 出账：-1 */
    DEBIT(-1);

    private final int value;

    JournalDirection(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }
}
