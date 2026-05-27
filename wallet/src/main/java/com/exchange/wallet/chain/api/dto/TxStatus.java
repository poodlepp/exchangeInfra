package com.exchange.wallet.chain.api.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class TxStatus {
    public enum Phase { NOT_FOUND, PENDING, MINED_OK, MINED_FAILED, DROPPED }

    private Phase phase;
    private long blockHeight;
    private int confirmCount;
    private String failureReason;
}
