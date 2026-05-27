package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class ChainTx {
    private Chain chain;
    private String txHash;
    private int vout;
    private long blockHeight;
    private String blockHash;
    private String parentHash;
    private String fromAddress;
    private String toAddress;
    private String coinSymbol;
    private String contract;
    private BigDecimal amount;
    private int direction;
    private int confirmCount;
    private String rawJson;
}
