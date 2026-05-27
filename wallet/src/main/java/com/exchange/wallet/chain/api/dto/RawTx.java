package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Map;

@Data
@Builder
public class RawTx {
    private Chain chain;
    private String fromAddress;
    private String toAddress;
    private String coinSymbol;
    private String contract;
    private BigDecimal amount;
    private Long nonce;
    private byte[] rawBytes;
    private Map<String, Object> chainSpecific;
}
