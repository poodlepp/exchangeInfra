package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;

@Data
@Builder
public class FeeQuoteRequest {
    private Chain chain;
    private String coinSymbol;
    private String contract;
    private String fromAddress;
    private String toAddress;
    private BigDecimal amount;
    private String urgency;
}
