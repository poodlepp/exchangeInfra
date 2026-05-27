package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;
import java.util.Map;

@Data
@Builder
public class FeeQuote {
    private Chain chain;
    private BigDecimal feeAmount;
    private String feeCoinSymbol;
    private Map<String, Object> chainSpecific;
}
