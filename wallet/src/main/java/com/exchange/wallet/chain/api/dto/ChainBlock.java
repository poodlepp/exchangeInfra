package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ChainBlock {
    private Chain chain;
    private long height;
    private String hash;
    private String parentHash;
    private long timestampMs;
    private Object rawBlock;
}
