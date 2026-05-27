package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KeyRef {
    private Chain chain;
    private String keyId;
    private String hdPath;
    private String address;
}
