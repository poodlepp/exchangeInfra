package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DerivedAddress {
    private Chain chain;
    private String address;
    private String hdPath;
    private String publicKeyHex;
}
