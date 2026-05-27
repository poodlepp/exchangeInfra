package com.exchange.wallet.chain.api.dto;

import com.exchange.wallet.chain.api.Chain;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SignedTx {
    private Chain chain;
    private String fromAddress;
    private byte[] signedBytes;
    private String hexEncoded;
    private String predictedTxHash;
}
