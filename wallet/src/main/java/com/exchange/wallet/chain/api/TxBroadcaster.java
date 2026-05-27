package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.SignedTx;

public interface TxBroadcaster {
    Chain chain();
    String broadcast(SignedTx signedTx);
}
