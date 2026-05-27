package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;

public interface Signer {
    SignedTx sign(RawTx rawTx, KeyRef keyRef);
}
