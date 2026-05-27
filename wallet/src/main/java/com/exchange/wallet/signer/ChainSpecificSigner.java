package com.exchange.wallet.signer;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;

public interface ChainSpecificSigner {
    Chain chain();
    SignedTx sign(RawTx rawTx, byte[] privateKey);
}
