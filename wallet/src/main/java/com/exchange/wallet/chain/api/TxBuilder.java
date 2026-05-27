package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.TransferRequest;

public interface TxBuilder {
    Chain chain();
    RawTx buildTransfer(TransferRequest req);
}
