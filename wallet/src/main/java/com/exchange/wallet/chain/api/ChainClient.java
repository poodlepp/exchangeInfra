package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.ChainBlock;
import com.exchange.wallet.chain.api.dto.TxStatus;
import java.math.BigDecimal;

public interface ChainClient {
    Chain chain();
    BigDecimal getBalance(String address, String coinSymbol);
    long getLatestHeight();
    ChainBlock getBlock(long height);
    TxStatus queryTxStatus(String txHash);
    long getOnChainNonce(String address);
}
