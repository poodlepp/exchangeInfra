package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.ChainBlock;
import com.exchange.wallet.chain.api.dto.ChainTx;
import java.util.List;

public interface TxParser {
    Chain chain();
    List<ChainTx> parse(ChainBlock block);
}
