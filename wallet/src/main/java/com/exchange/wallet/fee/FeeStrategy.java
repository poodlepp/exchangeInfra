package com.exchange.wallet.fee;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.FeeQuote;
import com.exchange.wallet.chain.api.dto.FeeQuoteRequest;

/**
 * 链特定的手续费估算策略 SPI。
 *
 * <p>各链实现位于：
 * - Plan 2: wallet.chain.eth.EthFeeStrategy（EIP-1559）
 * - Plan 4: wallet.chain.btc.BtcFeeStrategy（sat/vB + UTXO selection）
 * - Plan 5: wallet.chain.tron.TronFeeStrategy（energy + bandwidth）
 *
 * <p>实现类必须 @Component 注册为 Spring Bean，启动期由 {@link FeeStrategyRegistry} 收集。
 */
public interface FeeStrategy {

    /** 该策略针对的链 */
    Chain chain();

    /** 估算手续费 */
    FeeQuote quote(FeeQuoteRequest req);
}
