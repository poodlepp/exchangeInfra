package com.exchange.wallet.fee;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.FeeQuote;
import com.exchange.wallet.chain.api.dto.FeeQuoteRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FeeStrategyRegistryTest {

    @Test
    void routes_to_matching_chain_strategy() {
        FeeStrategyRegistry registry = new FeeStrategyRegistry(List.of(new FakeEthFeeStrategy()));

        FeeQuote quote = registry.quote(FeeQuoteRequest.builder()
                .chain(Chain.ETH)
                .coinSymbol("ETH")
                .amount(BigDecimal.ONE)
                .build());

        assertThat(quote.getChain()).isEqualTo(Chain.ETH);
        assertThat(quote.getFeeAmount()).isEqualByComparingTo("0.0021");
    }

    @Test
    void throws_for_unknown_chain() {
        FeeStrategyRegistry registry = new FeeStrategyRegistry(List.of(new FakeEthFeeStrategy()));

        assertThatThrownBy(() -> registry.quote(FeeQuoteRequest.builder()
                .chain(Chain.BTC)
                .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no FeeStrategy for chain BTC");
    }

    @Test
    void throws_when_two_strategies_share_same_chain() {
        assertThatThrownBy(() -> new FeeStrategyRegistry(
                List.of(new FakeEthFeeStrategy(), new FakeEthFeeStrategy())))
                .isInstanceOf(IllegalStateException.class);
    }

    private static final class FakeEthFeeStrategy implements FeeStrategy {
        @Override
        public Chain chain() {
            return Chain.ETH;
        }

        @Override
        public FeeQuote quote(FeeQuoteRequest req) {
            return FeeQuote.builder()
                    .chain(Chain.ETH)
                    .feeAmount(new BigDecimal("0.0021"))
                    .feeCoinSymbol("ETH")
                    .build();
        }
    }
}
