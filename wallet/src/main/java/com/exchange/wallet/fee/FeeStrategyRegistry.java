package com.exchange.wallet.fee;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.FeeQuote;
import com.exchange.wallet.chain.api.dto.FeeQuoteRequest;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 按 chain 路由的手续费策略注册表。业务层依赖一个 Bean，调用 {@link #quote} 即可。
 */
@Component
public class FeeStrategyRegistry {

    private final Map<Chain, FeeStrategy> strategies;

    public FeeStrategyRegistry(List<FeeStrategy> beans) {
        this.strategies = beans.stream()
                .collect(Collectors.toUnmodifiableMap(FeeStrategy::chain, s -> s));
    }

    public FeeQuote quote(FeeQuoteRequest req) {
        FeeStrategy s = strategies.get(req.getChain());
        if (s == null) {
            throw new IllegalStateException("no FeeStrategy for chain " + req.getChain());
        }
        return s.quote(req);
    }
}
