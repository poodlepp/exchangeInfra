package com.exchange.wallet.chain.api;

public enum Chain {
    BTC, ETH, TRON;

    public static Chain of(String name) {
        return Chain.valueOf(name.toUpperCase());
    }
}
