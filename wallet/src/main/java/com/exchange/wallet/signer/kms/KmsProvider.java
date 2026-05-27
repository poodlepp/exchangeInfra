package com.exchange.wallet.signer.kms;

public interface KmsProvider {
    byte[] resolveDataKey(String alias);
    String defaultAlias();
}
