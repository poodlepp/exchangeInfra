package com.exchange.wallet.chain.api;

import com.exchange.wallet.chain.api.dto.DerivedAddress;

public interface AddressDerivator {
    Chain chain();
    DerivedAddress derive(byte[] hdSeed, String hdPath);
}
