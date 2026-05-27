package com.exchange.wallet.signer.hd;

import org.springframework.stereotype.Component;
import org.web3j.crypto.MnemonicUtils;
import java.security.SecureRandom;

@Component
public class Bip39MnemonicService {

    private static final SecureRandom RNG = new SecureRandom();

    public String generateMnemonic() {
        byte[] entropy = new byte[16];   // 128 bit → 12 词
        RNG.nextBytes(entropy);
        return MnemonicUtils.generateMnemonic(entropy);
    }

    public byte[] mnemonicToSeed(String mnemonic, String passphrase) {
        return MnemonicUtils.generateSeed(mnemonic, passphrase == null ? "" : passphrase);
    }

    public boolean validate(String mnemonic) {
        return MnemonicUtils.validateMnemonic(mnemonic);
    }
}
