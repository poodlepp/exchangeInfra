package com.exchange.wallet.signer.hd;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class Bip39MnemonicServiceTest {

    private final Bip39MnemonicService svc = new Bip39MnemonicService();

    @Test
    void generated_mnemonic_is_valid_12_words() {
        String mnemonic = svc.generateMnemonic();
        assertThat(mnemonic.split(" ")).hasSize(12);
        assertThat(svc.validate(mnemonic)).isTrue();
    }

    @Test
    void seed_is_64_bytes() {
        String mnemonic = svc.generateMnemonic();
        byte[] seed = svc.mnemonicToSeed(mnemonic, "");
        assertThat(seed).hasSize(64);
    }

    @Test
    void same_mnemonic_same_seed() {
        String mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        byte[] s1 = svc.mnemonicToSeed(mnemonic, "");
        byte[] s2 = svc.mnemonicToSeed(mnemonic, "");
        assertThat(s1).isEqualTo(s2);
    }
}
