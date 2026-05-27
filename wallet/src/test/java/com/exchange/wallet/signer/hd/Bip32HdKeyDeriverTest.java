package com.exchange.wallet.signer.hd;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Bip32HdKeyDeriverTest {

    private final Bip39MnemonicService bip39 = new Bip39MnemonicService();
    private final Bip32HdKeyDeriver deriver = new Bip32HdKeyDeriver();

    @Test
    void deterministic_derivation() {
        String mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        byte[] seed = bip39.mnemonicToSeed(mnemonic, "");
        Bip32HdKeyDeriver.HdKey k1 = deriver.derive(seed, "m/44'/60'/0'/0/0");
        Bip32HdKeyDeriver.HdKey k2 = deriver.derive(seed, "m/44'/60'/0'/0/0");
        assertThat(k1.getPrivateKey()).hasSize(32).isEqualTo(k2.getPrivateKey());
    }

    @Test
    void different_paths_different_keys() {
        String mnemonic = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";
        byte[] seed = bip39.mnemonicToSeed(mnemonic, "");
        Bip32HdKeyDeriver.HdKey k0 = deriver.derive(seed, "m/44'/60'/0'/0/0");
        Bip32HdKeyDeriver.HdKey k1 = deriver.derive(seed, "m/44'/60'/0'/0/1");
        assertThat(k0.getPrivateKey()).isNotEqualTo(k1.getPrivateKey());
    }

    @Test
    void rejects_bad_path() {
        assertThatThrownBy(() -> deriver.derive(new byte[64], "44'/60'/0'/0/0"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
