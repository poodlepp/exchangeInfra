package com.exchange.wallet.signer;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;
import com.exchange.wallet.signer.hd.Bip32HdKeyDeriver;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SignerImplTest {

    static class FakeChainSigner implements ChainSpecificSigner {
        SignedTx returnValue = SignedTx.builder().chain(Chain.ETH).hexEncoded("0xdead").build();
        byte[] privateKeyAtCall;
        @Override public Chain chain() { return Chain.ETH; }
        @Override public SignedTx sign(RawTx rawTx, byte[] privateKey) {
            privateKeyAtCall = privateKey.clone();
            return returnValue;
        }
    }

    @Test
    void routes_to_matching_chain_signer_and_wipes_after_use() {
        KeyMaterialService kms = mock(KeyMaterialService.class);
        Bip32HdKeyDeriver deriver = mock(Bip32HdKeyDeriver.class);
        FakeChainSigner ethSigner = new FakeChainSigner();
        SignerImpl signer = new SignerImpl(kms, deriver, List.of(ethSigner));

        byte[] seed = new byte[64];
        for (int i = 0; i < seed.length; i++) seed[i] = (byte) i;
        when(kms.loadSeed("k1")).thenReturn(seed);

        byte[] priv = new byte[32];
        for (int i = 0; i < 32; i++) priv[i] = (byte) (i + 100);
        Bip32HdKeyDeriver.HdKey hd = new Bip32HdKeyDeriver.HdKey(priv, new byte[65], "m/44'/60'/0'/0/0");
        when(deriver.derive(seed, "m/44'/60'/0'/0/0")).thenReturn(hd);

        RawTx raw = RawTx.builder().chain(Chain.ETH).build();
        KeyRef ref = KeyRef.builder().chain(Chain.ETH).keyId("k1").hdPath("m/44'/60'/0'/0/0").build();

        SignedTx out = signer.sign(raw, ref);

        assertThat(out.getHexEncoded()).isEqualTo("0xdead");
        assertThat(ethSigner.privateKeyAtCall).contains((byte) 100);
        assertThat(priv).containsOnly((byte) 0);
        assertThat(seed).containsOnly((byte) 0);
    }

    @Test
    void chain_mismatch_rejected() {
        SignerImpl signer = new SignerImpl(mock(KeyMaterialService.class),
                mock(Bip32HdKeyDeriver.class), List.of(new FakeChainSigner()));
        RawTx raw = RawTx.builder().chain(Chain.BTC).build();
        KeyRef ref = KeyRef.builder().chain(Chain.ETH).build();
        assertThatThrownBy(() -> signer.sign(raw, ref))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void no_chain_signer_registered() {
        SignerImpl signer = new SignerImpl(mock(KeyMaterialService.class),
                mock(Bip32HdKeyDeriver.class), List.of());
        RawTx raw = RawTx.builder().chain(Chain.ETH).build();
        KeyRef ref = KeyRef.builder().chain(Chain.ETH).build();
        assertThatThrownBy(() -> signer.sign(raw, ref))
                .isInstanceOf(IllegalStateException.class);
    }
}
