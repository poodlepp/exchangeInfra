package com.exchange.wallet.signer;

import com.exchange.wallet.chain.api.Chain;
import com.exchange.wallet.chain.api.Signer;
import com.exchange.wallet.chain.api.dto.KeyRef;
import com.exchange.wallet.chain.api.dto.RawTx;
import com.exchange.wallet.chain.api.dto.SignedTx;
import com.exchange.wallet.signer.hd.Bip32HdKeyDeriver;
import com.exchange.wallet.signer.kms.AesGcmCipher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class SignerImpl implements Signer {

    private final KeyMaterialService keyMaterialService;
    private final Bip32HdKeyDeriver deriver;
    private final List<ChainSpecificSigner> chainSigners;

    @Override
    public SignedTx sign(RawTx rawTx, KeyRef keyRef) {
        if (rawTx.getChain() != keyRef.getChain()) {
            throw new IllegalArgumentException("chain mismatch: rawTx=" + rawTx.getChain()
                    + " keyRef=" + keyRef.getChain());
        }

        Map<Chain, ChainSpecificSigner> registry = chainSigners.stream()
                .collect(Collectors.toMap(ChainSpecificSigner::chain, s -> s));
        ChainSpecificSigner cs = registry.get(rawTx.getChain());
        if (cs == null) {
            throw new IllegalStateException("no ChainSpecificSigner for " + rawTx.getChain());
        }

        byte[] seed = null;
        Bip32HdKeyDeriver.HdKey hd = null;
        byte[] priv = null;
        try {
            seed = keyMaterialService.loadSeed(keyRef.getKeyId());
            hd = deriver.derive(seed, keyRef.getHdPath());
            priv = hd.getPrivateKey();
            return cs.sign(rawTx, priv);
        } finally {
            AesGcmCipher.wipe(seed);
            if (hd != null) {
                AesGcmCipher.wipe(hd.getPrivateKey());
            }
            AesGcmCipher.wipe(priv);
        }
    }
}
