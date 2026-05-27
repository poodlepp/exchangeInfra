package com.exchange.wallet.signer.kms;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class LocalKeystoreKmsProvider implements KmsProvider {

    @Value("${wallet.signer.kms.local-master-key-base64:}")
    private String masterKeyB64;

    @Value("${wallet.signer.kms.default-alias:local:default}")
    private String defaultAlias;

    private final Map<String, byte[]> cache = new ConcurrentHashMap<>();

    @Override
    public byte[] resolveDataKey(String alias) {
        return cache.computeIfAbsent(alias, k -> {
            if (masterKeyB64 != null && !masterKeyB64.isBlank()) {
                byte[] decoded = Base64.getDecoder().decode(masterKeyB64);
                if (decoded.length != 32) {
                    throw new IllegalStateException("local master key must be 32 bytes (base64)");
                }
                return decoded;
            }
            byte[] generated = new byte[32];
            new SecureRandom().nextBytes(generated);
            return generated;
        });
    }

    @Override
    public String defaultAlias() {
        return defaultAlias;
    }
}
