package com.exchange.wallet.signer.kms;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Base64;
import static org.assertj.core.api.Assertions.assertThat;

class LocalKeystoreKmsProviderTest {

    @Test
    void uses_configured_master_key() {
        LocalKeystoreKmsProvider p = new LocalKeystoreKmsProvider();
        byte[] key = new byte[32];
        for (int i = 0; i < 32; i++) key[i] = (byte) i;
        ReflectionTestUtils.setField(p, "masterKeyB64", Base64.getEncoder().encodeToString(key));
        ReflectionTestUtils.setField(p, "defaultAlias", "local:default");

        assertThat(p.resolveDataKey("local:default")).isEqualTo(key);
        assertThat(p.resolveDataKey("local:default")).isEqualTo(key);
    }

    @Test
    void generates_random_key_when_unset() {
        LocalKeystoreKmsProvider p = new LocalKeystoreKmsProvider();
        ReflectionTestUtils.setField(p, "masterKeyB64", "");
        ReflectionTestUtils.setField(p, "defaultAlias", "local:default");

        byte[] k1 = p.resolveDataKey("local:default");
        byte[] k2 = p.resolveDataKey("local:default");
        assertThat(k1).hasSize(32).isEqualTo(k2);
    }
}
