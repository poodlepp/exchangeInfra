package com.exchange.wallet.signer.kms;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import static org.assertj.core.api.Assertions.*;

class AesGcmCipherTest {

    private final AesGcmCipher cipher = new AesGcmCipher();

    @Test
    void encrypt_decrypt_roundtrip() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        byte[] plaintext = "super-secret-mnemonic seed".getBytes(StandardCharsets.UTF_8);

        AesGcmCipher.Cipherblob blob = cipher.encrypt(key, plaintext);
        byte[] decoded = cipher.decrypt(key, blob.getIv(), blob.getCipherText());

        assertThat(decoded).isEqualTo(plaintext);
        assertThat(blob.getIv()).hasSize(AesGcmCipher.IV_BYTES);
    }

    @Test
    void wrong_key_fails() {
        byte[] key1 = new byte[32];
        byte[] key2 = new byte[32];
        new SecureRandom().nextBytes(key1);
        new SecureRandom().nextBytes(key2);
        AesGcmCipher.Cipherblob blob = cipher.encrypt(key1, "x".getBytes());
        assertThatThrownBy(() -> cipher.decrypt(key2, blob.getIv(), blob.getCipherText()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void invalid_key_length_rejected() {
        assertThatThrownBy(() -> cipher.encrypt(new byte[16], new byte[]{1}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
