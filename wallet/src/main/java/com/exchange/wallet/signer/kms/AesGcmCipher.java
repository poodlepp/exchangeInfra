package com.exchange.wallet.signer.kms;

import lombok.Data;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Arrays;

public final class AesGcmCipher {

    public static final int IV_BYTES = 12;
    public static final int TAG_BITS = 128;

    private static final SecureRandom RNG = new SecureRandom();

    @Data
    public static class Cipherblob {
        private final byte[] iv;
        private final byte[] cipherText;
    }

    public Cipherblob encrypt(byte[] key, byte[] plaintext) {
        if (key.length != 32) throw new IllegalArgumentException("AES-256 key must be 32 bytes");
        byte[] iv = new byte[IV_BYTES];
        RNG.nextBytes(iv);
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            return new Cipherblob(iv, c.doFinal(plaintext));
        } catch (Exception e) {
            throw new IllegalStateException("AES-GCM encrypt failed", e);
        }
    }

    public byte[] decrypt(byte[] key, byte[] iv, byte[] cipherText) {
        if (key.length != 32) throw new IllegalArgumentException("AES-256 key must be 32 bytes");
        try {
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"),
                    new GCMParameterSpec(TAG_BITS, iv));
            return c.doFinal(cipherText);
        } catch (Exception e) {
            throw new IllegalStateException("AES-GCM decrypt failed", e);
        }
    }

    public static void wipe(byte[] data) {
        if (data != null) Arrays.fill(data, (byte) 0);
    }
}
