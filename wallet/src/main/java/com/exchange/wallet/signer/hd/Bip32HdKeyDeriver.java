package com.exchange.wallet.signer.hd;

import lombok.Data;
import org.springframework.stereotype.Component;
import org.web3j.crypto.Bip32ECKeyPair;

@Component
public class Bip32HdKeyDeriver {

    @Data
    public static class HdKey {
        private final byte[] privateKey;
        private final byte[] publicKey;
        private final String path;
    }

    public HdKey derive(byte[] seed, String hdPath) {
        Bip32ECKeyPair master = Bip32ECKeyPair.generateKeyPair(seed);
        int[] indices = parsePath(hdPath);
        Bip32ECKeyPair derived = Bip32ECKeyPair.deriveKeyPair(master, indices);
        byte[] priv = derived.getPrivateKey().toByteArray();
        priv = leftPad32(priv);
        byte[] pub = derived.getPublicKey().toByteArray();
        return new HdKey(priv, pub, hdPath);
    }

    private static int[] parsePath(String hdPath) {
        if (!hdPath.startsWith("m/")) throw new IllegalArgumentException("hdPath must start with m/");
        String[] parts = hdPath.substring(2).split("/");
        int[] out = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            boolean hard = p.endsWith("'");
            int n = Integer.parseInt(hard ? p.substring(0, p.length() - 1) : p);
            out[i] = hard ? (n | 0x80000000) : n;
        }
        return out;
    }

    private static byte[] leftPad32(byte[] in) {
        if (in.length == 32) return in;
        byte[] out = new byte[32];
        if (in.length < 32) {
            System.arraycopy(in, 0, out, 32 - in.length, in.length);
        } else {
            System.arraycopy(in, in.length - 32, out, 0, 32);
        }
        return out;
    }
}
