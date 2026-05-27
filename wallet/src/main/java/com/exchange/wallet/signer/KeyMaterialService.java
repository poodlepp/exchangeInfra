package com.exchange.wallet.signer;

import com.exchange.common.util.SnowflakeIdGenerator;
import com.exchange.wallet.signer.kms.AesGcmCipher;
import com.exchange.wallet.signer.kms.KmsProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class KeyMaterialService {

    private final KeyMaterialMapper mapper;
    private final KmsProvider kmsProvider;
    private final AesGcmCipher cipher = new AesGcmCipher();

    /** 生成新的 HD 种子并加密落库；返回 keyId */
    public String storeHdSeed(byte[] seed) {
        String keyId = "hd-" + UUID.randomUUID();
        byte[] dataKey = kmsProvider.resolveDataKey(kmsProvider.defaultAlias());
        AesGcmCipher.Cipherblob blob = cipher.encrypt(dataKey, seed);

        KeyMaterialEntity row = new KeyMaterialEntity();
        row.setId(SnowflakeIdGenerator.nextDefaultId());
        row.setKeyId(keyId);
        row.setKeyType("HD_SEED");
        row.setCipherText(blob.getCipherText());
        row.setIv(blob.getIv());
        row.setKmsAlias(kmsProvider.defaultAlias());
        row.setAlgoVersion(1);
        row.setCreatedAt(LocalDateTime.now());
        mapper.insert(row);
        return keyId;
    }

    /** 解密返回明文种子。调用方必须用完后 wipe。 */
    public byte[] loadSeed(String keyId) {
        KeyMaterialEntity row = mapper.findByKeyId(keyId);
        if (row == null) throw new IllegalArgumentException("keyId not found: " + keyId);
        byte[] dataKey = kmsProvider.resolveDataKey(row.getKmsAlias());
        return cipher.decrypt(dataKey, row.getIv(), row.getCipherText());
    }
}
