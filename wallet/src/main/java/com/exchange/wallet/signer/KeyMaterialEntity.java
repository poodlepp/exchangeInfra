package com.exchange.wallet.signer;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("key_material")
public class KeyMaterialEntity {
    @TableId(type = IdType.INPUT)
    private Long id;
    private String keyId;
    private String keyType;
    private byte[] cipherText;
    private byte[] iv;
    private String kmsAlias;
    private Integer algoVersion;
    private LocalDateTime createdAt;
}
