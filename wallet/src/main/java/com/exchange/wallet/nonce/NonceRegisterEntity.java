package com.exchange.wallet.nonce;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("nonce_register")
public class NonceRegisterEntity {

    @TableId(type = IdType.INPUT)
    private String chain;

    private String address;

    private Long nextNonce;

    private Long onChainNonce;

    private LocalDateTime reconciledAt;

    @Version
    private Integer version;
}
