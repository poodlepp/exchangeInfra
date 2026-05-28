package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("wallet_address")
public class WalletAddressEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private Long userId;
    private String chain;
    private String address;
    private String hdPath;
    private String keyId;
    private Integer status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
