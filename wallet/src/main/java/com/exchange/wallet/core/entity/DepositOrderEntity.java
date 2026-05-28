package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("deposit_order")
public class DepositOrderEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private Long userId;
    private Long coinId;
    private Long chainTxId;
    private BigDecimal amount;
    private String status;
    private Integer confirmCount;

    @Version
    private Integer version;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
