package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("sweep_order")
public class SweepOrderEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String chain;
    private Long coinId;
    private String srcAddress;
    private String dstAddress;
    private BigDecimal amount;
    private String status;
    private String dripTxHash;
    private String sweepTxHash;
    private Long nonce;
    private Integer retryCount;

    @Version
    private Integer version;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
