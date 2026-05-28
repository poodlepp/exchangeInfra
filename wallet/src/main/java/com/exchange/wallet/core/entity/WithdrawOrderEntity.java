package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("withdraw_order")
public class WithdrawOrderEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private Long userId;
    private Long coinId;
    private String chain;
    private String toAddress;
    private BigDecimal amount;
    private BigDecimal fee;
    private BigDecimal feeEstimate;
    private String status;
    private String failReason;
    private String riskDecision;
    private String signedRaw;
    private String txHash;
    private Long nonce;
    private String fromAddress;
    private Long replaceOfId;
    private Integer confirmCount;

    @Version
    private Integer version;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
