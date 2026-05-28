package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("treasury_movement")
public class TreasuryMovementEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String chain;
    private Long coinId;
    private String direction;
    private BigDecimal amount;
    private String status;
    private String psbt;
    private String txHash;
    private String proposer;
    private String approverList;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
