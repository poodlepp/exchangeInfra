package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("treasury_policy")
public class TreasuryPolicyEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String chain;
    private Long coinId;
    private BigDecimal hotLowRatio;
    private BigDecimal hotHighRatio;
    private BigDecimal hotTargetRatio;
    private BigDecimal totalTarget;
    private BigDecimal dailyOutflowAvg;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
