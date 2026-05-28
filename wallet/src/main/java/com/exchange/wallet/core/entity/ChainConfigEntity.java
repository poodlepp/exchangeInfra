package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("chain_config")
public class ChainConfigEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String chain;
    private Integer depositConfirms;
    private Integer withdrawConfirms;
    private Integer reorgDepth;
    private BigDecimal minWithdraw;
    private BigDecimal maxWithdraw;
    private String feeStrategy;
    private BigDecimal maxGasPriceGwei;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
