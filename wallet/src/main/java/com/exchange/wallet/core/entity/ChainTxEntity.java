package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("chain_tx")
public class ChainTxEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String chain;
    private String txHash;
    private Integer vout;
    private Long blockHeight;
    private String blockHash;
    private String parentHash;
    private String fromAddress;
    private String toAddress;
    private Long coinId;
    private BigDecimal amount;
    private Integer direction;
    private Integer confirmCount;
    private Integer status;
    private String rawJson;
    private LocalDateTime createdAt;
}
