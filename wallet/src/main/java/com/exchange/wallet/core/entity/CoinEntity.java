package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("coin")
public class CoinEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String symbol;
    private String chain;
    private String contract;
    private Integer decimals;
    private Integer status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
