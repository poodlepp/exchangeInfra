package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("address_balance")
public class AddressBalanceEntity {

    private String chain;
    private String address;
    private Long coinId;
    private BigDecimal balance;
    private Long blockHeight;
    private LocalDateTime refreshedAt;
}
