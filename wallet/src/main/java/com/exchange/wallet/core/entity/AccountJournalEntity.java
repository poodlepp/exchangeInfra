package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@TableName("account_journal")
public class AccountJournalEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String traceId;
    private Long accountId;
    private Long coinId;
    private String bizType;
    private Long bizId;
    private Integer direction;
    private BigDecimal amount;
    private BigDecimal balanceAfter;
    private String remark;
    private LocalDateTime createdAt;
}
