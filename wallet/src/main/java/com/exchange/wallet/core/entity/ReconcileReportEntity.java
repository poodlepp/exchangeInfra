package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Data
@TableName("reconcile_report")
public class ReconcileReportEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private LocalDate reportDate;
    private String chain;
    private Long coinId;
    private BigDecimal ledgerTotal;
    private BigDecimal chainTotal;
    private BigDecimal delta;
    private String status;
    private String detailJson;
    private LocalDateTime createdAt;
}
