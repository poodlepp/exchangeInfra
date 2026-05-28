package com.exchange.wallet.core.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("hd_path")
public class HdPathEntity {

    @TableId(type = IdType.INPUT)
    private Long id;

    private String chain;
    private String hdPath;
    private LocalDateTime usedAt;
}
