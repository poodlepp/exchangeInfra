package com.exchange.common.mq.consumed;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import java.time.LocalDateTime;

@Data
@TableName("consumed_record")
public class ConsumedRecordEntity {
    private String eventId;
    private String handlerName;
    private LocalDateTime consumedAt;
}
