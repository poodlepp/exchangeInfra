package com.exchange.common.mq.consumed;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class ConsumedRecordStore {

    private final ConsumedRecordMapper mapper;

    public boolean exists(String eventId, String handlerName) {
        return mapper.countByKey(eventId, handlerName) > 0;
    }

    public void markConsumed(String eventId, String handlerName) {
        mapper.insertIgnore(eventId, handlerName, LocalDateTime.now());
    }
}
