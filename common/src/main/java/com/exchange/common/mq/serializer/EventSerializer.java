package com.exchange.common.mq.serializer;

import com.exchange.common.mq.kafka.EventEnvelope;
import com.exchange.common.util.JsonUtil;
import org.springframework.stereotype.Component;

@Component
public class EventSerializer {
    public String toJson(EventEnvelope env) {
        return JsonUtil.toJson(env);
    }

    public EventEnvelope fromJson(String json) {
        return JsonUtil.fromJson(json, EventEnvelope.class);
    }
}
