package com.exchange.common.mq;

import com.exchange.common.mq.consumed.ConsumedRecordStore;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class IdempotentEventHandler<T extends DomainEvent> {

    private final ConsumedRecordStore consumedRecordStore;

    protected IdempotentEventHandler(ConsumedRecordStore consumedRecordStore) {
        this.consumedRecordStore = consumedRecordStore;
    }

    public final void onMessage(T event) {
        if (consumedRecordStore.exists(event.eventId(), handlerName())) {
            log.debug("idempotent skip event={} handler={}", event.eventId(), handlerName());
            return;
        }
        try {
            handle(event);
            consumedRecordStore.markConsumed(event.eventId(), handlerName());
        } catch (RetriableException e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("non-retriable error event={} handler={} cause={}",
                    event.eventId(), handlerName(), e.toString(), e);
            throw e;
        }
    }

    protected abstract void handle(T event);
    protected abstract String handlerName();
}
