package com.exchange.common.mq;

import com.exchange.common.mq.consumed.ConsumedRecordStore;
import org.junit.jupiter.api.Test;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class IdempotentEventHandlerTest {

    static class Foo extends AbstractDomainEvent {
        @Override public String aggregateId() { return "a"; }
        @Override public String eventType() { return "t.foo"; }
    }

    static class FooHandler extends IdempotentEventHandler<Foo> {
        final AtomicInteger calls = new AtomicInteger();
        FooHandler(ConsumedRecordStore s) { super(s); }
        @Override protected void handle(Foo event) { calls.incrementAndGet(); }
        @Override protected String handlerName() { return "test.foo.handler"; }
    }

    @Test
    void duplicate_event_skipped() {
        ConsumedRecordStore store = mock(ConsumedRecordStore.class);
        FooHandler h = new FooHandler(store);
        Foo e = new Foo();
        when(store.exists(e.eventId(), "test.foo.handler")).thenReturn(true);
        h.onMessage(e);
        assertThat(h.calls.get()).isZero();
        verify(store, never()).markConsumed(any(), any());
    }

    @Test
    void first_event_handled_then_marked() {
        ConsumedRecordStore store = mock(ConsumedRecordStore.class);
        FooHandler h = new FooHandler(store);
        Foo e = new Foo();
        when(store.exists(e.eventId(), "test.foo.handler")).thenReturn(false);
        h.onMessage(e);
        assertThat(h.calls.get()).isOne();
        verify(store).markConsumed(e.eventId(), "test.foo.handler");
    }

    @Test
    void retriable_exception_propagates_without_marking() {
        ConsumedRecordStore store = mock(ConsumedRecordStore.class);
        FooHandler h = new FooHandler(store) {
            @Override protected void handle(Foo event) { throw new RetriableException("transient"); }
        };
        Foo e = new Foo();
        when(store.exists(e.eventId(), "test.foo.handler")).thenReturn(false);
        assertThatThrownBy(() -> h.onMessage(e)).isInstanceOf(RetriableException.class);
        verify(store, never()).markConsumed(any(), any());
    }
}
