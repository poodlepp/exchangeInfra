package com.exchange.common.mq.outbox;

public enum OutboxStatus {
    PENDING(0), SENT(1), FAILED(2);

    public final int code;
    OutboxStatus(int code) { this.code = code; }

    public static OutboxStatus of(int code) {
        for (OutboxStatus s : values()) if (s.code == code) return s;
        throw new IllegalArgumentException("unknown OutboxStatus code: " + code);
    }
}
