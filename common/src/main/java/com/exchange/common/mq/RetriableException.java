package com.exchange.common.mq;

public class RetriableException extends RuntimeException {
    public RetriableException(String message) { super(message); }
    public RetriableException(String message, Throwable cause) { super(message, cause); }
}
