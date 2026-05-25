package com.exchange.common.api;

import lombok.Getter;

@Getter
public enum ResultCode {
    SUCCESS(200, "OK"),
    FAIL(500, "Internal Server Error"),
    UNAUTHORIZED(401, "Unauthorized"),
    FORBIDDEN(403, "Forbidden"),
    PARAM_ERROR(400, "Bad Request"),
    NOT_FOUND(404, "Not Found");

    private final int code;
    private final String message;

    ResultCode(int code, String message) {
        this.code = code;
        this.message = message;
    }
}
