package com.exchange.common.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public final class DateUtil {

    public static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_PATTERN = "yyyy-MM-dd";
    public static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);

    private DateUtil() {
    }

    public static String format(LocalDateTime dateTime) {
        return dateTime == null ? null : DATETIME_FORMATTER.format(dateTime);
    }

    public static String format(LocalDate date) {
        return date == null ? null : DATE_FORMATTER.format(date);
    }

    public static LocalDateTime parseDateTime(String text) {
        return text == null || text.isEmpty() ? null : LocalDateTime.parse(text, DATETIME_FORMATTER);
    }

    public static LocalDate parseDate(String text) {
        return text == null || text.isEmpty() ? null : LocalDate.parse(text, DATE_FORMATTER);
    }

    public static long toEpochMilli(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public static LocalDateTime fromEpochMilli(long epochMilli) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilli), ZoneId.systemDefault());
    }

    public static LocalDateTime now() {
        return LocalDateTime.now();
    }
}
