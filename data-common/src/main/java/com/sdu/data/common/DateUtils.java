package com.sdu.data.common;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class DateUtils {

    private DateUtils() {

    }

    public static String formatDate(long timestamp, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        return formatter.format(
                LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(timestamp),
                        ZoneId.systemDefault()
                )
        );
    }

    public static void sleep(long time, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException("sleep interrupter", e);
        }
    }
}
