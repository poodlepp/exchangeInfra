package com.exchange.common.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Twitter Snowflake ID generator. workerId / datacenterId may be configured via the
 * {@code exchange.snowflake.worker-id} / {@code exchange.snowflake.datacenter-id} properties
 * (default 0 / 0). Thread-safe.
 */
@Slf4j
public class SnowflakeIdGenerator {

    private static final long EPOCH = 1735689600000L;

    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;

    public static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    public static final long MAX_DATACENTER_ID = ~(-1L << DATACENTER_ID_BITS);
    private static final long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    private final long workerId;
    private final long datacenterId;

    private long sequence = 0L;
    private long lastTimestamp = -1L;

    public SnowflakeIdGenerator(long workerId, long datacenterId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("workerId out of range [0, " + MAX_WORKER_ID + "]");
        }
        if (datacenterId < 0 || datacenterId > MAX_DATACENTER_ID) {
            throw new IllegalArgumentException("datacenterId out of range [0, " + MAX_DATACENTER_ID + "]");
        }
        this.workerId = workerId;
        this.datacenterId = datacenterId;
    }

    public synchronized long nextId() {
        long timestamp = currentMillis();
        if (timestamp < lastTimestamp) {
            long offset = lastTimestamp - timestamp;
            if (offset <= 5L) {
                try {
                    wait(offset << 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                timestamp = currentMillis();
                if (timestamp < lastTimestamp) {
                    throw new IllegalStateException("Clock moved backwards by " + (lastTimestamp - timestamp) + "ms");
                }
            } else {
                throw new IllegalStateException("Clock moved backwards by " + offset + "ms");
            }
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0L) {
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;

        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private long waitNextMillis(long last) {
        long timestamp = currentMillis();
        while (timestamp <= last) {
            timestamp = currentMillis();
        }
        return timestamp;
    }

    private long currentMillis() {
        return System.currentTimeMillis();
    }

    /**
     * Best-effort default workerId derived from MAC address + JVM PID hash. Useful when no explicit
     * configuration is provided.
     */
    public static long defaultWorkerId() {
        try {
            long pid = ProcessHandle.current().pid();
            long macHash = 0L;
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces != null && interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                byte[] mac = ni.getHardwareAddress();
                if (mac != null) {
                    for (byte b : mac) {
                        macHash = macHash * 31 + (b & 0xFF);
                    }
                    break;
                }
            }
            String name = ManagementFactory.getRuntimeMXBean().getName();
            return Math.abs((macHash ^ pid ^ name.hashCode())) % (MAX_WORKER_ID + 1);
        } catch (Exception e) {
            log.warn("Failed to compute default workerId, fallback to 0", e);
            return 0L;
        }
    }

    /**
     * 默认单例（基于 {@link #defaultWorkerId()} 自动派生）。供无 Spring 上下文的场景使用，
     * 例如领域事件构造、单元测试。生产代码请用注入的 {@code SnowflakeIdGenerator} Bean。
     */
    private static final class Holder {
        private static final SnowflakeIdGenerator INSTANCE =
                new SnowflakeIdGenerator(defaultWorkerId(), 0L);
    }

    public static long nextDefaultId() {
        return Holder.INSTANCE.nextId();
    }
}
