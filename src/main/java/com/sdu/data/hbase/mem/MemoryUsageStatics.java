package com.sdu.data.hbase.mem;

import java.lang.management.ManagementFactory;

public class MemoryUsageStatics {

    public static MemoryUsageStatics INSTANCE = new MemoryUsageStatics();

    private MemoryUsageStatics() {

    }


    public long getInitHeapMB() {
        // jvm初始化堆内存, 即: -Xms
        return bytesToMB(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getInit());
    }

    public long getMaxHeapMB() {
        // jvm最大堆内存, 即: -Xmx
        return bytesToMB(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax());
    }

    public long getCommittedMB() {
        // jvm当前可用内存(包括已使用内存)
        // NOTE:
        // 随着堆内存使用增大, jvm可用内存随之增大, 当超过max阈值时则抛出OutOfMemoryError异常
        return bytesToMB(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getCommitted());
    }

    public long getUsedHeapMB() {
        // jvm
        return bytesToMB(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
    }

    private static long bytesToMB(long bytes) {
        return bytes >> 20;
    }

}

