package com.sdu.data.hbase.test;

import static java.lang.String.format;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.sdu.data.hbase.mem.MemoryUsageStatics;

public class MemoryUsageTest {

    // jvm堆参数: -Xms512m -Xmx1024m

    @Test
    public void testHeapMemory() {
        long init = MemoryUsageStatics.INSTANCE.getInitHeapMB();
        long max = MemoryUsageStatics.INSTANCE.getMaxHeapMB();
        long committed = MemoryUsageStatics.INSTANCE.getCommittedMB();
        String msg = format("init heap %dmb, committed heap %d, max heap %dmb", init, committed, max);
        System.out.println(msg);
    }

    @Test
    public void testMemoryUse() {
        int allocateSize = 100 * 1024 * 1024; // 100mb
        long max = MemoryUsageStatics.INSTANCE.getMaxHeapMB();
        int maxChunkSize = (int) max / 100;

        Set<byte[]> chunks = new HashSet<>();
        while (true) {
            try {
                chunks.add(new byte[allocateSize]);
                long committed = MemoryUsageStatics.INSTANCE.getCommittedMB();
                String msg = format("committed heap %d, max heap %dmb", committed, max);
                System.out.println(msg);
            } catch (OutOfMemoryError e) {
                break;
            }
        }

        String msg = format("max available chunk size: %d, allocated chunk size: %d", maxChunkSize, chunks.size());
        System.out.println(msg);
    }
}
