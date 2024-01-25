package com.sdu.data.hbase.mem;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class Chunk implements Serializable {

    // HBase为降低GC影响缓存在内存中的数据会寄存在Chunk中(内存池技术, 即防止内存碎片化减少FGC, 也可防止频繁申请内存导致YGC)
    // HBase Chunk可同时被多个线程存储数据, 故需要保证线程安全, HBase对线程访问特别巧妙(基于JDK ByteBuffer缓存数据):
    // 1. 每个线程分配ByteBuffer写地址, 这部分通过JDK AtomicInteger保障线程安全
    // 2. 各线程分配ByteBuffer写地址后, 若是HeapByteBuffer则转为操作byte[], 若是DirectByteBuffer则各线程操作地址不同故无线程安全问题
    // 对于第二步请查看ByteBufferUtils.copyToByteBuffer()方法

    protected static final int UNINITIALIZED = -1;
    protected static final int OOM = -2;

    protected final int id;
    protected final int size;
    protected ByteBuffer data;
    // 下个记录可写地址
    protected AtomicInteger nextFreeOffset = new AtomicInteger(-1);

    public Chunk(int id, int size) {
        this.id = id;
        this.size = size;
    }

    public void initialize() {
        assert nextFreeOffset.get() == UNINITIALIZED;
        try {
            allocateByteBuffer();
        } catch (OutOfMemoryError e) {
            boolean failInit = nextFreeOffset.compareAndSet(UNINITIALIZED, OOM);
            assert failInit;
            throw e;
        }

        boolean init = nextFreeOffset.compareAndSet(UNINITIALIZED, Bytes.SIZEOF_INT);
        Preconditions.checkArgument(init, "multiple threads tried to init same chunk");
    }

    // 申请内存, 若是申请成功则返回写地址, 否则返回-1
    // NOTE: 可能会有多线程访问, 这里需要一直轮询申请写地址直至更新下次地址成功位置
    public int alloc(int size) {
        while (true) {
            int oldOffset = nextFreeOffset.get();
            if (oldOffset == UNINITIALIZED) {   // 未初始化阻塞当前线程一段时间
                Thread.yield();
                continue;
            }
            if (oldOffset == OOM) {             // 无可用内存, 则返回-1
                return -1;
            }

            if (size + oldOffset > this.size) { // 说明没有充足的内存可用, 则返回-1
                return -1;
            }
            if (nextFreeOffset.compareAndSet(oldOffset, oldOffset + size)) {
                return oldOffset;
            }
        }
    }

    public ByteBuffer getBuffer() {
        return data;
    }

    public void reset() {
        if (nextFreeOffset.get() != UNINITIALIZED) {
            nextFreeOffset.set(UNINITIALIZED);
        }
    }

    public abstract void allocateByteBuffer();
}
