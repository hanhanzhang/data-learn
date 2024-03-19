package com.sdu.data.hbase.mem;

import java.nio.ByteBuffer;

public class ByteBufferUtils {

    private static final boolean UNSAFE_AVAIL = isUnsafeAvailable();

    private ByteBufferUtils() {

    }

    public static void copyToByteBuffer(ByteBuffer out, int outOffset, byte[] in, int inOffset, int length) {
        if (out.hasArray()) {       // on heap: 不同线程操作字节数组的不同地址故不存在线程安全问题, 注意这里也没有更改out的position等属性信息
            System.arraycopy(in, inOffset, out.array(), outOffset, length);
        } else if (UNSAFE_AVAIL) {  // off heap: 不同线程操作物理内存不同地址故不存在线程安全问题,

        } else {
            ByteBuffer outTemp = out.duplicate();   // 实质还是操作不同数组地址, 每个线程操作各自持有ByteBuffer对象
            outTemp.position(outOffset);
            outTemp.put(in, inOffset, length);
        }
    }

    private static boolean isUnsafeAvailable() {
        throw new RuntimeException("");
    }
}
