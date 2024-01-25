package com.sdu.data.hbase.mem;

public class ClassSize {

    // Java对象内存占用字节数包括三部分: 对象头、实例数据及补齐数据(必须是8字节整数倍)
    // 对象头包括两部分(若对象为数组则包括三部分):
    // 1. MarkWord: 存储对象运行时的数据(eg: hashcode, 锁状态标记, gc分代年龄等), 64位操作系统占用8字节, 32位操作系统占用4字节
    // 2. 对象元数据指针: 对象指向它的类元数据的指针, 这部分涉及指针压缩概念, 开启指针压缩状况下占4字节, 未开启状况占8字节
    // 3. 数组长度: 这部分只是数组对象才有, 若是非数组对象就没这部分, 这部分占4字节
    //
    // 对齐填充: Java对象大小默认按8字节对齐,也就是说对象大小必须是8字节的倍数, 若是不够8字节的话那么就会进行对齐填充

    private ClassSize() {

    }


    private class HeapMemoryLayout implements MemoryLayout {

        @Override
        public int headSize() {
            // 若是开启指针压缩则对象指针为4字节, 这里则按8字节计算故有误差
            return 2 * oopSize();
        }

        @Override
        public int arrayHeaderSize() {
            return (int) align( 3L * oopSize());
        }
    }

    public static interface MemoryLayout {

        int headSize();

        int arrayHeaderSize();

        // 普通对象指针(ordinary object pointer)大小依赖操作系统和是否开启指针压缩(CompressedOops)
        default int oopSize() {
            return is32BitJVM() ? 4 : 8;
        }


        // 8字节对齐
        default long align(long num) {
            // 1. num + 7保证除以8时向上取整
            // 2. 除以8再乘以8
            return ((num + 7) >> 3) << 3;
        }
    }

    // 判断操作是否为32位操作系统
    private static boolean is32BitJVM() {
        String model = System.getProperty("sun.arch.data.model");
        return model != null && model.equals("32");
    }

}
