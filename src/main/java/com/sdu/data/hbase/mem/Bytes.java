package com.sdu.data.hbase.mem;

public class Bytes {

    // boolean占用1字节
    public static final int SIZEOF_BOOLEAN = 1;
    //
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;
    // char占用2字节
    public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;
    // double占用8字节
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;
    // float占用4字节
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;
    // int占用4字节
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;
    // long占用8字节
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;
    // short占用2字节
    public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;

}
