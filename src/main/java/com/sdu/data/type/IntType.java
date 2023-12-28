package com.sdu.data.type;

public class IntType extends BasicType {

    public static final IntType NULLABLE_INT = new IntType(true);
    public static final IntType NOTNULL_INT = new IntType(false);

    private IntType(boolean nullable) {
        super(nullable, LogicalType.INT);
    }
}
