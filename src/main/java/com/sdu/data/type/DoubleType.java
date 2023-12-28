package com.sdu.data.type;

public class DoubleType extends BasicType {

    public static final DoubleType NULLABLE_DOUBLE = new DoubleType(true);
    public static final DoubleType NOTNULL_DOUBLE = new DoubleType(false);

    private DoubleType(boolean nullable) {
        super(nullable, LogicalType.DOUBLE);
    }
}
