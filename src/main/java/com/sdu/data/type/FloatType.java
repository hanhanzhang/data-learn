package com.sdu.data.type;

public class FloatType extends BasicType {

    public static final FloatType NULLABLE_FLOAT = new FloatType(true);
    public static final FloatType NOTNULL_FLOAT = new FloatType(false);

    private FloatType(boolean nullable) {
        super(nullable, LogicalType.FLOAT);
    }
}
