package com.sdu.data.parquet.type;

public class BooleanType extends BasicType {

    public static final BooleanType NULLABLE_BOOLEAN = new BooleanType(true);
    public static final BooleanType NOTNULL_BOOLEAN = new BooleanType(false);

    private BooleanType(boolean nullable) {
        super(nullable, LogicalType.BOOLEAN);
    }
}
