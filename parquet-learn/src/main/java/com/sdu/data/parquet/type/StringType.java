package com.sdu.data.parquet.type;

public class StringType extends BasicType {

    public static final StringType NULLABLE_STRING = new StringType(true);
    public static final StringType NOTNULL_STRING = new StringType(false);

    private StringType(boolean nullable) {
        super(nullable, LogicalType.STRING);
    }
}
