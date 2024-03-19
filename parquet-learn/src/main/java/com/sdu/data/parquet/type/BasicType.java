package com.sdu.data.parquet.type;

public class BasicType implements Type {

    private final boolean nullable;
    private final LogicalType type;

    public BasicType(boolean nullable, LogicalType type) {
        this.nullable = nullable;
        this.type = type;
    }

    @Override
    public boolean isPrimary() {
        return true;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public LogicalType type() {
        return type;
    }
}
