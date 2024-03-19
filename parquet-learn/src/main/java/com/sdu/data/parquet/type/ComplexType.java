package com.sdu.data.parquet.type;

public class ComplexType implements Type {

    private final boolean nullable;
    private final LogicalType logicalType;

    public ComplexType(boolean nullable, LogicalType logicalType) {
        this.nullable = nullable;
        this.logicalType = logicalType;
    }

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public LogicalType type() {
        return logicalType;
    }
}
