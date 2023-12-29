package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public abstract class BasicValueContainer implements ValueContainer {

    private final LogicalType type;

    public BasicValueContainer(LogicalType type) {
        this.type = type;
    }

    public void addInteger(Integer value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void addFloat(Float value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void addDouble(Double value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void addString(String value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    public void addBoolean(Boolean value) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public LogicalType valueType() {
        return type;
    }
}
