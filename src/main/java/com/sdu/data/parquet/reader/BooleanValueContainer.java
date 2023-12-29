package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public class BooleanValueContainer extends BasicValueContainer {

    private Boolean value;

    public BooleanValueContainer() {
        super(LogicalType.BOOLEAN);
    }

    @Override
    public void addBoolean(Boolean value) {
        this.value = value;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void reset() {
        this.value = null;
    }
}
