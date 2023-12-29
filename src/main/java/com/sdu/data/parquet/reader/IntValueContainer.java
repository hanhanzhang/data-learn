package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public class IntValueContainer extends BasicValueContainer {

    private Integer value;

    public IntValueContainer() {
        super(LogicalType.INT);
    }

    @Override
    public void addInteger(Integer value) {
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
