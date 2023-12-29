package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public class DoubleValueContainer extends BasicValueContainer {

    private Double value;

    public DoubleValueContainer() {
        super(LogicalType.DOUBLE);
    }

    @Override
    public void addDouble(Double value) {
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
