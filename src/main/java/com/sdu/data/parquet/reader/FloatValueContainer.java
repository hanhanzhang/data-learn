package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public class FloatValueContainer extends BasicValueContainer {

    private Float value;

    public FloatValueContainer() {
        super(LogicalType.FLOAT);
    }

    @Override
    public void addFloat(Float value) {
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
