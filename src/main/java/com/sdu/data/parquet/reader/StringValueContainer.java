package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public class StringValueContainer extends BasicValueContainer {

    private String value;

    public StringValueContainer() {
        super(LogicalType.STRING);
    }

    @Override
    public void addString(String value) {
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
