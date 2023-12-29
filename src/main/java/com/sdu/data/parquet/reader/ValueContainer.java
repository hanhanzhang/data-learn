package com.sdu.data.parquet.reader;

import com.sdu.data.type.LogicalType;

public interface ValueContainer {

    Object getValue();

    void reset();

    LogicalType valueType();

    default <T> T as(Class<T> clazz) {
        return clazz.cast(ValueContainer.this);
    }
}
