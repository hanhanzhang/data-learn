package com.sdu.data.parquet.reader;

import com.sdu.data.type.Type;

public interface ListValueContainer extends ValueContainer {

    void addValue(Object value);

    ValueContainer getElementValueContainer();

    Type getElementType();
}
