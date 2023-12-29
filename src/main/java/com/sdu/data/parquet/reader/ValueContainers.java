package com.sdu.data.parquet.reader;

import com.sdu.data.type.ListType;
import com.sdu.data.type.Type;

public class ValueContainers {

    public static ValueContainer createValueContainer(Type type) {
        switch (type.type()) {
            case BOOLEAN:
                return new BooleanValueContainer();
            case STRING:
                return new StringValueContainer();
            case DOUBLE:
                return new DoubleValueContainer();
            case FLOAT:
                return new FloatValueContainer();
            case INT:
                return new IntValueContainer();
            case LIST:
                ListType listType = (ListType) type;
                return new ListValueContainerImpl(listType.getElementType());
            case ROW:
            default:
                throw new UnsupportedOperationException("unsupported type: " + type);

        }
    }
}
