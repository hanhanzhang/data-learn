package com.sdu.data.parquet.reader;

import static com.sdu.data.parquet.reader.ValueContainers.createValueContainer;

import java.util.LinkedList;
import java.util.List;

import com.sdu.data.type.LogicalType;
import com.sdu.data.type.Type;

public class ListValueContainerImpl implements ListValueContainer {


    private final Type elementType;
    private List<Object> elements;

    public ListValueContainerImpl(Type elementType) {
        this.elementType = elementType;
    }

    @Override
    public void addValue(Object value) {
        if (elements == null) {
            elements = new LinkedList<>();
        }
        elements.add(value);
    }

    @Override
    public Type getElementType() {
        return elementType;
    }

    @Override
    public ValueContainer getElementValueContainer() {
        return createValueContainer(elementType);
    }

    @Override
    public List<Object> getValue() {
        return elements;
    }

    @Override
    public void reset() {
        this.elements = null;
    }

    @Override
    public LogicalType valueType() {
        return LogicalType.LIST;
    }

}
