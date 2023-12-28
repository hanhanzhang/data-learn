package com.sdu.data.type;

public class ListType extends ComplexType {

    private final Type elementType;

    public ListType(boolean nullable, Type elementType) {
        super(nullable, LogicalType.LIST);
        this.elementType = elementType;
    }

    public Type getElementType() {
        return elementType;
    }
}
