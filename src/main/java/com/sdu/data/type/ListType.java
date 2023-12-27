package com.sdu.data.type;

public class ListType implements Type {

    private final Type elementType;

    public ListType(Type elementType) {
        this.elementType = elementType;
    }

    public Type getElementType() {
        return elementType;
    }

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public TypeEnum type() {
        return TypeEnum.LIST;
    }
}
