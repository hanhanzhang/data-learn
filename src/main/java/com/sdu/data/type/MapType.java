package com.sdu.data.type;

public class MapType implements Type {

    private final Type keyType;
    private final Type valueType;

    public MapType(Type keyType, Type valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public Type getKeyType() {
        return keyType;
    }

    public Type getValueType() {
        return valueType;
    }

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public TypeEnum type() {
        return TypeEnum.MAP;
    }
}
