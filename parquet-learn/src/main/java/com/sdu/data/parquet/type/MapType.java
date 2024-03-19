package com.sdu.data.parquet.type;

public class MapType extends ComplexType {

    private final Type keyType;
    private final Type valueType;

    public MapType(boolean nullable, Type keyType, Type valueType) {
        super(nullable, LogicalType.MAP);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public Type getKeyType() {
        return keyType;
    }

    public Type getValueType() {
        return valueType;
    }
}
