package com.sdu.data.type;

public class Types {

    private Types() { }

    public static IntType intType() {
        return IntType.INT_TYPE;
    }

    public static FloatType floatType() {
        return FloatType.FLOAT_TYPE;
    }

    public static DoubleType doubleType() {
        return DoubleType.DOUBLE_TYPE;
    }

    public static BooleanType booleanType() {
        return BooleanType.BOOLEAN_TYPE;
    }

    public static StringType stringType() {
        return StringType.STRING_TYPE;
    }

    public static MapType mapType(Type keyType, Type valueType) {
        return new MapType(keyType, valueType);
    }

    public static ListType listType(Type elementType) {
        return new ListType(elementType);
    }
}
