package com.sdu.data.parquet.type;

public class Types {

    private Types() { }

    public static IntType intType(boolean nullable) {
        return nullable ? IntType.NULLABLE_INT : IntType.NOTNULL_INT;
    }

    public static FloatType floatType(boolean nullable) {
        return nullable ? FloatType.NULLABLE_FLOAT : FloatType.NOTNULL_FLOAT;
    }

    public static DoubleType doubleType(boolean nullable) {
        return nullable ? DoubleType.NULLABLE_DOUBLE : DoubleType.NOTNULL_DOUBLE;
    }

    public static BooleanType booleanType(boolean nullable) {
        return nullable ? BooleanType.NULLABLE_BOOLEAN : BooleanType.NOTNULL_BOOLEAN;
    }

    public static StringType stringType(boolean nullable) {
        return nullable ? StringType.NULLABLE_STRING : StringType.NOTNULL_STRING;
    }

    public static MapType mapType(boolean nullable, Type keyType, Type valueType) {
        return new MapType(nullable, keyType, valueType);
    }

    public static ListType listType(boolean nullable, Type elementType) {
        return new ListType(nullable, elementType);
    }

    public static RowType rowType(boolean nullable, String[] fieldNames, Type[] fieldTypes) {
        return new RowType(nullable, fieldNames, fieldTypes);
    }
}
