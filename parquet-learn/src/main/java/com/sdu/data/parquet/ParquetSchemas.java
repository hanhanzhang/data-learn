package com.sdu.data.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import com.sdu.data.parquet.type.BasicType;
import com.sdu.data.parquet.type.RowType;

public class ParquetSchemas {

    public static final String ROW_MESSAGE = "message";

    public static final String ARRAY_NAME = "list";
    public static final String ARRAY_ELEMENT_NAME = "element";

    public static final String MAP_NAME = "key_value";
    public static final String MAP_KEY_NAME = "key";
    public static final String MAP_VALUE_NAME = "value";

    private ParquetSchemas() { }

    public static MessageType convertParquetSchema(RowType rowType) {
        List<Type> types = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            com.sdu.data.parquet.type.Type type = rowType.getFieldType(i);
            String fieldName = rowType.getFieldName(i);
            types.add(createParquetType(fieldName, type));
        }
        return new MessageType(ROW_MESSAGE, types);
    }

    public static RowType convertRowType(MessageType messageType, boolean standardSchema) {
        int fieldCount = messageType.getFieldCount();
        String[] fieldNames = new String[fieldCount];
        com.sdu.data.parquet.type.Type[] fieldTypes = new com.sdu.data.parquet.type.Type[fieldCount];
        for (int i = 0; i < fieldCount; ++i) {
            Type type = messageType.getType(i);
            fieldNames[i] = type.getName();
            fieldTypes[i] = createType(type);
        }
        return new RowType(false, fieldNames, fieldTypes);
    }

    private static com.sdu.data.parquet.type.Type createType(Type type) {
        if (type.isPrimitive()) {
            return createBasicType(type.asPrimitiveType());
        }
        return createComplexType(type.asGroupType());
    }

    private static com.sdu.data.parquet.type.Type createBasicType(PrimitiveType type) {
        boolean nullable = type.isRepetition(Type.Repetition.OPTIONAL);
        switch (type.getPrimitiveTypeName()) {
            case INT32:
                return com.sdu.data.parquet.type.Types.intType(nullable);
            case FLOAT:
                return com.sdu.data.parquet.type.Types.floatType(nullable);
            case DOUBLE:
                return com.sdu.data.parquet.type.Types.doubleType(nullable);
            case BINARY:
                return com.sdu.data.parquet.type.Types.stringType(nullable);
            case BOOLEAN:
                return com.sdu.data.parquet.type.Types.booleanType(nullable);
            default:
                throw new UnsupportedOperationException("unsupported primary parquet type: " + type.getPrimitiveTypeName());
        }
    }

    private static com.sdu.data.parquet.type.Type createComplexType(GroupType type) {
        GroupType childrenType = type.getType(0).asGroupType();
        boolean nullable = childrenType.isRepetition(Type.Repetition.OPTIONAL);
        switch (childrenType.getFieldCount()) {
            case 1: // LIST
                Type elementType = childrenType.getType(0);
                return com.sdu.data.parquet.type.Types.listType(nullable, createType(elementType));
            case 2: // MAP
                Type keyType = childrenType.getType(0);
                Type valueType = childrenType.getType(1);
                return com.sdu.data.parquet.type.Types.mapType(nullable, createType(keyType), createType(valueType));
            default:
                throw new UnsupportedOperationException("unsupported primary parquet type: " + type);
        }
    }

    private static Type createParquetType(String name, com.sdu.data.parquet.type.Type type) {
        if (type.isPrimary()) {
            return createParquetPrimaryType(name, (BasicType) type);
        }
        return createParquetGroupType(name, type);
    }

    private static PrimitiveType createParquetPrimaryType(String name, BasicType type) {
        Type.Repetition repetition = type.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        switch (type.type()) {
            case INT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                            .named(name);
            case FLOAT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
                        .named(name);
            case DOUBLE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                        .named(name);
            case STRING:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType())
                        .named(name);
            case BOOLEAN:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                        .named(name);
            default:
                throw new UnsupportedOperationException("unsupported basic type: " + type.type());
        }
    }

    private static GroupType createParquetGroupType(String name, com.sdu.data.parquet.type.Type type) {
        Type.Repetition repetition = type.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        switch (type.type()) {
            case LIST:
                com.sdu.data.parquet.type.ListType listType = (com.sdu.data.parquet.type.ListType) type;
                // NOTE: 需要标记<name>是否可空, 若不空时是重复元素, repetition只能标记一种状态, 故:
                // 1. <list-repetition> 标记是否可空
                // 2. repeated group list 标记是重复元素
                // <list-repetition> group <name> {
                //   repeated group list {
                //     <element-repetition> <element-type> element;
                //   }
                // }
                return Types.list(repetition)
                        .element(createParquetType(ARRAY_ELEMENT_NAME, listType.getElementType()))
                        .named(name);

            case MAP:
                com.sdu.data.parquet.type.MapType mapType = (com.sdu.data.parquet.type.MapType) type;
                // <map-repetition> group <name> {
                //    repeated group key_value {
                //       <key-repetition> <key-type> key_name;
                //       <value-repetition> <key-type> value_name;
                //    }
                // }
                return Types.map(repetition)
                        .key(createParquetType(MAP_KEY_NAME, mapType.getKeyType()))
                        .value(createParquetType(MAP_VALUE_NAME, mapType.getValueType()))
                        .named(name);

            case ROW:
                com.sdu.data.parquet.type.RowType rowType = (com.sdu.data.parquet.type.RowType) type;
                Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);
                for (int i = 0; i < rowType.getFieldCount(); ++i) {
                    builder.addField(createParquetType(rowType.getFieldName(i), rowType.getFieldType(i)));
                }
                return builder.named(name);
            default:
                throw new UnsupportedOperationException("unsupported complex type: " + type.type());
        }
    }
}
