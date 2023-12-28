package com.sdu.data.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import com.sdu.data.type.BasicType;
import com.sdu.data.type.RowType;

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
            com.sdu.data.type.Type type = rowType.getFieldType(i);
            String fieldName = rowType.getFieldName(i);
            types.add(createParquetType(fieldName, type));
        }
        return new MessageType(ROW_MESSAGE, types);
    }

    public static RowType convertRowType(MessageType messageType) {
        int fieldCount = messageType.getFieldCount();
        String[] fieldNames = new String[fieldCount];
        com.sdu.data.type.Type[] fieldTypes = new com.sdu.data.type.Type[fieldCount];
        for (int i = 0; i < fieldCount; ++i) {
            Type type = messageType.getType(i);
            fieldNames[i] = type.getName();
            fieldTypes[i] = createType(type);
        }
        return new RowType(fieldNames, fieldTypes);
    }

    private static com.sdu.data.type.Type createType(Type type) {
        if (type.isPrimitive()) {
            return createBasicType(type.asPrimitiveType());
        }
        return createComplexType(type.asGroupType());
    }

    private static com.sdu.data.type.Type createBasicType(PrimitiveType type) {
        switch (type.getPrimitiveTypeName()) {
            case INT32:
                return com.sdu.data.type.Types.intType();
            case FLOAT:
                return com.sdu.data.type.Types.floatType();
            case DOUBLE:
                return com.sdu.data.type.Types.doubleType();
            case BINARY:
                return com.sdu.data.type.Types.stringType();
            case BOOLEAN:
                return com.sdu.data.type.Types.booleanType();
            default:
                throw new UnsupportedOperationException("unsupported primary parquet type: " + type.getPrimitiveTypeName());
        }
    }

    private static com.sdu.data.type.Type createComplexType(GroupType type) {
        GroupType childrenType = type.getType(0).asGroupType();
        switch (childrenType.getFieldCount()) {
            case 1: // LIST
                Type elementType = childrenType.getType(0);
                return com.sdu.data.type.Types.listType(createType(elementType));
            case 2: // MAP
                Type keyType = childrenType.getType(0);
                Type valueType = childrenType.getType(1);
                return com.sdu.data.type.Types.mapType(createType(keyType), createType(valueType));
            default:
                throw new UnsupportedOperationException("unsupported primary parquet type: " + type);
        }
    }

    private static Type createParquetType(String name, com.sdu.data.type.Type type) {
        if (type.isPrimary()) {
            return createParquetPrimaryType(name, (BasicType) type);
        }
        return createParquetGroupType(name, type);
    }

    private static PrimitiveType createParquetPrimaryType(String name, BasicType type) {
        switch (type.type()) {
            case INT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
                            .named(name);
            case FLOAT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.REQUIRED)
                        .named(name);
            case DOUBLE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.REQUIRED)
                        .named(name);
            case STRING:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
                        .as(LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType())
                        .named(name);
            case BOOLEAN:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.REQUIRED)
                        .named(name);
            default:
                throw new UnsupportedOperationException("unsupported basic type: " + type.type());
        }
    }

    private static GroupType createParquetGroupType(String name, com.sdu.data.type.Type type) {
        switch (type.type()) {
            // <list-repetition> group <name> {
            //   repeated group list {
            //     <element-repetition> <element-type> element;
            //   }
            // }
            case LIST:
                com.sdu.data.type.ListType listType = (com.sdu.data.type.ListType) type;
                return Types.list(Type.Repetition.OPTIONAL)
                        .element(createParquetType(ARRAY_ELEMENT_NAME, listType.getElementType()))
                        .named(name);

            // <map-repetition> group <name> {
            //    repeated group key_value {
            //       <key-repetition> <key-type> key_name;
            //       <value-repetition> <key-type> value_name;
            //    }
            // }
            case MAP:
                com.sdu.data.type.MapType mapType = (com.sdu.data.type.MapType) type;
                return Types.map(Type.Repetition.OPTIONAL)
                        .key(createParquetType(MAP_KEY_NAME, mapType.getKeyType()))
                        .value(createParquetType(MAP_VALUE_NAME, mapType.getValueType()))
                        .named(name);

            case ROW:
                com.sdu.data.type.RowType rowType = (com.sdu.data.type.RowType) type;
                Types.GroupBuilder<GroupType> builder = Types.buildGroup(Type.Repetition.OPTIONAL);
                for (int i = 0; i < rowType.getFieldCount(); ++i) {
                    builder.addField(createParquetType(rowType.getFieldName(i), rowType.getFieldType(i)));
                }
                return builder.named(name);
            default:
                throw new UnsupportedOperationException("unsupported complex type: " + type.type());
        }
    }
}
