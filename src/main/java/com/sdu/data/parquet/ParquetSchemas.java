package com.sdu.data.parquet;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;
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

            default:
                throw new UnsupportedOperationException("unsupported complex type: " + type.type());
        }
    }
}
