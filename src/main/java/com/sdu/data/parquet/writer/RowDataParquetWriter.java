package com.sdu.data.parquet.writer;

import com.sdu.data.parquet.RowData;
import com.sdu.data.type.ListType;
import com.sdu.data.type.MapType;
import com.sdu.data.type.RowType;
import com.sdu.data.type.Type;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import java.io.Serializable;
import java.util.Map;

import static com.sdu.data.parquet.ParquetSchemas.*;

public class RowDataParquetWriter {

    private final RecordConsumer recordConsumer;
    private final FieldWriter[] fieldWriters;

    public RowDataParquetWriter(boolean standardSchema, RecordConsumer recordConsumer, RowType rowType) {
        this.recordConsumer = recordConsumer;
        this.fieldWriters = buildRowFieldWriter(standardSchema, rowType);
    }

    public void writeRow(RowData rowData) {
        recordConsumer.startMessage();
        for (int i = 0; i < rowData.getFieldCount(); ++i) {
            if (!rowData.isNullAt(i)) {
                Object fieldValue = rowData.getFieldValue(i);
                String fieldName = rowData.getFieldName(i);
                recordConsumer.startField(fieldName, i);
                fieldWriters[i].write(recordConsumer, fieldValue);
                recordConsumer.endField(fieldName, i);
            }
        }
        recordConsumer.endMessage();
    }

    public interface FieldWriter extends Serializable {

        void write(RecordConsumer consumer, Object value);

    }

    private static FieldWriter[] buildRowFieldWriter(boolean standardSchema, RowType rowType) {
        FieldWriter[] fieldWriters = new FieldWriter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            fieldWriters[i] = buildFieldWriter(standardSchema, rowType.getFieldType(i));
        }
        return fieldWriters;
    }

    private static FieldWriter buildFieldWriter(boolean standardSchema, final Type type) {
        switch (type.type()) {
            case INT:
                return (consumer, value) -> consumer.addInteger((Integer) value);
            case FLOAT:
                return (consumer, value) -> consumer.addFloat((float) value);
            case DOUBLE:
                return (consumer, value) -> consumer.addDouble((double) value);
            case STRING:
                return (consumer, value) -> consumer.addBinary(Binary.fromString((String) value));
            case BOOLEAN:
                return (consumer, value) -> consumer.addBoolean((boolean) value);
            case LIST:
                ListType listType = (ListType) type;
                FieldWriter elementWriter = buildFieldWriter(standardSchema, listType.getElementType());
                if (standardSchema) {
                    // <list-repetition> group <name> {
                    //   repeated group list {
                    //     <element-repetition> <element-type> element;
                    //   }
                    // }
                    return (consumer, value) -> {
                        consumer.startGroup();
                        consumer.startField(ARRAY_NAME, 0);
                        Object[] elements = (Object[]) value;
                        for (int i = 0; i < elements.length; ++i) {
                            consumer.startGroup();
                            consumer.startField(ARRAY_ELEMENT_NAME, 0);
                            elementWriter.write(consumer, elements[i]);
                            consumer.endField(ARRAY_ELEMENT_NAME, 0);
                            consumer.endGroup();
                        }
                        consumer.endField(ARRAY_NAME, 0);
                        consumer.endGroup();
                    };
                }
                // repeated group <name> {
                //   <element-repetition> <element-type> element;
                // }
                return (consumer, value) -> {
                    Object[] elements = (Object[]) value;
                    consumer.startGroup();
                    for (int i = 0; i < elements.length; ++i) {
                        consumer.startField(ARRAY_ELEMENT_NAME, 0);
                        elementWriter.write(consumer, elements[i]);
                        consumer.endField(ARRAY_ELEMENT_NAME, 0);
                    }
                    consumer.endGroup();
                };


            case MAP:
                MapType mapType = (MapType) type;
                FieldWriter keyWriter = buildFieldWriter(standardSchema, mapType.getKeyType());
                FieldWriter valueWriter = buildFieldWriter(standardSchema, mapType.getValueType());
                if (standardSchema) {
                    // <map-repetition> group <name> {
                    //    repeated group key_value {
                    //       <key-repetition> <key-type> key_name;
                    //       <value-repetition> <key-type> value_name;
                    //    }
                    // }
                    return (consumer, value) -> {
                        @SuppressWarnings("unchecked")
                        Map<Object, Object> valueMap = (Map<Object, Object>) value;
                        consumer.startGroup();
                        consumer.startField(MAP_NAME, 0);
                        for (Map.Entry<Object, Object> entry : valueMap.entrySet()) {
                            consumer.startGroup();

                            consumer.startField(MAP_KEY_NAME, 0);
                            keyWriter.write(consumer, entry.getKey());
                            consumer.endField(MAP_KEY_NAME, 0);

                            consumer.startField(MAP_VALUE_NAME, 1);
                            valueWriter.write(consumer, entry.getValue());
                            consumer.endField(MAP_VALUE_NAME, 1);

                            consumer.endGroup();
                        }
                        consumer.endField(MAP_NAME, 0);
                        consumer.endGroup();
                    };
                }
                // repeated group <name> {
                //   <key-repetition> <key-type> key_name;
                //   <value-repetition> <key-type> value_name;
                // }
                return (consumer, value) -> {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> valueMap = (Map<Object, Object>) value;
                    consumer.startGroup();
                    for (Map.Entry<Object, Object> entry : valueMap.entrySet()) {
                        consumer.startField(MAP_KEY_NAME, 0);
                        keyWriter.write(consumer, entry.getKey());
                        consumer.endField(MAP_KEY_NAME, 0);

                        consumer.startField(MAP_VALUE_NAME, 1);
                        valueWriter.write(consumer, entry.getValue());
                        consumer.endField(MAP_VALUE_NAME, 1);
                    }
                    consumer.endGroup();
                };



            case ROW:
                RowType rowType = (RowType) type;
                FieldWriter[] fieldWriters = buildRowFieldWriter(standardSchema, rowType);
                return (consumer, value) -> {
                    RowData rowData = (RowData) value;
                    for (int i = 0; i < rowType.getFieldCount(); ++i) {
                        consumer.startField(rowType.getFieldName(i), i);
                        fieldWriters[i].write(consumer, rowData.getFieldValue(i));
                        consumer.endField(rowType.getFieldName(i), i);
                    }
                };


            default:
                throw new UnsupportedOperationException("unsupported type: " + type.type());
        }
    }
}
