package com.sdu.data.parquet.writer;

import static com.sdu.data.parquet.ParquetSchemas.ARRAY_ELEMENT_NAME;
import static com.sdu.data.parquet.ParquetSchemas.ARRAY_NAME;
import static com.sdu.data.parquet.ParquetSchemas.MAP_KEY_NAME;
import static com.sdu.data.parquet.ParquetSchemas.MAP_NAME;
import static com.sdu.data.parquet.ParquetSchemas.MAP_VALUE_NAME;

import java.io.Serializable;
import java.util.Map;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.sdu.data.parquet.RowData;
import com.sdu.data.type.ListType;
import com.sdu.data.type.MapType;
import com.sdu.data.type.RowType;
import com.sdu.data.type.Type;

public class RowDataParquetWriter {

    private final RecordConsumer recordConsumer;
    private final FieldWriter[] fieldWriters;

    public RowDataParquetWriter(RecordConsumer recordConsumer, RowType rowType) {
        this.recordConsumer = recordConsumer;
        this.fieldWriters = buildRowFieldWriter(rowType);
    }

    public void writeRow(RowData rowData) {
        recordConsumer.startMessage();
        for (int i = 0; i < rowData.getFieldCount(); ++i) {
            Object fieldValue = rowData.getFieldValue(i);
            String fieldName = rowData.getFieldName(i);
            recordConsumer.startField(fieldName, i);
            fieldWriters[i].write(recordConsumer, fieldValue);
            recordConsumer.endField(fieldName, i);
        }
        recordConsumer.endMessage();
    }

    public interface FieldWriter extends Serializable {

        void write(RecordConsumer consumer, Object value);

    }

    private static FieldWriter[] buildRowFieldWriter(RowType rowType) {
        FieldWriter[] fieldWriters = new FieldWriter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            fieldWriters[i] = buildFieldWriter(rowType.getFieldType(i));
        }
        return fieldWriters;
    }

    private static FieldWriter buildFieldWriter(final Type type) {
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
                // <list-repetition> group <name> {
                //   repeated group list {
                //     <element-repetition> <element-type> element;
                //   }
                // }
                ListType listType = (ListType) type;
                FieldWriter elementWriter = buildFieldWriter(listType.getElementType());
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
            case MAP:
                // <map-repetition> group <name> {
                //    repeated group key_value {
                //       <key-repetition> <key-type> key_name;
                //       <value-repetition> <key-type> value_name;
                //    }
                // }
                MapType mapType = (MapType) type;
                FieldWriter keyWriter = buildFieldWriter(mapType.getKeyType());
                FieldWriter valueWriter = buildFieldWriter(mapType.getValueType());
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
            case ROW:
                RowType rowType = (RowType) type;
                FieldWriter[] fieldWriters = buildRowFieldWriter(rowType);
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
