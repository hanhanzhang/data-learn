package com.sdu.data.parquet;

import com.sdu.data.type.RowType;
import com.sdu.data.type.Type;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import java.io.Serializable;

public class RowDataParquetWriter {

    private final RecordConsumer recordConsumer;
    private final FieldWriter[] fieldWriters;

    public RowDataParquetWriter(RecordConsumer recordConsumer, RowType rowType) {
        this.recordConsumer = recordConsumer;
        this.fieldWriters = buildRowFieldWriter(rowType, recordConsumer);
    }

    public void writeRow(RowData rowData) {
        recordConsumer.startMessage();
        for (int i = 0; i < rowData.getFieldCount(); ++i) {
            Object fieldValue = rowData.getFieldValue(i);
            String fieldName = rowData.getFieldName(i);
            //
            recordConsumer.startField(fieldName, i);
            fieldWriters[i].write(fieldValue);
            recordConsumer.endField(fieldName, i);
        }
        recordConsumer.endMessage();
    }

    public interface FieldWriter extends Serializable {

        void write(Object value);

    }

    private static FieldWriter[] buildRowFieldWriter(RowType rowType, RecordConsumer consumer) {
        FieldWriter[] fieldWriters = new FieldWriter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            fieldWriters[i] = buildFieldWriter(rowType.getFieldType(i), consumer);
        }
        return fieldWriters;
    }

    private static FieldWriter buildFieldWriter(Type type, RecordConsumer consumer) {
        switch (type.type()) {
            case INT:
                return value -> consumer.addInteger((int) value);
            case FLOAT:
                return value -> consumer.addFloat((float) value);
            case DOUBLE:
                return value -> consumer.addDouble((double) value);
            case STRING:
                return value -> consumer.addBinary(Binary.fromString((String) value));
            case BOOLEAN:
                return value -> consumer.addBoolean((boolean) value);
            case LIST:

            case MAP:

            default:
                throw new UnsupportedOperationException("unsupported type: " + type.type());
        }
    }
}
