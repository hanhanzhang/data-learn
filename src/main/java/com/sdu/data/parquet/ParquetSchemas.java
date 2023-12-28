package com.sdu.data.parquet;

import com.sdu.data.type.RowType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.List;

public class ParquetSchemas {

    public static final String SDU_LAKE = "sdu-lake";

    private ParquetSchemas() { }

    public static MessageType convertParquetSchema(RowType rowType) {
        List<Type> types = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            com.sdu.data.type.Type type = rowType.getFieldType(i);
            if (type.isPrimary()) {

            } else {

            }
        }
        return new MessageType(SDU_LAKE, types);
    }

}
