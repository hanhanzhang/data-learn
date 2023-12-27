package com.sdu.data.parquet;

import com.sdu.data.type.RowType;
import org.apache.parquet.schema.MessageType;

public class ParquetSchemas {

    private ParquetSchemas() { }

    public static MessageType convertParquetSchema(RowType rowType) {
        throw new RuntimeException();
    }

}
