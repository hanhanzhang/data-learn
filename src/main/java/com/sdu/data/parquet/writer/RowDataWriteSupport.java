package com.sdu.data.parquet.writer;

import com.sdu.data.parquet.ParquetSchemas;
import com.sdu.data.parquet.RowData;
import com.sdu.data.type.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;

public class RowDataWriteSupport extends WriteSupport<RowData> {

    private final RowType rowType;
    private final MessageType schema;
    private final boolean standardSchema;
    private RowDataParquetWriter writer;

    public RowDataWriteSupport(RowType rowType, boolean standardSchema) {
        this.rowType = rowType;
        this.standardSchema = standardSchema;
        this.schema = ParquetSchemas.convertParquetSchema(rowType, standardSchema);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.writer = new RowDataParquetWriter(standardSchema, recordConsumer, rowType);
    }

    @Override
    public void write(RowData record) {
        writer.writeRow(record);
    }
}