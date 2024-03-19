package com.sdu.data.parquet.writer;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import com.sdu.data.parquet.ParquetSchemas;
import com.sdu.data.parquet.RowData;
import com.sdu.data.parquet.type.RowType;

public class RowDataWriteSupport extends WriteSupport<RowData> {

    private final RowType rowType;
    private final MessageType schema;
    private RowDataParquetWriter writer;

    public RowDataWriteSupport(RowType rowType) {
        this.rowType = rowType;
        this.schema = ParquetSchemas.convertParquetSchema(rowType);
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.writer = new RowDataParquetWriter(recordConsumer, rowType);
    }

    @Override
    public void write(RowData record) {
        writer.writeRow(record);
    }
}
