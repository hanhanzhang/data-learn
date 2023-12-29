package com.sdu.data.parquet.reader;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.sdu.data.parquet.RowData;

public class RowDataRecordMaterializer extends RecordMaterializer<RowData> {

    private final RowDataConvert root;

    public RowDataRecordMaterializer(MessageType schema) {
        this.root = new RowDataConvert(schema);
    }

    @Override
    public RowData getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }

}
