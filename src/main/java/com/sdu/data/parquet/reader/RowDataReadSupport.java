package com.sdu.data.parquet.reader;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.sdu.data.parquet.RowData;

public class RowDataReadSupport extends ReadSupport<RowData> {

    @Override
    public RecordMaterializer<RowData> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return null;
    }

}
