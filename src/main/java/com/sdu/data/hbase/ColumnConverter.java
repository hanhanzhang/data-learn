package com.sdu.data.hbase;

import java.io.Serializable;

import org.apache.hadoop.hbase.util.Bytes;

public interface ColumnConverter<RESULT> extends Serializable {

    RESULT convert(byte[] rowKey, byte[] family, byte[] qualifier, byte[] value) throws Exception;


    ColumnConverter<HBaseRowData<String, String, String, String>> SIMPLE_COLUMN_CONVERTER =
            (rowKey, family, qualifier, value) -> HBaseRowData.of(
                    Bytes.toString(rowKey),
                    Bytes.toString(family),
                    Bytes.toString(qualifier),
                    Bytes.toString(value));

}
