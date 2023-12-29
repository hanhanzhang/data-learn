package com.sdu.data.parquet.reader;

public interface RowDataContainer extends ValueContainer {

    ValueContainer getFieldValueContainer(int index);

}
