package com.sdu.data.parquet;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.sdu.data.type.RowType;

public class RowData implements Serializable {

    private final RowType rowType;
    private final Object[] values;

    public RowData(RowType rowType, int fieldCount) {
        this.rowType = rowType;
        this.values = new Object[fieldCount];
    }

    public RowData(RowType rowType, Object[] values) {
        Preconditions.checkNotNull(rowType);
        Preconditions.checkNotNull(values);
        Preconditions.checkArgument(values.length == rowType.getFieldCount());
        this.rowType = rowType;
        this.values = values;
    }

    public int getFieldCount() {
        return rowType.getFieldCount();
    }

    public Object getFieldValue(int index) {
        Preconditions.checkArgument(index >= 0 && index < values.length);
        return values[index];
    }

    public void setFieldValue(int index, Object value) {
        Preconditions.checkArgument(index >= 0 && index < values.length);
        values[index] = value;
    }

    public String getFieldName(int index) {
        return rowType.getFieldName(index);
    }

    public RowType getRowType() {
        return rowType;
    }

    public List<Object> toList() {
        return Arrays.stream(values).toList();
    }
}
