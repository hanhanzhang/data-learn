package com.sdu.data.parquet;

import com.google.common.base.Preconditions;
import com.sdu.data.type.RowType;

import java.io.Serializable;

public class RowData implements Serializable {

    private final RowType rowType;
    private final Object[] values;

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

    public String getFieldName(int index) {
        return rowType.getFieldName(index);
    }
}
