package com.sdu.data.parquet;

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.sdu.data.type.LogicalType;
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

    public boolean isNullAt(int index) {
        Preconditions.checkArgument(index >= 0 && index < values.length);
        return values[index] == null;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(getFieldName(i)).append(": ");
            sb.append(formatFieldValue(rowType.getFieldType(i).type(), getFieldValue(i)));
        }
        sb.append("}");
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static String formatFieldValue(LogicalType type, Object value) {
        switch (type) {
            case INT:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case BOOLEAN:
                return String.valueOf(value);
            case LIST:
                return String.format("[%s]", StringUtils.join(((Object[]) value), ','));
            case MAP:
                StringBuilder sb = new StringBuilder("[");
                int index = 0;
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) value).entrySet()) {
                    if (index != 0) {
                        sb.append(",");
                    }
                    sb.append("(").append(entry.getKey()).append(", ").append(entry.getValue()).append(")");
                    index++;
                }
                sb.append("]");
                return sb.toString();
            case ROW:
                return ((RowData) value).toString();
            default:
                throw new UnsupportedOperationException("unsupported type: " + type);
        }
    }
}
