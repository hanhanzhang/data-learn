package com.sdu.data.parquet.reader;

import com.sdu.data.parquet.RowData;
import com.sdu.data.type.LogicalType;
import com.sdu.data.type.RowType;

public class RowDataContainerImpl implements RowDataContainer {

    private final RowType rowType;
    private final ValueContainer[] fieldContainers;

    public RowDataContainerImpl(RowType rowType) {
        this.rowType = rowType;
        this.fieldContainers = new ValueContainer[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldContainers[i] = ValueContainers.createValueContainer(rowType.getFieldType(i));;
        }
    }

    @Override
    public ValueContainer getFieldValueContainer(int index) {
        return fieldContainers[index];
    }

    @Override
    public Object getValue() {
        Object[] fieldValues = new Object[fieldContainers.length];
        for (int i = 0; i < fieldValues.length; ++i) {
            fieldValues[i] = fieldContainers[i].getValue();
        }
        return new RowData(rowType, fieldValues);
    }

    @Override
    public void reset() {
        for (ValueContainer fieldValueContainer : fieldContainers) {
            fieldValueContainer.reset();
        }
    }

    @Override
    public LogicalType valueType() {
        return LogicalType.ROW;
    }

}
