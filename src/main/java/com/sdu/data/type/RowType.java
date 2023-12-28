package com.sdu.data.type;

import com.google.common.base.Preconditions;

public class RowType extends ComplexType {

    private final String[] filedNames;
    private final Type[] fieldTypes;

    public RowType(boolean nullable, String[] filedNames, Type[] fieldTypes) {
        super(nullable, LogicalType.ROW);
        Preconditions.checkNotNull(filedNames);
        Preconditions.checkNotNull(fieldTypes);
        Preconditions.checkArgument(fieldTypes.length == filedNames.length);

        this.filedNames = filedNames;
        this.fieldTypes = fieldTypes;
    }

    public String getFieldName(int index) {
        Preconditions.checkArgument(index >= 0 && index < filedNames.length);
        return filedNames[index];
    }

    public Type getFieldType(int index) {
        Preconditions.checkArgument(index >= 0 && index < fieldTypes.length);
        return fieldTypes[index];
    }

    public int getFieldCount() {
        return filedNames.length;
    }

}
