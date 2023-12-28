package com.sdu.data.type;

import com.google.common.base.Preconditions;

public class RowType implements Type {

    private final String[] filedNames;
    private final Type[] fieldTypes;

    public RowType(String[] filedNames, Type[] fieldTypes) {
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

    @Override
    public boolean isPrimary() {
        return false;
    }

    @Override
    public TypeEnum type() {
        return TypeEnum.ROW;
    }
}
