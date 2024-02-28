package com.sdu.data.hbase;

import java.io.Serializable;

public class HBaseRowData<RowKey, COLUMN_FAMILY, COLUMN_QUALIFIER, QUALIFIER_VALUE> implements Serializable {

    private final RowKey key;
    private final COLUMN_FAMILY family;
    private final COLUMN_QUALIFIER qualifier;
    private final QUALIFIER_VALUE value;

    private HBaseRowData(RowKey key, COLUMN_FAMILY family, COLUMN_QUALIFIER qualifier, QUALIFIER_VALUE value) {
        this.key = key;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    public RowKey getKey() {
        return key;
    }

    public COLUMN_FAMILY getFamily() {
        return family;
    }

    public COLUMN_QUALIFIER getQualifier() {
        return qualifier;
    }

    public QUALIFIER_VALUE getValue() {
        return value;
    }

    public static <RowKey, COLUMN_FAMILY, COLUMN_QUALIFIER, QUALIFIER_VALUE> HBaseRowData<RowKey, COLUMN_FAMILY, COLUMN_QUALIFIER, QUALIFIER_VALUE> of(RowKey key, COLUMN_FAMILY family, COLUMN_QUALIFIER qualifier, QUALIFIER_VALUE value) {
        return new HBaseRowData<>(key, family, qualifier, value);
    }
}
