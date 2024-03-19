package com.sdu.data.parquet.type;

public interface Type {

    boolean isPrimary();

    boolean isNullable();

    LogicalType type();

}
