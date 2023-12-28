package com.sdu.data.type;

public interface Type {

    boolean isPrimary();

    boolean isNullable();

    LogicalType type();

}
