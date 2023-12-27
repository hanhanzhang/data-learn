package com.sdu.data.type;

public class IntType extends BasicType {

    public static final IntType INT_TYPE = new IntType();

    private IntType() {
        super(TypeEnum.INT);
    }
}
