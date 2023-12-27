package com.sdu.data.type;

public class DoubleType extends BasicType {

    public static final DoubleType DOUBLE_TYPE = new DoubleType();

    private DoubleType() {
        super(TypeEnum.DOUBLE);
    }
}
