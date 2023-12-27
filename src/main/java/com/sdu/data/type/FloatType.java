package com.sdu.data.type;

public class FloatType extends BasicType {

    public static final FloatType FLOAT_TYPE = new FloatType();

    private FloatType() {
        super(TypeEnum.FLOAT);
    }
}
