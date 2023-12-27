package com.sdu.data.type;

public class BooleanType extends BasicType {

    public static final BooleanType BOOLEAN_TYPE = new BooleanType();

    private BooleanType() {
        super(TypeEnum.BOOLEAN);
    }
}
