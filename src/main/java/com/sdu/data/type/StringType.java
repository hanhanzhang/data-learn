package com.sdu.data.type;

public class StringType extends BasicType {

    public static final StringType STRING_TYPE = new StringType();

    private StringType() {
        super(TypeEnum.STRING);
    }
}
