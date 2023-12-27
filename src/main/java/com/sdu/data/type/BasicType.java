package com.sdu.data.type;

public class BasicType implements Type {

    private final TypeEnum type;

    public BasicType(TypeEnum type) {
        this.type = type;
    }

    @Override
    public boolean isPrimary() {
        return true;
    }

    @Override
    public TypeEnum type() {
        return type;
    }
}
