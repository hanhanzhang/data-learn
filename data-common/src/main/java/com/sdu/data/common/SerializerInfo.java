package com.sdu.data.common;

import java.io.Serializable;

public interface SerializerInfo<T> extends Serializable {

    Class<T> serializerClass();

}
