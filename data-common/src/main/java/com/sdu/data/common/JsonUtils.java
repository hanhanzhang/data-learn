package com.sdu.data.common;

import java.util.ServiceLoader;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

@SuppressWarnings({"rawtypes", "unchecked"})
public class JsonUtils {

    private static final ObjectMapper MAPPER;

    static {
        ServiceLoader<SerializerInfo> loader = ServiceLoader.load(SerializerInfo.class);
        SimpleModule module = new SimpleModule();
        for (SerializerInfo serializerInfo : loader) {
            if (serializerInfo instanceof JsonSerializer) {
                module.addSerializer(serializerInfo.serializerClass(), (JsonSerializer) serializerInfo);
            }
        }
        MAPPER = new ObjectMapper();
        MAPPER.registerModule(module);
    }

    public static String toJson(Object obj) throws Exception {
        return MAPPER.writeValueAsString(obj);
    }

}
