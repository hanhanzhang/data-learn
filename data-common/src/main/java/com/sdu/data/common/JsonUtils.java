package com.sdu.data.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
public class JsonUtils {

    private static final ObjectMapper MAPPER;

    static {
        SimpleModule module = new SimpleModule();
//        module.addSerializer(RegionInfo.class, RegionInfoSerializer.INSTANCE);
        MAPPER = new ObjectMapper();
        MAPPER.registerModule(module);
    }

    public static String toJson(Object obj) throws Exception {
        return MAPPER.writeValueAsString(obj);
    }

}
