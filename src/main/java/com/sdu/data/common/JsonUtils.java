package com.sdu.data.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class JsonUtils {

    private static final Gson GSON;

    static {
        GsonBuilder builder = new GsonBuilder();
        GSON = builder.create();
    }

    public static String toJson(Object obj) {
        return GSON.toJson(obj);
    }

}
