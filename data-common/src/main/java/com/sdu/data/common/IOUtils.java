package com.sdu.data.common;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;

public class IOUtils {

    private IOUtils() { }


    public static void closeStream(Closeable stream) {
        if (stream != null) {
            cleanupWithLogger(null, stream);
        }
    }

    public static void cleanupWithLogger(Logger logger, Closeable ... closeables) {
        for (Closeable c : closeables) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    if (logger != null) {
                        logger.error("exception in closing {}", c, e);
                    }
                }
            }
        }
    }
}
