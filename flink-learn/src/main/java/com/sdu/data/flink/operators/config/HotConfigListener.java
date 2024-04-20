package com.sdu.data.flink.operators.config;

import java.io.Serializable;

public interface HotConfigListener extends Serializable {

    void notifyConfig(String oldConfig, String newConfig);

    void notifyExceptionWhenDetectConfig(Throwable cause);

}
