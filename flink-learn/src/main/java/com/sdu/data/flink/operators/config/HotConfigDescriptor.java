package com.sdu.data.flink.operators.config;

import java.io.Serializable;

public interface HotConfigDescriptor extends Serializable {

    HotConfigType configType();

    // 更新超时时间, 单位: ms
    long updateTimeoutMills();

}
