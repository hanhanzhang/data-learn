package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.Function;

public interface HotUpdateFunction extends Function {

    void configChanged(String config) throws Exception ;

}
