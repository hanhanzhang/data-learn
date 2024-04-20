package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.FilterFunction;

public interface HotUpdateFilterFunction<IN> extends FilterFunction<IN>, HotUpdateFunction {
}
