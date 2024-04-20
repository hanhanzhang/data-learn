package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.MapFunction;

public interface HotUpdateMapFunction<IN, OUT> extends MapFunction<IN, OUT>, HotUpdateFunction {
}
