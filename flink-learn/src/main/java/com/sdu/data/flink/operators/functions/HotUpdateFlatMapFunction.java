package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;

public interface HotUpdateFlatMapFunction<IN, OUT> extends FlatMapFunction<IN, OUT>, HotUpdateFunction {
}
