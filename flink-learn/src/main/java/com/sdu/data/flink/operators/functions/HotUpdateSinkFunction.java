package com.sdu.data.flink.operators.functions;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public interface HotUpdateSinkFunction<IN> extends SinkFunction<IN>, HotUpdateFunction {
}
