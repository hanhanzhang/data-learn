package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public abstract class HotUpdateRichSinkFunction<IN> extends AbstractRichFunction implements SinkFunction<IN> {

}
