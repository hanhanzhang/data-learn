package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;

public abstract class HotUpdateRichFlatMapFunction <IN, OUT> extends AbstractRichFunction implements HotUpdateFlatMapFunction<IN, OUT> {

    @Override
    public abstract void flatMap(IN value, Collector<OUT> out) throws Exception;

}

