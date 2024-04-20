package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;

public abstract class HotUpdateRichMapFunction<IN, OUT> extends AbstractRichFunction implements HotUpdateMapFunction<IN, OUT> {

    @Override
    public abstract OUT map(IN value) throws Exception;

}
