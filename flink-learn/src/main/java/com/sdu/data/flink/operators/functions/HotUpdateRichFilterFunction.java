package com.sdu.data.flink.operators.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.FilterFunction;

public abstract class HotUpdateRichFilterFunction <T> extends AbstractRichFunction
        implements FilterFunction<T> {

    @Override
    public abstract boolean filter(T value) throws Exception;

}
