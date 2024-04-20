package com.sdu.data.flink.operators;

import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.sdu.data.flink.operators.functions.HotUpdateFilterFunction;

// @See StreamFilter
public class HotUpdateStreamFilterOperator<IN>
        extends HotUpdateBaseStreamOperator<IN, HotUpdateFilterFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    public HotUpdateStreamFilterOperator(HotUpdateFilterFunction<IN> userFunction) {
        super(userFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
        }
    }
}
