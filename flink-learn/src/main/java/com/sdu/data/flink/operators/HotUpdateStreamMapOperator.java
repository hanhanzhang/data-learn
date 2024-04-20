package com.sdu.data.flink.operators;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.sdu.data.flink.operators.functions.HotUpdateMapFunction;

// @See StreamMap
public class HotUpdateStreamMapOperator<IN, OUT>
        extends HotUpdateBaseStreamOperator<OUT, HotUpdateMapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    public HotUpdateStreamMapOperator(HotUpdateMapFunction<IN, OUT> userFunction) {
        super(userFunction);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        output.collect(element.replace(userFunction.map(element.getValue())));
    }


}
