package com.sdu.data.flink.operators;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import com.sdu.data.flink.operators.functions.HotUpdateFlatMapFunction;

// @See StreamFlatMap
public class HotUpdateStreamFlatMapOperator<IN, OUT>
        extends HotUpdateBaseStreamOperator<OUT, HotUpdateFlatMapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private transient TimestampedCollector<OUT> collector;

    public HotUpdateStreamFlatMapOperator(HotUpdateFlatMapFunction<IN, OUT> userFunction) {
        super(userFunction);
        collector = new TimestampedCollector<>(output);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.flatMap(element.getValue(), collector);
    }
}
