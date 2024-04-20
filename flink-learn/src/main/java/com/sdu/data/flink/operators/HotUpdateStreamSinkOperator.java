package com.sdu.data.flink.operators;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import com.sdu.data.flink.operators.functions.HotUpdateSinkFunction;

// @see StreamSink
public class HotUpdateStreamSinkOperator<IN>
        extends HotUpdateBaseStreamOperator<Object, HotUpdateSinkFunction<IN>>
        implements OneInputStreamOperator<IN, Object> {

    private long currentWatermark = Long.MIN_VALUE;

    private transient SimpleContext<IN> sinkContext;

    public HotUpdateStreamSinkOperator(HotUpdateSinkFunction<IN> userFunction) {
        super(userFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.sinkContext = new SimpleContext<>(getProcessingTimeService());
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        sinkContext.element = element;
        userFunction.invoke(element.getValue(), sinkContext);
    }

    @Override
    protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
        // all operators are tracking latencies
        this.latencyStats.reportLatency(marker);

        // sinks don't forward latency markers
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        this.currentWatermark = mark.getTimestamp();
        userFunction.writeWatermark(
                new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
    }

    private class SimpleContext<IN> implements SinkFunction.Context {

        private StreamRecord<IN> element;

        private final ProcessingTimeService processingTimeService;

        public SimpleContext(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        @Override
        public long currentProcessingTime() {
            return processingTimeService.getCurrentProcessingTime();
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public Long timestamp() {
            if (element.hasTimestamp()) {
                return element.getTimestamp();
            }
            return null;
        }

    }
}
