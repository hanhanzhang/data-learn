package com.sdu.data.flink.operators;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import com.sdu.data.flink.operators.config.HotConfigDescriptor;
import com.sdu.data.flink.operators.functions.HotUpdateFilterFunction;
import com.sdu.data.flink.operators.functions.HotUpdateFlatMapFunction;
import com.sdu.data.flink.operators.functions.HotUpdateMapFunction;

public class DataStreams {

    private DataStreams() { }

    public static <IN, OUT> SingleOutputStreamOperator<OUT> mapWithHotConfigDescriptor(DataStream<IN> input, HotConfigDescriptor descriptor, HotUpdateMapFunction<IN, OUT> function) {
        TypeInformation<OUT> outType =
                TypeExtractor.getMapReturnTypes(
                        input.getExecutionEnvironment().clean(function),
                        input.getType(),
                        Utils.getCallLocationName(),
                        true
                );
        return input.transform(
                "HotConfigMap",
                outType,
                HotUpdateOperatorFactory.of(descriptor, new HotUpdateStreamMapOperator<>(function))
        );
    }

    public static <IN, OUT> SingleOutputStreamOperator<OUT> flatMapWithHotConfigDescriptor(DataStream<IN> input, HotConfigDescriptor descriptor, HotUpdateFlatMapFunction<IN, OUT> function) {
        TypeInformation<OUT> outType =
                TypeExtractor.getFlatMapReturnTypes(
                        input.getExecutionEnvironment().clean(function),
                        input.getType(),
                        Utils.getCallLocationName(), true
                );
        return input.transform(
                "HotConfigFlatMap",
                outType,
                HotUpdateOperatorFactory.of(descriptor, new HotUpdateStreamFlatMapOperator<>(function))
        );
    }

    public static <IN> SingleOutputStreamOperator<IN> filterWithHotConfigDescriptor(DataStream<IN> input, HotConfigDescriptor descriptor, HotUpdateFilterFunction<IN> function) {
        return input.transform(
                "HotConfigFilter",
                input.getType(),
                HotUpdateOperatorFactory.of(descriptor, new HotUpdateStreamFilterOperator<>(function))
        );
    }

//    public static <IN> DataStreamSink<IN> sinkWithHotConfigDescriptor(DataStream<IN> input, HotConfigDescriptor descriptor, HotUpdateSinkFunction<IN> function) {
//        HotUpdateStreamSinkOperator<IN> sinkOperator = new HotUpdateStreamSinkOperator<>(function);
//        final StreamExecutionEnvironment executionEnvironment = input.getExecutionEnvironment();
//        PhysicalTransformation<IN> transformation =
//                new LegacySinkTransformation<>(
//                        input.getTransformation(),
//                        "Unnamed",
//                        HotUpdateOperatorFactory.of(descriptor, sinkOperator),
//                        executionEnvironment.getParallelism(),
//                        false);
//        executionEnvironment.addOperator(transformation);
//        return new DataStreamSink<>(transformation);
//    }

}
