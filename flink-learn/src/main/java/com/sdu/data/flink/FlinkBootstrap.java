package com.sdu.data.flink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sdu.data.common.JsonUtils;
import com.sdu.data.flink.operators.DataStreams;
import com.sdu.data.flink.operators.config.files.HotConfigFileDescriptor;
import com.sdu.data.flink.operators.functions.HotUpdateRichMapFunction;

public class FlinkBootstrap implements Bootstrap {


    public static class WordCounterFunction extends HotUpdateRichMapFunction<String, Tuple2<String, Integer>> {

        private static final Logger LOG = LoggerFactory.getLogger(WordCounterFunction.class);

//        private static final String STATE_NAME = "@word-counter@";

        private transient Map<String, Integer> wordWeights;
        private final Object lock = new Object();
        private int subtask;
        private int totalTasks;
        private long startTimestamp;

//        private transient ValueState<Integer> wordCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            totalTasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            startTimestamp = System.currentTimeMillis();

//            StateTtlConfig ttlCfg = StateTtlConfig
//                    .newBuilder(Duration.of(14, ChronoUnit.HOURS))   // 存活时间
//                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 永远不返回过期的用户数据
//                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 每次写操作创建和更新时,修改上次访问时间戳
//                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 目前只支持ProcessingTime
//                    .build();
//            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(STATE_NAME, Types.INT);
//            descriptor.enableTimeToLive(ttlCfg);
//            wordCounter = getRuntimeContext().getState(descriptor);

            wordWeights = new HashMap<>();
        }

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            if (subtask == 0) {
                // 运行3分钟挂掉, Region-Recovery, 观察OperatorCoordinator
                long endTimestamp = startTimestamp + 3 * 60 * 1000L;
                if (endTimestamp <= System.currentTimeMillis()) {
                    throw new RuntimeException("failed task");
                }
            }

            int weight = 1;
            synchronized (lock) {
                if (wordWeights == null) {
                    lock.wait();
                }
                weight = wordWeights.getOrDefault(value, 1);
            }
//            Integer cnt = wordCounter.value();
//            int newCount = cnt == null ? weight : cnt + weight;
//            wordCounter.update(newCount);
            return Tuple2.of(value, weight);
        }

        @Override
        public void configChanged(String config) throws Exception {
            Map<String, Integer> weights = JsonUtils.fromJson(config, new TypeReference<Map<String, Integer>>() {});
            synchronized (lock) {
                LOG.info("Task({}/{}) update word weights: {}", subtask + 1, totalTasks, config);
                this.wordWeights = weights;
                lock.notify();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Bootstrap.getDefaultStreamExecutionEnv();

        DataStream<String> sourceStream = Bootstrap.wordSourceStream(env, 2);

        // 计数
//        KeyedStream<String, String> wordCountStream = sourceStream.keyBy(x -> x);
        HotConfigFileDescriptor fileDescriptor = new HotConfigFileDescriptor(
                "/Users/hanhan.zhang/Downloads/words.txt",
                1_000L,
                2 * 60 * 1000L
        );
        DataStreams
                .mapWithHotConfigDescriptor(sourceStream, fileDescriptor, new WordCounterFunction())
                .name("word_counter")
                .setParallelism(2)
                .map(t -> String.format("字符: %s, 当前计数: %d", t.f0, t.f1))
                .name("word_format")
                .setParallelism(2)
                .addSink(new PrintSinkFunction<>())
                .setParallelism(2);


        env.execute("word_counter_bootstrap");
    }

}
