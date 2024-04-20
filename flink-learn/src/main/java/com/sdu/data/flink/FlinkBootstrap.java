package com.sdu.data.flink;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sdu.data.common.JsonUtils;
import com.sdu.data.flink.operators.DataStreams;
import com.sdu.data.flink.operators.config.HotConfigDescriptor;
import com.sdu.data.flink.operators.config.HotConfigType;
import com.sdu.data.flink.operators.functions.HotUpdateRichMapFunction;

public class FlinkBootstrap {

    public static class WordWeightHotConfigDescriptor implements HotConfigDescriptor {

        @Override
        public String subscribeTopic() {
            return "/Users/hanhan.zhang/Downloads/words.txt";
        }

        @Override
        public HotConfigType configType() {
            return HotConfigType.FILE;
        }

        @Override
        public long updateTimeoutMills() {
            return 2 * 60 * 1000L;
        }

    }

    public static class WordSourceFunction implements ParallelSourceFunction<String> {

        private boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random random = new Random();
            while (running) {
                ctx.collect(createRandomChineseCharacters(random));
                TimeUnit.MILLISECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }

        private static String createRandomChineseCharacters(Random random) {
            // Unicode范围：4e00-9fa5 是中文字符的范围
            int unicodeStart = 0x4e00;
            int unicodeEnd = 0x9fa5;
            int code =  unicodeStart + random.nextInt(unicodeEnd - unicodeStart + 1);
            return new String(Character.toChars(code));
        }

    }

    public static class WordCounterFunction extends HotUpdateRichMapFunction<String, Tuple2<String, Integer>> {

        private static final Logger LOG = LoggerFactory.getLogger(WordCounterFunction.class);

        private static final String STATE_NAME = "@word-counter@";

        private transient Map<String, Integer> wordWeights;
        private Object lock;
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
            lock = new Object();
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
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();

        // checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(3 * 60 * 1000L);
        checkpointConfig.setCheckpointTimeout(5 * 60 * 1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //
        DataStream<String> sourceStream = env
                .addSource(new WordSourceFunction())
                .name("word_source")
                .setParallelism(2);

        // 计数
//        KeyedStream<String, String> wordCountStream = sourceStream.keyBy(x -> x);
        DataStreams
                .mapWithHotConfigDescriptor(sourceStream, new WordWeightHotConfigDescriptor(), new WordCounterFunction())
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
