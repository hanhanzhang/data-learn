package com.sdu.data.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FlinkBootstrap {

    public static class WordSourceFunction implements SourceFunction<String> {

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

    public static class WordCounterFunction extends RichMapFunction<String, Tuple2<String, Integer>> {

        private static final String STATE_NAME = "@word-counter@";
        private static final String TIMESTAMP_STATE_NAME = "@timestamp-word-counter@";

        private transient ValueState<Integer> wordCounter;

        private transient MapState<Long, Integer> timestampToCounter;

        @Override
        public void open(OpenContext openContext) throws Exception {
            StateTtlConfig ttlCfg = StateTtlConfig
                    .newBuilder(Duration.of(14, ChronoUnit.HOURS))   // 存活时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 永远不返回过期的用户数据
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 每次写操作创建和更新时,修改上次访问时间戳
                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) // 目前只支持ProcessingTime
                    .build();
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(STATE_NAME, Types.INT);
            descriptor.enableTimeToLive(ttlCfg);
            wordCounter = getRuntimeContext().getState(descriptor);

            MapStateDescriptor<Long, Integer> descriptor1 = new MapStateDescriptor<>(TIMESTAMP_STATE_NAME, Types.LONG, Types.INT);
            descriptor1.enableTimeToLive(ttlCfg);
            timestampToCounter = getRuntimeContext().getMapState(descriptor1);
        }

        @Override
        public Tuple2<String, Integer> map(String value) throws Exception {
            Integer cnt = wordCounter.value();
            int newCount = cnt == null ? 1 : cnt + 1;
            wordCounter.update(newCount);
            timestampToCounter.put(System.currentTimeMillis(), newCount);
            return Tuple2.of(value, newCount);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(3 * 60 * 1000L);
        checkpointConfig.setCheckpointTimeout(5 * 60 * 1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //
        DataStream<String> sourceStream = env.addSource(new WordSourceFunction()).name("word_source");
        // 计数
        DataStream<String> wordCountStream = sourceStream
                .keyBy(x -> x)
                .map(new WordCounterFunction()).setParallelism(1).name("word_counter")
                .map(t -> String.format("字符: %s, 当前计数: %d", t.f0, t.f1));
        // 打印
        wordCountStream.addSink(new PrintSinkFunction<>());

        env.execute("word_counter_bootstrap");
    }

}
