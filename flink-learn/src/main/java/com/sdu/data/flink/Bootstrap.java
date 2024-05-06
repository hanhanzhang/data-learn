package com.sdu.data.flink;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public interface Bootstrap {

    class WordSourceFunction implements ParallelSourceFunction<String> {

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

    static StreamExecutionEnvironment getDefaultStreamExecutionEnv() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        // checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(3 * 60 * 1000L);
        checkpointConfig.setCheckpointTimeout(5 * 60 * 1000L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        return env;
    }

    static SingleOutputStreamOperator<String> wordSourceStream(StreamExecutionEnvironment env, int parallelism) {
        return env
                .addSource(new WordSourceFunction())
                .name("word_source")
                .setParallelism(parallelism);
    }

}
