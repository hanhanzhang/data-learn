package com.sdu.data.flink;

import com.sdu.data.common.JsonUtils;
import org.apache.flink.api.common.DynamicConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Dynamic variables bootstrap. */
public class FlinkDynamicVariablesBootstrap implements Bootstrap {

    public static class WordFilterWithDynamicVariablesFunction extends RichFilterFunction<String> {

        private static final Logger LOG =
                LoggerFactory.getLogger(WordFilterWithDynamicVariablesFunction.class);

        private static final String KEY = "filters";

        private transient long timestamp = 0;
        private transient int subtask;
        private transient DynamicConfig config;
        private transient Map<String, Set<String>> filterWords;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            this.config = getRuntimeContext().getDynamicConfig();
            this.filterWords = new HashMap<>();
            this.timestamp = System.currentTimeMillis();
            LOG.info(
                    "Task started, default dynamic variable : {} = {}",
                    KEY,
                    config.getDynamicVariable(KEY, ""));
        }

        @Override
        public boolean filter(String word) throws Exception {
            if (subtask == 0 && (System.currentTimeMillis() - timestamp) >= 2 * 60 * 1000L) {
                throw new RuntimeException("restart again.");
            }
            String value = config.getDynamicVariable(KEY, "");
            Set<String> filters =
                    getAndRemove(filterWords, value, config.getTotalDynamicVariables());
            return !filters.contains(word);
        }

        private static Set<String> getAndRemove(
                Map<String, Set<String>> filterWords, String value, Map<String, String> variables)
                throws Exception {
            Set<String> filters = filterWords.get(value);
            if (filters != null) {
                return filters;
            }
            filterWords.clear();
            LOG.info("receive dynamic variable, key: {}, value: {}", KEY, value);
            LOG.info("total dynamic variables: {}", JsonUtils.toJson(variables));
            filters = Sets.newHashSet(value.split(","));
            filterWords.put(value, filters);
            return filters;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Bootstrap.getDefaultStreamExecutionEnv();

        DataStream<String> sourceStream = Bootstrap.wordSourceStream(env, 2);

        //
        sourceStream
                .filter(new WordFilterWithDynamicVariablesFunction())
                .name("filter_with_dynamic_variables")
                .setParallelism(2)
                .addSink(new PrintSinkFunction<>())
                .name("sink_console")
                .setParallelism(2);

        env.execute();
    }
}
