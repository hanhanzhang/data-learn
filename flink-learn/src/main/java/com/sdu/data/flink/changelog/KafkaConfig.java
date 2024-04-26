package com.sdu.data.flink.changelog;

import static com.sdu.data.flink.changelog.KafkaStateChangelogOptions.KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.configuration.ReadableConfig;

public class KafkaConfig implements Serializable {

    public static Properties fromConfiguration(ReadableConfig configuration) {
        Properties props = new Properties();

        props.put(
                KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY.key(),
                configuration.get(KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY));

        return props;
    }

}
