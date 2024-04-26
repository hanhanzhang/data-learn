package com.sdu.data.flink.changelog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

@PublicEvolving
public class KafkaStateChangelogOptions {

    public static final ConfigOption<String> KAFKA_CHANGE_LOG_STATE_TOPIC =
            ConfigOptions.key("kafka.state-change-log.topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Topic record state change log.");

    public static final ConfigOption<MemorySize> KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY =
            ConfigOptions.key("kafka.state-change-log.consume.buffer.capacity")
                    .memoryType()
                    .defaultValue(MemorySize.parse("10MB"))
                    .withDescription("Buffer size used when cache kafka consume record");

}
