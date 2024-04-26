package com.sdu.data.flink.changelog;

import static com.sdu.data.flink.changelog.KafkaStateChangelogOptions.KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.configuration.MemorySize;

public class KafkaStreamStateHandleLocation implements Serializable {

    private final Properties props;
    private final String topic;
    private final int partition;
    private final long startOffset;
    private final long endOffset;

    public KafkaStreamStateHandleLocation(Properties props, String topic, int partition, long startOffset, long endOffset) {
        this.props = props;
        this.topic = topic;
        this.partition = partition;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    public Properties getProps() {
        return props;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    public int getBufferSize() {
        MemorySize size = (MemorySize) props.getOrDefault(
                KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY.key(),
                KAFKA_CHANGE_LOG_STATE_CONSUME_BUFFER_CAPACITY.defaultValue()
        );
        return (int) size.getBytes();
    }

}
