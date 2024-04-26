package com.sdu.data.flink.changelog;

import static java.lang.String.format;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDataInputStream extends FSDataInputStream {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDataInputStream.class);

    private static final String JOB_NAME = "job.name";

    // --------------------------------------------------------
    //  consume
    // --------------------------------------------------------
    private final String operatorID;
    private final KafkaConsumer<byte[], byte[]> client;
    private final long startOffset;
    private final long endOffset;
    private final TopicPartition topicPartition;
    private final int bufferSize;

    private volatile long consumeOffset;
    private KafkaStreamReader reader;
    private CircularBuffer buffer;

    public KafkaDataInputStream(
            String operatorID,
            Properties consumerProps,
            String topic,
            int partition,
            long startOffset,
            long endOffset,
            int bufferSize) {
        this.operatorID = operatorID;
        this.client = new KafkaConsumer<>(wrapWithGroupId(topic, operatorID, consumerProps));
        this.topicPartition = new TopicPartition(topic, partition);
        this.startOffset = startOffset;
        this.consumeOffset = startOffset;
        this.endOffset = endOffset;
        this.bufferSize = bufferSize;
        this.client.assign(Collections.singleton(this.topicPartition));
    }

    @Override
    public void seek(long desired) throws IOException {
        Preconditions.checkArgument(desired >= startOffset && desired <= endOffset);
        Preconditions.checkArgument(reader == null, "consume offset can be set only once");
        this.consumeOffset = desired;
        client.seek(topicPartition, desired);
        buffer = new CircularBuffer(bufferSize);
        reader = new KafkaStreamReader(operatorID,client, buffer, endOffset);
        reader.start();
    }

    @Override
    public long getPos() throws IOException {
        return reader == null ? this.consumeOffset : reader.currentConsumeOffset();
    }

    @Override
    public int read() throws IOException {
        try {
            return buffer.read();
        } catch (Exception e) {
            throw new IOException("failed read stream state", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.interrupt();
        }
        client.close();
    }

    private static Properties wrapWithGroupId(String topic, String operatorID, Properties props) {
        String jobName = System.getProperty(JOB_NAME, "UNKNOWN");
        String groupId = format("state-change-log-reader.%s.%s.%s", jobName, operatorID, UUID.randomUUID());
        LOG.info("read state change log from topic '{}' with group.id '{}'", topic, groupId);
        props.put("group.id", groupId);
        return props;
    }

    private static class CircularBuffer {

        private final Object lock;
        private final byte[] buf;
        private final int size;
        private int head = 0;
        private int tail = 0;

        CircularBuffer(int size) {
            this.lock = new Object();
            this.size = size;
            this.buf = new byte[size];
            Arrays.fill(buf, 0, buf.length, (byte) -1);
        }

        public void write(byte[] bytes) throws Exception {
            synchronized (lock) {
                for (byte aByte : bytes) {
                    if (isFull()) {
                        lock.wait();
                    }
                    buf[tail] = aByte;
                    tail = (tail + 1) % size;
                    lock.notify();
                }
            }
        }

        public boolean isEmpty() {
            return head == tail;
        }

        public boolean isFull() {
            return (tail + 1) % size == head;
        }

        public int read() throws Exception {
            synchronized (lock) {
                if (isEmpty()) {
                    lock.wait();
                }
                int value = buf[head];
                // 标记结束
                buf[head] = -1;
                head = (head + 1) % size;
                lock.notify();
                return value;
            }
        }
    }

    private static class KafkaStreamReader extends Thread {

        private final String operatorID;
        private final KafkaConsumer<byte[], byte[]> client;
        private final CircularBuffer buffer;
        private final long endOffset;

        private long currentOffset = 0;

        KafkaStreamReader(
                String operatorID,
                KafkaConsumer<byte[], byte[]> client,
                CircularBuffer buffer,
                long endOffset) {
            this.operatorID = operatorID;
            this.client = client;
            this.buffer = buffer;
            this.endOffset = endOffset;
        }

        @Override
        public void run() {
            while (!interrupted() && currentOffset < endOffset) {
                try {
                    // todo: 超时 & 重试
                    ConsumerRecords<byte[], byte[]> records = client.poll(Duration.ofMinutes(2));
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        if (record.partition() > endOffset) {
                            continue;
                        }
                        String stateOperatorID = new String(record.key());
                        if (operatorID.equals(stateOperatorID)) {
                            currentOffset = record.offset();
                            byte[] values = record.value();
                            // 写满时阻塞当前线程
                            buffer.write(values);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("failed fetch stream state from kafka", e);
                }
            }
        }

        long currentConsumeOffset() {
            return currentOffset;
        }
    }
}
