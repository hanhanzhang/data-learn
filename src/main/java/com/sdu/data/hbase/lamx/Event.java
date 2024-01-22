package com.sdu.data.hbase.lamx;

import static java.lang.String.format;

public interface Event {

    void setSequenceId(long sequenceId);

    long sequenceId();

    long timestamp();

    EventType type();

    enum EventType {

        Start, Stop

    }

    class EventImpl implements Event {

        private final long timestamp;
        private final EventType type;
        private long sequenceId;

        private EventImpl(EventType type, long timestamp) {
            this.type = type;
            this.timestamp = timestamp;
        }

        @Override
        public void setSequenceId(long sequenceId) {
            this.sequenceId = sequenceId;
        }

        @Override
        public long sequenceId() {
            return sequenceId;
        }

        @Override
        public long timestamp() {
            return timestamp;
        }

        @Override
        public EventType type() {
            return type;
        }

        @Override
        public String toString() {
            return format("[type = %s, sequence = %s, timestamp = %s]", type(), sequenceId(), timestamp());
        }

        public static EventImpl of(EventType eventType, long timestamp) {
            return new EventImpl(eventType, timestamp);
        }
    }
}
