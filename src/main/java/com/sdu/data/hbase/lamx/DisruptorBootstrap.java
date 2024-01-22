package com.sdu.data.hbase.lamx;

import static com.sdu.data.hbase.lamx.Event.EventType.Start;
import static com.sdu.data.hbase.lamx.Event.EventType.Stop;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorBootstrap {

    private final Disruptor<EventChunk> disruptor;

    @SafeVarargs
    public DisruptorBootstrap(EventFactory<EventChunk> eventFactory, EventHandler<EventChunk> ... handlers) {
        this.disruptor = new Disruptor<>(eventFactory, 128,
                new ThreadFactoryBuilder().setNameFormat("disruptor-thread-%d").setDaemon(false).build(),
                ProducerType.MULTI, new BlockingWaitStrategy());
        this.disruptor.setDefaultExceptionHandler(new PrintExceptionHandler());
        // EventHandler单线程按序消费, 每个EventHandler分配一个线程
        this.disruptor.handleEventsWith(handlers);
        this.disruptor.start();
    }

    public void publishEvent(Event event) {
        long sequencer = this.disruptor.getRingBuffer().next();
        try {
            event.setSequenceId(sequencer);
            EventChunk eventChunk = this.disruptor.getRingBuffer().get(sequencer);
            eventChunk.hold(event);
        } finally {
            this.disruptor.getRingBuffer().publish(sequencer);
        }
    }

    public void close() {
        this.disruptor.shutdown();
    }


    private static class PrintExceptionHandler implements ExceptionHandler<EventChunk> {

        @Override
        public void handleEventException(Throwable ex, long sequence, EventChunk event) {
            System.out.println("failed process event, event:" + event.getEvent() + ", msg: " + ex.toString());
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            System.out.println("failed start disruptor, msg: " + ex.toString());
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            System.out.println("failed shutdown disruptor, msg: " + ex.toString());
        }

    }

    private static class PrintEventHandler implements EventHandler<EventChunk> {

        private long consumerSequence = -1L;

        @Override
        public void onEvent(EventChunk event, long sequence, boolean endOfBatch) throws Exception {
            Event e = event.getEvent();
            System.out.println("process event: " + e + ", thread: " + Thread.currentThread().getName() + ", endOfBatch " + endOfBatch);
            consumerSequence += 1;
            // 顺序消费
            if (consumerSequence != e.sequenceId()) {
                throw new RuntimeException("failed consume sequence");
            }
        }
    }


    private static class EventProducer extends Thread {

        private final DisruptorBootstrap disruptorBootstrap;
        private final long eventNumbers;
        private final Event.EventType eventType;

        public EventProducer(long eventNumbers, DisruptorBootstrap disruptorBootstrap, Event.EventType eventType) {
            this.eventNumbers = eventNumbers;
            this.disruptorBootstrap = disruptorBootstrap;
            this.eventType = eventType;
        }

        @Override
        public void run() {
            for (int i = 0; i < eventNumbers; i++) {
                disruptorBootstrap.publishEvent(Event.EventImpl.of(eventType, System.currentTimeMillis()));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        DisruptorBootstrap disruptorBootstrap = new DisruptorBootstrap(EventChunk::new, new PrintEventHandler());

        // 两个生产者
        EventProducer startProducer = new EventProducer(2048, disruptorBootstrap, Start);
        EventProducer stopProducer = new EventProducer(1024, disruptorBootstrap, Stop);
        startProducer.start();
        stopProducer.start();

        startProducer.join();
        stopProducer.join();

        disruptorBootstrap.close();
    }

}

