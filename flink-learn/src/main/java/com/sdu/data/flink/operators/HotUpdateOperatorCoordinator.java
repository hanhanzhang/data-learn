package com.sdu.data.flink.operators;

import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdu.data.flink.operators.config.HotConfigDescriptor;
import com.sdu.data.flink.operators.config.HotConfigListener;
import com.sdu.data.flink.operators.config.HotConfigManager;
import com.sdu.data.flink.operators.event.HotUpdateAcknowledgeOperatorEvent;
import com.sdu.data.flink.operators.event.HotUpdateDataAttachOperatorEvent;

public class HotUpdateOperatorCoordinator implements OperatorCoordinator, HotConfigListener {

    private static final Logger LOG = LoggerFactory.getLogger(HotUpdateOperatorCoordinator.class);

    private final Context context;
    private final HotConfigDescriptor descriptor;

    private transient SubtaskGateways subtaskGateways;

    // 热配变更请求
    private transient LinkedBlockingQueue<HotUpdateDataAttachOperatorEvent> eventQueue;
    private transient OperatorEventSender eventSender;
    private transient AtomicReference<HotUpdateDataAttachOperatorEvent> lastOperatorEvent;

    // 热配变更响应
    private transient ConcurrentMap<Long, PendingOperatorEventRequest> pendingRequests;
    private transient ScheduledExecutorService timer;
    private transient ConcurrentMap<Long, ScheduledFuture<?>> timeoutFutures;

    public HotUpdateOperatorCoordinator(Context context, HotConfigDescriptor descriptor) {
        this.context = context;
        this.descriptor = descriptor;
    }

    @Override
    public void start() throws Exception {
        subtaskGateways = new SubtaskGateways(context.currentParallelism());

        timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("hot-config-update-timeout-thread-%d").build());
        timeoutFutures = new ConcurrentHashMap<>();
        pendingRequests = new ConcurrentHashMap<>();
        lastOperatorEvent = new AtomicReference<>(null);
        eventQueue = new LinkedBlockingQueue<>();
        eventSender = new OperatorEventSender(context.currentParallelism(), eventQueue, this::failJob);
        eventSender.start();

        // 注册配置
        String newConfig = HotConfigManager.INSTANCE.register(descriptor, this);
        eventQueue.put(new HotUpdateDataAttachOperatorEvent(newConfig));
    }

    @Override
    public void close() throws Exception {
        eventSender.close();
        timer.shutdownNow();
    }

    @Override
    public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) throws Exception {
        if (event instanceof HotUpdateAcknowledgeOperatorEvent) {
            HotUpdateAcknowledgeOperatorEvent acknowledgeOperatorEvent = (HotUpdateAcknowledgeOperatorEvent) event;
            if (acknowledgeOperatorEvent.getCause() != null) {
                failJob(acknowledgeOperatorEvent.getCause());
            } else {
                PendingOperatorEventRequest pendingRequest = pendingRequests.get(acknowledgeOperatorEvent.getEventId());
                boolean success = pendingRequest.markStatus(subtask, true);
                if (success) {
                    LOG.info("hot config update success, config: {}", pendingRequest.getOperatorEvent().getAttachData());
                    pendingRequests.remove(acknowledgeOperatorEvent.getEventId());
                    timeoutFutures.remove(acknowledgeOperatorEvent.getEventId()).cancel(true);
                }
            }
        }
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {
        resultFuture.complete(new byte[0]);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // Use the latest config on every startup, ignore
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData) throws Exception {
        // Use the latest config on every startup, ignore
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // recovery, need send latest config
        final HotUpdateDataAttachOperatorEvent operatorEvent = lastOperatorEvent.get();
        if (operatorEvent != null && eventQueue.isEmpty()) { // 若是eventQueue不空, 则可以按照队列中配置更新
            subtaskGateways.sendOperatorEvent(subtask, lastOperatorEvent.get());
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        LOG.trace("Task({}/{}) failed, attempt: {}", subtask, context.currentParallelism(), attemptNumber, reason);
        subtaskGateways.unregisterSubtaskGateway(subtask);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        LOG.info("Task({}/{}) ready, current attempt: {}", subtask, context.currentParallelism(), attemptNumber);
        subtaskGateways.registerSubtaskGateway(subtask, gateway);
    }

    // ----------------------------------------------------------------
    // hot config changed
    // ----------------------------------------------------------------

    @Override
    public void notifyConfig(String oldConfig, String newConfig) {
        try {
            eventQueue.put(new HotUpdateDataAttachOperatorEvent(newConfig));
        } catch (Exception e) {
            this.failJob(new RuntimeException("failed buffer hot config changed event."));
        }
    }

    @Override
    public void notifyExceptionWhenDetectConfig(Throwable cause) {
        // ignore
        // todo: alert
    }

    private void failJob(Throwable cause) {
        LOG.error("failed job", cause);
        context.failJob(cause);
    }

    private static interface ExceptionHandler {

        void occurException(Throwable cause);

    }

    private static final class PendingOperatorEventRequest {

        private final HotUpdateDataAttachOperatorEvent operatorEvent;
        private final Boolean[] statuses;

        PendingOperatorEventRequest(int parallelism, HotUpdateDataAttachOperatorEvent operatorEvent) {
            this.operatorEvent = operatorEvent;
            this.statuses = new Boolean[parallelism];
        }

        public HotUpdateDataAttachOperatorEvent getOperatorEvent() {
            return operatorEvent;
        }

        public boolean markStatus(int subtask, boolean status) {
            this.statuses[subtask] = status;
            return Arrays.stream(statuses).anyMatch(s -> s);
        }
    }

    private final class TimeoutTask implements Runnable {

        private final PendingOperatorEventRequest request;
        private final ExceptionHandler exceptionHandler;

        TimeoutTask(PendingOperatorEventRequest request, ExceptionHandler exceptionHandler) {
            this.request = request;
            this.exceptionHandler = exceptionHandler;
        }

        @Override
        public void run() {
            PendingOperatorEventRequest pendingRequest = pendingRequests.remove(request.getOperatorEvent().getEventId());
            if (pendingRequest == null) {
                LOG.info("operator event(id = {}) already finished, ignore timeout", request.getOperatorEvent().getEventId());
                return;
            }
            timeoutFutures.remove(request.getOperatorEvent().getEventId());
            exceptionHandler.occurException(new TimeoutException("config update timout"));
        }

    }

    private final class OperatorEventSender extends Thread {

        private volatile boolean running;
        private final int parallelism;
        private final Queue<HotUpdateDataAttachOperatorEvent> eventQueue;
        private final ExceptionHandler exceptionHandler;

        OperatorEventSender(int parallelism, Queue<HotUpdateDataAttachOperatorEvent> eventQueue, ExceptionHandler exceptionHandler) {
            this.running = true;
            this.parallelism = parallelism;
            this.eventQueue = eventQueue;
            this.exceptionHandler = exceptionHandler;
        }

        @Override
        public void run() {
            while (this.running) {
                try {
                    HotUpdateDataAttachOperatorEvent event = eventQueue.poll();
                    if (event == null) {
                        continue;
                    }
                    // 注册超时任务
                    PendingOperatorEventRequest request = new PendingOperatorEventRequest(parallelism, event);
                    pendingRequests.put(event.getEventId(), request);
                    ScheduledFuture<?> timeoutFuture = timer.schedule(
                            new TimeoutTask(request, exceptionHandler),
                            descriptor.updateTimeoutMills(),
                            TimeUnit.MILLISECONDS);
                    timeoutFutures.put(event.getEventId(), timeoutFuture);
                    // 发生变更请求
                    subtaskGateways.sendOperatorEvent(event);
                    lastOperatorEvent.set(event);
                } catch (Exception e) {
                    exceptionHandler.occurException(e);
                }
            }
        }

        public void close() throws Exception {
            this.running = false;
            this.join();
        }

    }

    private static final class SubtaskGateways {

        private final Object lock = new Object();
        private final int parallelism;
        private final SubtaskGateway[] gateways;

        SubtaskGateways(int parallelism) {
            Preconditions.checkArgument(parallelism > 0, "task parallelism should large than zero.");
            this.parallelism = parallelism;
            gateways = new SubtaskGateway[parallelism];
        }

        void registerSubtaskGateway(int subtask, SubtaskGateway gateway) {
            checkState(subtask);
            synchronized (lock) {
                gateways[subtask] = gateway;
                if (isReady()) {
                    lock.notify();
                }
            }
        }

        void unregisterSubtaskGateway(int subtask) {
            checkState(subtask);
            synchronized (lock) {
                gateways[subtask] = null;
            }
        }

        void sendOperatorEvent(OperatorEvent event) throws Exception {
            synchronized (lock) {
                if (!isReady()) {
                    lock.wait();
                }
                for (SubtaskGateway gateway : gateways) {
                    gateway.sendEvent(event);
                }
            }
        }

        void sendOperatorEvent(int subtask, OperatorEvent event) {
            checkState(subtask);
            synchronized (lock) {
                Preconditions.checkArgument(gateways[subtask] != null);
                gateways[subtask].sendEvent(event);
            }
        }

        boolean subtaskReady(int subtask) {
            return gateways[subtask] != null;
        }

        private void checkState(int subtask) {
            Preconditions.checkArgument(subtask >= 0 && subtask < parallelism);
        }

        private boolean isReady() {
            return Arrays.stream(gateways).allMatch(Objects::nonNull);
        }
    }
}
