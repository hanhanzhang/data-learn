package com.sdu.data.flink.operators;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.shaded.curator5.com.google.common.collect.Sets;
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
    private transient Set<Integer> defaultTargetTasks;
    private transient LinkedBlockingQueue<HotUpdateDataRequest> requestQueue;
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
        defaultTargetTasks = Sets.newHashSet(
                IntStream.range(0, context.currentParallelism())
                        .boxed()
                        .collect(Collectors.toList())
        );
        timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("hot-config-update-timeout-thread-%d").build());
        timeoutFutures = new ConcurrentHashMap<>();
        pendingRequests = new ConcurrentHashMap<>();
        lastOperatorEvent = new AtomicReference<>(null);
        requestQueue = new LinkedBlockingQueue<>();
        eventSender = new OperatorEventSender(requestQueue, this::failJob);
        eventSender.start();

        // 注册配置
        String newConfig = HotConfigManager.INSTANCE.register(descriptor, this);
        requestQueue.put(new HotUpdateDataRequest(defaultTargetTasks, new HotUpdateDataAttachOperatorEvent(newConfig)));
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
                    LOG.info("Task({}) update config success, config: {}", pendingRequest.getTasks(), pendingRequest.getOperatorEvent().getAttachData());
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
        if (operatorEvent != null && requestQueue.isEmpty()) { // 若是eventQueue不空, 则可以按照队列中配置更新
            boolean success = requestQueue.offer(
                    new HotUpdateDataRequest(
                            Sets.newHashSet(subtask),
                            new HotUpdateDataAttachOperatorEvent(lastOperatorEvent.get().getAttachData()
                            )
                    ));
            if (!success) {
                this.failJob(new RuntimeException(format("Task(%d/%d) failed notify config", subtask + 1, context.currentParallelism())));
            }
        }
    }

    @Override
    public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
        LOG.info("Task({}/{}) failed, attempt: {}", subtask + 1, context.currentParallelism(), attemptNumber, reason);
        subtaskGateways.unregisterSubtaskGateway(subtask);
    }

    @Override
    public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
        LOG.info("Task({}/{}) ready, current attempt: {}", subtask + 1, context.currentParallelism(), attemptNumber);
        subtaskGateways.registerSubtaskGateway(subtask, gateway);
    }

    // ----------------------------------------------------------------
    // hot config changed
    // ----------------------------------------------------------------

    @Override
    public void notifyConfig(String oldConfig, String newConfig) {
        try {
            requestQueue.put(new HotUpdateDataRequest(defaultTargetTasks, new HotUpdateDataAttachOperatorEvent(newConfig)));
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

    private static final class HotUpdateDataRequest {

        private final long createTimestamp;
        private final Set<Integer> targetTasks;
        private final HotUpdateDataAttachOperatorEvent event;

        HotUpdateDataRequest(Set<Integer> targetTasks, HotUpdateDataAttachOperatorEvent event) {
            this.createTimestamp = System.currentTimeMillis();
            this.targetTasks = targetTasks;
            this.event = event;
        }

        public long getCreateTimestamp() {
            return createTimestamp;
        }

        public Set<Integer> getTargetTasks() {
            return targetTasks;
        }

        public HotUpdateDataAttachOperatorEvent getEvent() {
            return event;
        }

    }

    private static final class PendingOperatorEventRequest {

        private final HotUpdateDataAttachOperatorEvent operatorEvent;
        private final Set<Integer> tasks;
        private final Boolean[] statuses;

        PendingOperatorEventRequest(Set<Integer> tasks, HotUpdateDataAttachOperatorEvent operatorEvent) {
            this.tasks = tasks;
            this.operatorEvent = operatorEvent;
            this.statuses = new Boolean[tasks.size()];
        }

        public HotUpdateDataAttachOperatorEvent getOperatorEvent() {
            return operatorEvent;
        }

        public boolean markStatus(int subtask, boolean status) {
            this.statuses[subtask] = status;
            return Arrays.stream(statuses).allMatch(s -> s != null && s);
        }

        public String getTasks() {
            return StringUtils.join(tasks, ',');
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
        private final Queue<HotUpdateDataRequest> requestQueue;
        private final ExceptionHandler exceptionHandler;

        OperatorEventSender(Queue<HotUpdateDataRequest> requestQueue, ExceptionHandler exceptionHandler) {
            this.running = true;
            this.requestQueue = requestQueue;
            this.exceptionHandler = exceptionHandler;
        }

        @Override
        public void run() {
            while (this.running) {
                try {
                    HotUpdateDataRequest request = requestQueue.poll();
                    if (request == null) {
                        continue;
                    }
                    HotUpdateDataAttachOperatorEvent event = request.getEvent();
                    Set<Integer> targetTasks = request.getTargetTasks();
                    LOG.info("dispatch operator event, id: {}, attach: {}", event.getEventId(), event.getAttachData());
                    // 注册超时任务
                    PendingOperatorEventRequest pendingRequest = new PendingOperatorEventRequest(targetTasks, event);
                    pendingRequests.put(event.getEventId(), pendingRequest);
                    ScheduledFuture<?> timeoutFuture = timer.schedule(
                            new TimeoutTask(pendingRequest, exceptionHandler),
                            descriptor.updateTimeoutMills(),
                            TimeUnit.MILLISECONDS);
                    timeoutFutures.put(event.getEventId(), timeoutFuture);
                    // 发生变更请求
                    subtaskGateways.sendOperatorEvent(event, targetTasks);
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

        void sendOperatorEvent(OperatorEvent event, Set<Integer> targetTasks) throws Exception {
            synchronized (lock) {
                if (!isReady()) {
                    lock.wait();
                }
                for (int subtask : targetTasks) {
                    SubtaskGateway gateway = gateways[subtask];
                    Preconditions.checkArgument(gateway != null);
                    gateway.sendEvent(event);
                }
            }
        }

        private void checkState(int subtask) {
            Preconditions.checkArgument(subtask >= 0 && subtask < parallelism);
        }

        private boolean isReady() {
            return Arrays.stream(gateways).allMatch(Objects::nonNull);
        }
    }
}
