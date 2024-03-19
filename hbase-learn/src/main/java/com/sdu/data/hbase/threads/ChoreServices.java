package com.sdu.data.hbase.threads;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.RestrictedApi;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static java.lang.String.format;

public class ChoreServices {

    private final ScheduledThreadPoolExecutor scheduler;

    private final Map<ScheduleChore, ScheduledFuture<?>> scheduledChores;
    // 若定时任务未能在调度周期被调度, 则需扩容调度线程数, 此变量记录为ScheduledChore扩容核心线程是否成功
    private final HashMap<ScheduleChore, Boolean> choresMissingStartTime;

    public ChoreServices(final String coreThreadPoolPrefix, int corePoolSize) {
        this.scheduler = new ScheduledThreadPoolExecutor(corePoolSize, new ChoreServiceThreadFactory(coreThreadPoolPrefix));
        // 若removedOnCancelTask = true时:
        // 1. Future.cancel则会立即将任务从任务队列中剔除, 这只是针对未被调度执行的任务, 若已调度执行的任务已经不在任务队列了
        // 2. Future.cancel()将任务状态标记为INTERRUPTED, 任务不会再次被放入任务队列
        // NOTE:
        // 具体可查看ScheduledFutureTask.cancel()方法
        this.scheduler.setRemoveOnCancelPolicy(true);
        this.scheduledChores = new HashMap<>();
        this.choresMissingStartTime = new HashMap<>();
    }

    // 可被任意线程添加任务
    public boolean scheduleChore(ScheduleChore chore) {
        if (chore == null) {
            return false;
        }

        synchronized (this) {
            if (chore.getChoreServices() == this) {
                return false;
            }
            if (chore.getPeriod() <= 0) {
                String msg = format("Chore(%s) is disabled because its period is not positive.", chore.getName());
                System.err.println(msg);
                return false;
            }
            if (chore.getChoreServices() != null) {
                String msg = format("Cancel chore(%s) from its previous service", chore.getName());
                System.out.println(msg);
                chore.getChoreServices().cancelChore(chore);
            }
            chore.setChoreServices(this);
            ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(chore, chore.getInitialDelay(), chore.getPeriod(), chore.getTimeUnit());
            scheduledChores.put(chore, scheduledFuture);
            return true;
        }
    }

    public synchronized boolean isChoreScheduled(ScheduleChore chore) {
        return chore != null && scheduledChores.containsKey(chore) && !scheduledChores.get(chore).isDone();
    }

    public synchronized void onChoreMissedStartTime(ScheduleChore chore) {
        if (!scheduledChores.containsKey(chore)) {
            return;
        }

        if (!choresMissingStartTime.containsKey(chore) || !choresMissingStartTime.get(chore)) {
            choresMissingStartTime.put(chore, requestCorePoolIncrease());
        }

        rescheduleChore(chore);
    }

    @RestrictedApi(explanation = "Should only be called in ScheduleChore", link = "",
            allowedOnPath = ".*/com/sdu/data/hbase/threads/(ScheduleChore|ChoreServices).java")
    synchronized void cancelChore(ScheduleChore chore) {
        cancelChore(chore, true);
    }

    @RestrictedApi(explanation = "Should only be called in ScheduleChore", link = "",
            allowedOnPath = ".*/com/sdu/data/hbase/threads/(ScheduleChore|ChoreServices).java")
    synchronized void cancelChore(ScheduleChore chore, boolean mayInterruptIfRunning) {
        if (chore == null || !scheduledChores.containsKey(chore)) {
            return;
        }
        ScheduledFuture<?> scheduledFuture = scheduledChores.remove(chore);
        // Java线程中断是一种协作机制, 调用Future.interrupt()不一定中断正在运行的任务, 只是将线程标记为interrupted
        // NOTE:
        // 1. 线程池提交的任务被封装成ScheduledFutureTask, Future.cancel() -> ScheduledFutureTask.cancel() -> Thread.interrupt()
        // 2. 如果任务需感知线程被中断状态, 则需通过Thread.isInterrupted()判断再做任务逻辑处理
        boolean suc = scheduledFuture.cancel(mayInterruptIfRunning);
        String msg = format("Chore(%s) cancelled %s", chore.getName(), suc ? "success" : "failure");
        System.out.println(msg);
        if (choresMissingStartTime.containsKey(chore)) {
            choresMissingStartTime.remove(chore);
            requestCorePoolDecrease();
        }
    }

    @VisibleForTesting
    public synchronized void cancelChoreForTest(ScheduleChore chore, boolean mayInterruptIfRunning) {
        cancelChore(chore, mayInterruptIfRunning);
    }

    private void rescheduleChore(ScheduleChore chore) {
        if (scheduledChores.containsKey(chore)) {
            ScheduledFuture<?> future = scheduledChores.get(chore);
            future.cancel(false);
        }
        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(chore, chore.getInitialDelay(),
                chore.getPeriod(), chore.getTimeUnit());
        scheduledChores.put(chore, future);
    }

    // 扩容线程, 新增1个线程, 保证每个ScheduleChore分配一个线程执行
    private synchronized boolean requestCorePoolIncrease() {
        if (scheduler.getCorePoolSize() < scheduledChores.size()) {
            scheduler.setCorePoolSize(scheduler.getCorePoolSize() + 1);
            return true;
        }
        return false;
    }

    // 缩容线程, 减少1个线程
    public synchronized void requestCorePoolDecrease() {
        int coreSize = scheduler.getCorePoolSize();
        if (coreSize == 1) {
            return;
        }
        scheduler.setCorePoolSize(coreSize - 1);
    }
}
