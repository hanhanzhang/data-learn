package com.sdu.data.hbase.threads;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ChoreServices {

    private final ScheduledThreadPoolExecutor scheduler;

    private final Map<ScheduleChore, ScheduledFuture<?>> scheduledChores;
    // 若定时任务未能在调度周期被调度, 则需扩容调度线程数, 此变量记录为ScheduledChore扩容核心线程是否成功
    private final HashMap<ScheduleChore, Boolean> choresMissingStartTime;

    public ChoreServices(final String coreThreadPoolPrefix, int corePoolSize, boolean removedOnCancelTask) {
        scheduler = new ScheduledThreadPoolExecutor(corePoolSize, new ChoreServiceThreadFactory(coreThreadPoolPrefix));
        // removedOnCancelTask = true: 调度任务取消时则将调度任务从任务队列中剔除, 防止再次被调度
        scheduler.setRemoveOnCancelPolicy(removedOnCancelTask);
        scheduledChores = new HashMap<>();
        choresMissingStartTime = new HashMap<>();
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

    // 同一个定时任务可能同时被不同被定时线程池中不同线程访问
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

    public synchronized void cancelChore(ScheduleChore chore) {
        cancelChore(chore, true);
    }

    public synchronized void cancelChore(ScheduleChore chore, boolean mayInterruptIfRunning) {
        if (chore == null || !scheduledChores.containsKey(chore)) {
            return;
        }
        ScheduledFuture<?> scheduledFuture = scheduledChores.remove(chore);
        scheduledFuture.cancel(mayInterruptIfRunning);
        if (choresMissingStartTime.containsKey(chore)) {
            choresMissingStartTime.remove(chore);
            requestCorePoolDecrease();
        }
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
