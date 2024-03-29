package com.sdu.data.hbase.threads;


import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public abstract class ScheduleChore implements Runnable {

    // ScheduledThreadPoolExecutor线程池周期性调度任务时, 当执行完调度周期内任务时, 根据任务状态判断是否将任务重新放回调度队列, 这说明:
    // 1. Future.cancel()取消任务, 任务状态被标记为INTERRUPTED, 任务不会再次被放入任务队列
    // 2. Runnable对象被重复使用, 但只会ScheduledThreadPoolExecutor线程池中单个线程访问
    // NOTE:
    // 相关事项可具体查看ScheduledFutureTask.run()方法

    private final String name;
    // 调度周期
    private final int period;
    // 调度周期时间单位
    private final TimeUnit timeUnit;
    // 初次执行延迟时间
    private final long initialDelay;
    // 可被其他线程取消调度任务
    private final Stoppable stoppable;

    private ChoreServices choreServices;

    // 最近执行时间
    private long timeOfLastRun = -1;
    // 本次执行时间
    private long timeOfThisRun = -1;

    public ScheduleChore(String name, int period, TimeUnit timeUnit, long initialDelay, Stoppable stoppable) {
        this.name = name;
        this.period = period;
        this.timeUnit = timeUnit;
        this.initialDelay = initialDelay;
        this.stoppable = stoppable;
    }

    public String getName() {
        return name;
    }

    public int getPeriod() {
        return period;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public long getInitialDelay() {
        return initialDelay;
    }

    public void setChoreServices(ChoreServices choreServices) {
        this.choreServices = choreServices;
    }

    public ChoreServices getChoreServices() {
        return choreServices;
    }

    @Override
    public void run() {
        updateTimeTrackingBeforeRun();
        String msg = format("Thread(%s) start schedule chore(%s) at %d", Thread.currentThread().getName(), getName(), timeOfThisRun);
        System.out.println(msg);
        if (missedStartTime() && isScheduling()) {
            // 核心线程不足够并发执行, 动态扩容核心线程数
            onChoreMissedStartTime();
        } else if (stoppable.isStopped() || !isScheduling()) {
            shutdown(false);
        } else {
            try {
                chore();
            } catch (Throwable e) {
                String error = format("Failed schedule chore(%s), error message: %s", getName(), e.getMessage());
                System.out.println(error);
                if (this.stoppable.isStopped()) {
                    cancel(false);
                }
            }
        }
    }

    public abstract void chore();
    public void cleanup() { };

    private synchronized void updateTimeTrackingBeforeRun() {
        this.timeOfLastRun = timeOfThisRun;
        this.timeOfThisRun = System.currentTimeMillis();
    }

    private synchronized boolean isScheduling() {
        return choreServices != null && choreServices.isChoreScheduled(this);
    }

    private synchronized void onChoreMissedStartTime() {
        if (choreServices != null) {
            choreServices.onChoreMissedStartTime(this);
        }
    }

    public synchronized void shutdown() {
        shutdown(true);
    }

    private synchronized void shutdown(boolean mayInterruptIfRunning) {
        cancel(mayInterruptIfRunning);
        cleanup();
    }

    public synchronized void cancel(boolean mayInterruptIfRunning) {
        if (isScheduling()) {
            choreServices.cancelChore(this, mayInterruptIfRunning);
        }
        choreServices = null;
    }

    // 判断是否延迟执行
    private boolean missedStartTime() {
        if (isValidTimestamp(timeOfLastRun) || isValidTimestamp(timeOfThisRun)) {
            return false;
        }
        long timeIntervals = timeOfThisRun - timeOfLastRun;
        // 允许一定幅度
        return 1.5 * timeUnit.toMillis(period) < timeIntervals;
    }


    private static boolean isValidTimestamp(long timestamp) {
        return timestamp <= 0 || timestamp > System.currentTimeMillis();
    }


}
