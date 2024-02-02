package com.sdu.data.hbase.threads;


import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public abstract class ScheduleChore implements Runnable {

    // 定时线程池队列中定时任务是复用, 故可能上一周定时调度未完成, 又开启新一轮任务调度, 这两轮调度在不同调度线程中, 故需保证线程安全

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

    // 最近执行时间(需保证线程安全, 这有可能上次调度未执行结束, 新一轮调度开始, 两次调度执行在不同线程中)
    private long timeOfLastRun = -1;
    // 本次执行时间(需保证线程安全, 这有可能上次调度未执行结束, 新一轮调度开始, 两次调度执行在不同线程中)
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
        msg = format("Thread(%s) finished schedule chore(%s) at %d", Thread.currentThread().getName(), getName(), timeOfThisRun);
        System.out.println(msg);
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
