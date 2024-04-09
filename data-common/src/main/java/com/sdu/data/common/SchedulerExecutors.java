package com.sdu.data.common;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.sdu.data.common.DateUtils.formatDate;
import static com.sdu.data.common.DateUtils.sleep;
import static java.lang.String.format;

public class SchedulerExecutors {

    /*
     * 验证项
     * 1. 由于线程数少且定时任务执行慢, 导致定时任务未在规定时间内执行
     * 2. ScheduledExecutorService.execute()和ScheduledExecutorService.schedule()区别
     * */

    private static final ScheduledExecutorService ses;

    static {
        // 单线程, 用于验证定时任务未在规定时间内执行
        ses = Executors.newScheduledThreadPool(1);
    }

    private static class TaskDelegate implements Runnable {

        private final Runnable delegate;
        private final long submitTimestamp;

        TaskDelegate(Runnable delegate) {
            this.delegate = delegate;
            this.submitTimestamp = System.currentTimeMillis();
        }

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            delegate.run();
            String pattern = "yyyy-MM-dd HH:mm:ss";
            String msg = format("submit at %s, execute at %s, elapsed time %d ms", formatDate(submitTimestamp, pattern), formatDate(now, pattern), now - submitTimestamp);
            System.out.println(msg);
        }
    }

    public static void execute(Runnable action) {
        ses.execute(new TaskDelegate(action));
    }

    public static void schedule(Runnable action, long delay, TimeUnit timeUnit) {
        ses.schedule(new TaskDelegate(action), delay, timeUnit);
    }

    public static void main(String[] args) {
        // 提交两个任务, 每个任务延迟5秒执行, 第一个任务执行耗时10s, 预期第二个任务未在规定时间内执行
        Runnable action1 = () -> sleep(10, TimeUnit.SECONDS);
        Runnable action2 = () -> sleep(2, TimeUnit.SECONDS);
        schedule(action1, 5, TimeUnit.SECONDS);
        schedule(action2, 5, TimeUnit.SECONDS);
    }
}
