package com.sdu.data.hbase.test;

import com.sdu.data.hbase.threads.ChoreServices;
import com.sdu.data.hbase.threads.ScheduleChore;
import com.sdu.data.hbase.threads.Stoppable;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class ChoreServiceTest {

    private final String choreThreadPrefix = "test";

    private static class DefaultStoppable implements Stoppable {

        private String reason;
        private boolean stopped = false;

        @Override
        public void stop(String why) {
            this.reason = why;
            this.stopped = true;
        }

        @Override
        public boolean isStopped() {
            return stopped;
        }
    }

    private static class ScheduleChoreTestTask extends ScheduleChore {

        private final int sleepTimes;
        private final boolean printable;
        private final TimeUnit timeUnit;
        private final AtomicInteger invoked = new AtomicInteger(0);

        public ScheduleChoreTestTask(boolean printable, TimeUnit timeUnit, int sleepTimes, int periodSeconds) {
            super("test", periodSeconds, TimeUnit.SECONDS, 0, new DefaultStoppable());
            this.printable = printable;
            this.timeUnit = timeUnit;
            this.sleepTimes = sleepTimes;
        }

        @Override
        public void chore() {
            try {
                invoked.incrementAndGet();
                long startTimestamp = System.currentTimeMillis();
                if (printable) {
                    System.out.println("start execute chore: " + getName() + ", timestamp: " + System.currentTimeMillis());
                }
                long intervals = 0L;
                long timestamp = timeUnit.toMillis(sleepTimes);

                Set<Long> alreadyPrints = new HashSet<>();

                while ((intervals = (System.currentTimeMillis() - startTimestamp) - timestamp) <= 0) {
                    long waitSeconds = TimeUnit.SECONDS.convert(-intervals, TimeUnit.MILLISECONDS);
                    if (!alreadyPrints.contains(waitSeconds)) {
                        alreadyPrints.add(waitSeconds);
                        if (printable) {
                            String msg = format("chore should wait %d seconds, thread interrupt: %s", waitSeconds, Thread.currentThread().isInterrupted());
                            System.out.println(msg);
                        }
                    }
                }
                if (printable) {
                    System.out.println("finish execute chore: " + getName() + ", timestamp: " + System.currentTimeMillis());
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }

        public int getScheduledTimes() {
            return invoked.get();
        }
    }

    @Test
    public void testTaskCancel() throws Exception {
        ChoreServices choreServices = new ChoreServices(choreThreadPrefix, 1);
        // 构建任务
        ScheduleChoreTestTask task = new ScheduleChoreTestTask(true, TimeUnit.SECONDS, 10, 30);
        choreServices.scheduleChore(task);
        // 睡眠3秒, 取消任务
        TimeUnit.SECONDS.sleep(3);
        System.out.println("start cancel task, name: " + task.getName());
        choreServices.cancelChoreForTest(task, true);
        // 阻塞等待3分钟, 看任务是否再次被调度
        TimeUnit.SECONDS.sleep(120);
        Assert.assertEquals(1, task.getScheduledTimes());
        String msg = format("finished schedule, invoke times: %d", task.getScheduledTimes());
        System.out.println(msg);
    }

    @Test
    public void testConcurrentModifyRunnableObjectProperty() throws Exception {
        // 测试并发修改Runnable对象属性
        ChoreServices choreServices = new ChoreServices(choreThreadPrefix, 10);
        // 构建任务
        ScheduleChoreTestTask task = new ScheduleChoreTestTask(false, TimeUnit.SECONDS, 10, 5);
        choreServices.scheduleChore(task);
        TimeUnit.SECONDS.sleep(120);
    }

}
