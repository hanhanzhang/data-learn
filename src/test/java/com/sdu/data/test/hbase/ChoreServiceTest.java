package com.sdu.data.test.hbase;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.sdu.data.hbase.threads.ChoreServices;
import com.sdu.data.hbase.threads.ScheduleChore;
import com.sdu.data.hbase.threads.Stoppable;

public class ChoreServiceTest {

    private final String choreThreadPrefix = "test";

    private static class DefaultStoppable implements Stoppable {

        private String reason;
        private boolean stopped = false;

        @Override
        public void stop(String why) {
            this.reason = reason;
            this.stopped = true;
        }

        @Override
        public boolean isStopped() {
            return stopped;
        }
    }

    private static class ScheduleChoreTestTask extends ScheduleChore {

        private final int sleepTimes;
        private final TimeUnit timeUnit;

        public ScheduleChoreTestTask(TimeUnit timeUnit, int sleepTimes) {
            super("test", 30, TimeUnit.SECONDS, 0, new DefaultStoppable());
            this.timeUnit = timeUnit;
            this.sleepTimes = sleepTimes;
        }

        @Override
        public void chore() {
            try {
                long startTimestamp = System.currentTimeMillis();
                System.out.println("start execute chore: " + getName() + ", timestamp: " + System.currentTimeMillis());
                long intervals = 0L;
                long timestamp = timeUnit.toMillis(sleepTimes);

                Set<Long> alreadyPrints = new HashSet<>();

                while ((intervals = (System.currentTimeMillis() - startTimestamp) - timestamp) <= 0) {
                    long waitSeconds = TimeUnit.SECONDS.convert(-intervals, TimeUnit.MILLISECONDS);
                    if (!alreadyPrints.contains(waitSeconds)) {
                        alreadyPrints.add(waitSeconds);
                        System.out.println("chore should wait " + waitSeconds + " second");
                    }
                }
                System.out.println("finish execute chore: " + getName() + ", timestamp: " + System.currentTimeMillis());
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testTaskCancelWithoutRemove() throws Exception {
        // 取消调度任务, 但不从任务队列中剔除可再次被调度
        ChoreServices choreServices = new ChoreServices(choreThreadPrefix, 1, false);
        // 构建任务
        ScheduleChoreTestTask task = new ScheduleChoreTestTask(TimeUnit.SECONDS, 10);
        choreServices.scheduleChore(task);
        // 睡眠3秒, 取消任务
        TimeUnit.SECONDS.sleep(3);
        System.out.println("start cancel task, name: " + task.getName());
        choreServices.cancelChore(task);
        // 阻塞等待2分钟, 看任务是否再次被调度
        TimeUnit.MINUTES.sleep(2);
    }

}
