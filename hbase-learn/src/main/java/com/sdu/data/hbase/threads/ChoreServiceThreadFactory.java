package com.sdu.data.hbase.threads;

import static java.lang.String.format;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ChoreServiceThreadFactory implements ThreadFactory {

    private final static String THREAD_NAME_SUFFIX = ".chore.";
    private final String threadPrefix;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public ChoreServiceThreadFactory(String threadPrefix) {
        this.threadPrefix = threadPrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        // 线程有两种类型: 用户线程(daemon = false)和守护线程(daemon = true)
        // 当JVM中所有的线程都是守护线程的时候, JVM就可以退出了; 如果还有一个或以上的非守护线程则JVM不会退出
        Thread thread = new Thread(r, format("%s%s%d", threadPrefix, THREAD_NAME_SUFFIX, threadNumber.getAndIncrement()));
        thread.setDaemon(true);
        return thread;
    }

    public static void main(String[] args) {

    }
}
