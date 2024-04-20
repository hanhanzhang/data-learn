package com.sdu.data.flink.operators.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava31.com.google.common.io.Files;
import org.apache.flink.util.Preconditions;

public class HotConfigFileDetector implements HotConfigDetector {

    public static final HotConfigFileDetector INSTANCE = new HotConfigFileDetector();

    private transient ScheduledExecutorService timer;

    private final Object registerLock = new Object();

    private Map<String, List<HotConfigListener>> fileListeners;
    private ConcurrentMap<String, FileMonitorInfo> fileMonitorMessages;

    private HotConfigFileDetector() { }

    @Override
    public void open() {
        timer = Executors.newSingleThreadScheduledExecutor();
        fileListeners = new HashMap<>();
        fileMonitorMessages = new ConcurrentHashMap<>();
    }

    @Override
    public String register(HotConfigDescriptor descriptor, HotConfigListener listener) {
        Tuple2<Boolean, FileMonitorInfo> ret = readFile(descriptor.subscribeTopic());
        synchronized (registerLock) {
            List<HotConfigListener> listeners =
                    fileListeners.computeIfAbsent(descriptor.subscribeTopic(), k -> new LinkedList<>());
            listeners.add(listener);
            timer.scheduleAtFixedRate(new FileMonitorTask(descriptor.subscribeTopic()), 0, 10,  TimeUnit.SECONDS);
        }
        return ret.f1.getCurrentData();
    }

    @Override
    public void unregister(String subscribeTopic, HotConfigListener listener) {

    }

    private Tuple2<Boolean, FileMonitorInfo> readFile(String path) {
        try {
            File file = new File(path);
            long lastModified = file.lastModified();

            FileMonitorInfo fileMonitorInfo = fileMonitorMessages.get(path);
            if (fileMonitorInfo != null && fileMonitorInfo.getMonitorModifiedTimestamp() == lastModified) {
                return Tuple2.of(false, fileMonitorInfo);
            }

            String content = Files.asCharSource(file, UTF_8).read();
            fileMonitorInfo = new FileMonitorInfo(
                    fileMonitorInfo != null ? fileMonitorInfo.getCurrentData() : null,
                    content,
                    lastModified
            );
            fileMonitorMessages.put(path, fileMonitorInfo);
            return Tuple2.of(true, fileMonitorInfo);
        } catch (Exception e) {
            throw new RuntimeException("failed read file", e);
        }
    }

    private static final class FileMonitorInfo {

        private final String previousData;
        private final String currentData;
        private final long monitorModifiedTimestamp;

        FileMonitorInfo(String previousData, String currentData, long monitorModifiedTimestamp) {
            this.previousData = previousData;
            this.currentData = currentData;
            this.monitorModifiedTimestamp = monitorModifiedTimestamp;
        }

        public String getPreviousData() {
            return previousData;
        }

        public String getCurrentData() {
            return currentData;
        }

        public long getMonitorModifiedTimestamp() {
            return monitorModifiedTimestamp;
        }
    }

    private final class FileMonitorTask implements Runnable {

        private final String path;

        FileMonitorTask(String path) {
            this.path = path;
        }

        @Override
        public void run() {
            Tuple2<Boolean, FileMonitorInfo> ret = readFile(path);
            if (ret.f0) {
                List<HotConfigListener> listeners = fileListeners.get(path);
                Preconditions.checkArgument(listeners != null && !listeners.isEmpty());
                for (HotConfigListener listener : listeners) {
                    listener.notifyConfig(ret.f1.getPreviousData(), ret.f1.getCurrentData());
                }
            }
        }

    }

}
