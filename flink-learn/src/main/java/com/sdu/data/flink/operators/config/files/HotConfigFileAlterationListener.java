package com.sdu.data.flink.operators.config.files;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;

import com.sdu.data.flink.operators.config.HotConfigListener;

@ThreadSafe
public class HotConfigFileAlterationListener extends FileAlterationListenerAdaptor {

    private final Object lock = new Object();
    private final Map<String, String> fileContents;
    private final Map<String, Integer> listenerNumbers;
    private final Map<String, List<HotConfigListener>> fileListeners;

    public HotConfigFileAlterationListener() {
        this.fileContents = new HashMap<>();
        this.fileListeners = new HashMap<>();
        this.listenerNumbers = new HashMap<>();
    }

    @Override
    public void onFileChange(File file) {
        synchronized (lock) {
            String path = file.getPath();
            List<HotConfigListener> listeners = fileListeners.get(path);
            if (listeners == null || listeners.isEmpty()) {
                return;
            }
            try {
                String data = HotConfigFileDescriptor.readConfig(path);
                String oldData = fileContents.put(path, data);
                listeners.forEach(listener -> listener.notifyConfig(oldData, data));
            } catch (Exception e) {
                listeners.forEach(listener -> listener.notifyExceptionWhenDetectConfig(e));
            }
        }
    }

    public String registerListener(String parent, String file, HotConfigListener listener) {
        synchronized (lock) {
            List<HotConfigListener> listeners = fileListeners.computeIfAbsent(file, new FileRegisterFunction());
            listeners.add(listener);
            // 引用计数
            listenerNumbers.compute(parent, (k, cnt) -> cnt == null ? 1 : cnt + 1);
            return fileContents.get(file);
        }
    }

    public void unregisterListener(String parent, String file, HotConfigListener listener, Runnable action) {
        synchronized (lock) {
            fileListeners.computeIfPresent(file, (k, listeners) -> {
                listeners.remove(listener);
                return listeners.isEmpty() ? null : listeners;
            });
            Integer ret = listenerNumbers.computeIfPresent(parent, (k, cnt) -> cnt > 1 ? cnt - 1 : null);
            if (ret == null) {
                action.run();
            }
        }
    }

    private class FileRegisterFunction implements Function<String, List<HotConfigListener>> {
        @Override
        public List<HotConfigListener> apply(String file) {
            String content = HotConfigFileDescriptor.readConfig(file);
            fileContents.put(file, content);
            return new ArrayList<>();
        }

    }
}
