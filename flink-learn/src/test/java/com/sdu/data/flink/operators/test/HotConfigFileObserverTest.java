package com.sdu.data.flink.operators.test;

import static java.lang.String.format;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.sdu.data.flink.operators.config.HotConfigListener;
import com.sdu.data.flink.operators.config.HotConfigManager;
import com.sdu.data.flink.operators.config.files.HotConfigFileDescriptor;

public class HotConfigFileObserverTest {

    @Test
    public void testRegisterFileObserver() throws Exception {
        String path = "/Users/hanhan.zhang/Downloads/words.txt";
        HotConfigFileDescriptor fileDescriptor = new HotConfigFileDescriptor(path, 1_1000, 50_0000L);
        String content = HotConfigManager.INSTANCE.register(fileDescriptor, new HotConfigFileListener());
        String msg = format("register file listener, path: %s, content: %s", path, content);
        System.out.println(msg);
        TimeUnit.SECONDS.sleep(10 * 60L);
    }

    @Test
    public void testRemoveFileObserver() throws Exception {
        String path = "/Users/hanhan.zhang/Downloads/words.txt";
        HotConfigFileDescriptor fileDescriptor = new HotConfigFileDescriptor(path, 1_1000, 50_0000L);
        HotConfigFileListener listener =  new HotConfigFileListener();
        String content = HotConfigManager.INSTANCE.register(fileDescriptor, listener);
        String msg = format("register file listener, path: %s, content: %s", path, content);
        System.out.println(msg);

        HotConfigManager.INSTANCE.unregister(fileDescriptor, listener);
        TimeUnit.SECONDS.sleep(10 * 60L);
    }

    private static class HotConfigFileListener implements HotConfigListener {

        private final String uid;

        public HotConfigFileListener() {
            this.uid = UUID.randomUUID().toString();
        }

        @Override
        public void notifyConfig(String oldConfig, String newConfig) {
            String msg = format("uid: %s, notify config changed, old: %s, new: %s", uid, oldConfig, newConfig);
            System.out.println(msg);
        }

        @Override
        public void notifyExceptionWhenDetectConfig(Throwable cause) {
            // ignore
        }

    }
}
