package com.sdu.data.flink.changelog;

import java.io.IOException;

public interface KafkaStateChangeUploader extends AutoCloseable {

    UploadResult upload() throws IOException;

    final class UploadResult {



    }

}
