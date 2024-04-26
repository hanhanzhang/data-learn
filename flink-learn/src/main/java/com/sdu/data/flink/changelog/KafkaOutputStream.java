package com.sdu.data.flink.changelog;

import java.io.IOException;
import java.io.OutputStream;

public class KafkaOutputStream extends OutputStream {

    @Override
    public void write(int b) throws IOException {
        throw new IOException("unsupported");
    }

}
