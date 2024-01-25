package com.sdu.data.hbase.mem;

import java.nio.ByteBuffer;

public class OnHeapChunk extends Chunk {

    public OnHeapChunk(int id, int size) {
        super(id, size);
    }

    @Override
    public void allocateByteBuffer() {
        this.data = ByteBuffer.allocate(this.size);
        this.data.putInt(0, this.id);
    }
}
