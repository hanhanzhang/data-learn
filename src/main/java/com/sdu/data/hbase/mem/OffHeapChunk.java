package com.sdu.data.hbase.mem;

import java.nio.ByteBuffer;

public class OffHeapChunk extends Chunk {

    public OffHeapChunk(int id, int size) {
        super(id, size);
    }

    @Override
    public void allocateByteBuffer() {
        this.data = ByteBuffer.allocateDirect(this.size);
        this.data.putInt(0, this.id);
    }
}
