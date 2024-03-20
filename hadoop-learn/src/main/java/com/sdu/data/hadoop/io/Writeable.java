package com.sdu.data.hadoop.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Writeable {

    void write(DataOutput out) throws IOException;

    void readFields(DataInput in) throws IOException;

}
