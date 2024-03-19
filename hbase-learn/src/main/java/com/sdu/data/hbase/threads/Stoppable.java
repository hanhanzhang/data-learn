package com.sdu.data.hbase.threads;

public interface Stoppable {

    void stop(String why);

    boolean isStopped();

}
