package com.sdu.data.hadoop.rpc;

import java.io.IOException;

public class RpcException extends IOException {

    public RpcException(String message) {
        super(message);
    }

    public RpcException(String message, Throwable cause) {
        super(message, cause);
    }
}
