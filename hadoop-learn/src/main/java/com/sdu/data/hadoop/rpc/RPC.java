package com.sdu.data.hadoop.rpc;

public class RPC {

    public enum RpcKind {
        RPC_PROTOCOL_BUFFER((short) 3);

        public final short value;

        RpcKind(short value) {
            this.value = value;
        }
    }

}
