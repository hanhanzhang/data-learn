package com.sdu.data.hadoop.rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sdu.data.common.IOUtils;
import com.sdu.data.hadoop.io.Writeable;

public class Client implements AutoCloseable {

    private static final AtomicInteger callIdCounter = new AtomicInteger();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ConcurrentMap<ConnectionId, Connection> connections = new ConcurrentHashMap<>();

    public Writeable call(RPC.RpcKind rpcKind, ConnectionId remoteId, Writeable rpcRequest) throws IOException {
        // STEP3: 获取结果
        Call call = new Call(rpcKind, rpcRequest);
        // STEP1: Connection是否连接, 若未连接则发起连接
        Connection connection = getConnection(rpcKind, remoteId, call);
        // STEP2: 向服务端发送请求
        connection.sendRpcCall(call);
        return getRpcResponse(call, -1, null);
    }

    private Writeable getRpcResponse(final Call call, final long timeout, final TimeUnit unit) throws IOException {
        // 同步阻塞
        synchronized (call) {
            while (!call.done) {
                try {
                    call.wait(unit.toMillis(timeout));
                    if (!call.done) {
                        return null;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("Call interrupted");
                }
            }
            if (call.error != null) {
                throw new IOException("call exception", call.error);
            } else {
                return call.rpcResponse;
            }
        }

    }

    private Connection getConnection(RPC.RpcKind rpcKind, ConnectionId remoteId, Call call) throws IOException {
        if (!running.get()) {
            throw new IOException("Client is stopped");
        }
        Connection connection;
        // 自旋直至连接成功
        while (true) {
            connection = connections.get(remoteId);
            if (connection == null) {
                // NOTE: 这里并发起对服务端连接操作, 主要是考虑到并发构建Connection实例, 造成不必要的连接
                connection = new Connection(remoteId);
                Connection existConn = connections.putIfAbsent(remoteId, connection);
                if (existConn != connection) {
                    connection = existConn;
                }
            }
            if (connection.addCall(call)) {
                break;
            } else { // 意味着Connection已被关闭, 进入下次循环重新连接
                connections.remove(remoteId);
            }
        }

        // 发起客户端连接
        connection.setupIOStreams();
        return connection;
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // 关闭连接
        for (Connection connection : connections.values()) {
            connection.interrupt();
            connection.interruptConnectingThread();
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    private static int nextCallId() {
        return callIdCounter.getAndIncrement();
    }

    // RPC调用线程向其添加RPC请求
    private class Connection extends Thread {

        // 连接线程
        private final AtomicReference<Thread> connectionThread = new AtomicReference<>();
        // Hashtable线程安全
        private final Hashtable<Integer, Call> calls = new Hashtable<>();
        // 连接信息
        private final int maxResponseLength;
        private Socket socket;
        private IpcStreams ipcStreams;
        private final AtomicBoolean shouldCloseConnection = new AtomicBoolean(false);
        private IOException closeException;


        Connection(ConnectionId connectionId) {
            this.maxResponseLength = connectionId.getMaxResponseLength();
        }

        private synchronized boolean addCall(Call call) {
            if (shouldCloseConnection.get()) {
                return false;
            }
            calls.put(call.id, call);
            // 唤醒Connection线程(若无RPC请求则Connection线程阻塞等待)
            notify();
            return true;
        }

        // RPC调用线程发起服务器连接(避免重复连接且需要串行访问)
        private synchronized void setupIOStreams() {
            if (socket != null || shouldCloseConnection.get()) {
                return;
            }
            try {
                connectionThread.set(Thread.currentThread());
                while (true) {
                    // STEP1: 连接服务端
                    setupConnection();
                    ipcStreams = new IpcStreams(socket, maxResponseLength);
                    // STEP2: 向服务端发送连接头信息
                    writeConnectionHeader(ipcStreams);
                    // STEP3: 启动Connection线程, 持续接受来自服务端的响应
                    this.start();
                    return;
                }
            } catch (Throwable t) {
                // execute close operation

            } finally {
                connectionThread.set(null);
            }
        }

        private synchronized void setupConnection() {
            // 连接服务端
        }

        private void writeConnectionHeader(IpcStreams streams) {
            // +---------+---------+---------------+---------------+
            // | 4 bytes | 1 byte  |     1 byte    |    1 byte     |
            // +---------+---------+---------------+---------------+
            // |   hrpc  | version | server class  | auth protocol |
            // +---------+---------+---------------+---------------+

        }

        //
        private void sendRpcCall(Call call) {

        }

        @Override
        public void run() {
            // 持续读取服务端的RPC响应, 读取一次服务端响应
        }

        private synchronized void markClosed(IOException e) {
            if (shouldCloseConnection.compareAndSet(false, true)) {
                this.closeException = e;
                notifyAll();
            }
        }

        public void interruptConnectingThread() {
            Thread connectThread = connectionThread.get();
            if (connectThread != null) {
                connectThread.interrupt();
            }
        }
    }

    public static class ConnectionId {

        private InetSocketAddress address;
        private int maxResponseLength;

        public int getMaxResponseLength() {
            return maxResponseLength;
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    // IpcStream封装Socket数据通道
    private class IpcStreams implements AutoCloseable, Flushable {

        private DataInputStream in;
        private DataOutputStream out;

        private int maxResponseLength;

        IpcStreams(Socket socket, int maxResponseLength) {

        }

        public ByteBuffer readResponse() throws IOException {
            // TCP粘包问题解决: response = body length(int) + body bytes
            // 若是响应内容长度超阈值则抛异常
            int length = in.readInt();
            if (length <= 0) {
                throw new RpcException("RPC response has invalid length");
            }
            if (maxResponseLength > 0 && maxResponseLength < length) {
                throw new RpcException("RPC response exceeds maximum data length");
            }
            ByteBuffer buffer = ByteBuffer.allocate(length);
            // NOTE: 由于粘包问题, 这里使用readFully()读取完整的响应
            in.readFully(buffer.array());
            return buffer;
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws Exception {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

    // RPC调用线程和RPC响应处理线程中间桥梁:
    // 1.
    private class Call {

        private final int id;
        private final RPC.RpcKind rpcKind;
        // 请求是否结束
        private boolean done;
        // request
        private final Writeable rpcRequest;
        // response
        private Writeable rpcResponse;
        // null if success
        private IOException error;

        public Call(RPC.RpcKind rpcKind, Writeable request) {
            this.rpcKind = rpcKind;
            this.rpcRequest = request;
            this.id = nextCallId();
        }

        public synchronized void setException(IOException error) {
            this.error = error;
            callComplete();
        }

        public synchronized void setResponse(Writeable response) {
            this.rpcResponse = response;
            callComplete();
        }

        private synchronized void callComplete() {
            this.done = true;
            notify();
        }

    }
}
