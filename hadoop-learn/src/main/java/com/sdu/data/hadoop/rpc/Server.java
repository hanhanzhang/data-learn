package com.sdu.data.hadoop.rpc;

import java.nio.channels.ServerSocketChannel;

/**
 * 1. Listener: 负责接收客户端连接请求, 将客户端请求封装 Connection 对象
 *
 * 2. Reader: 负责读取客户端链接请求,
 *
 * 3. Handler:
 *
 * 4. Responder
 * */
public abstract class Server {

    private class Listener extends Thread {

        private ServerSocketChannel ssc;

        private Reader[] readers;
        private int currentReader;



        private class Reader extends Thread {

        }

    }

    private class Handler extends Thread {

    }

}
