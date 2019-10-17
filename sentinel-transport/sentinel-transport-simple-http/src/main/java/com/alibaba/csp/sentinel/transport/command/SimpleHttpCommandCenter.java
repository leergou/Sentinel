/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.transport.command;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;

import com.alibaba.csp.sentinel.command.CommandHandler;
import com.alibaba.csp.sentinel.command.CommandHandlerProvider;
import com.alibaba.csp.sentinel.concurrent.NamedThreadFactory;
import com.alibaba.csp.sentinel.log.CommandCenterLog;
import com.alibaba.csp.sentinel.transport.CommandCenter;
import com.alibaba.csp.sentinel.transport.command.http.HttpEventTask;
import com.alibaba.csp.sentinel.transport.config.TransportConfig;
import com.alibaba.csp.sentinel.util.StringUtil;

/***
 * The simple command center provides service to exchange information.
 *
 * @author youji.zj
 */
public class SimpleHttpCommandCenter implements CommandCenter {

    private static final int PORT_UNINITIALIZED = -1;

    private static final int DEFAULT_SERVER_SO_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8719;

    /**
     * 该Map 存放的数据key是：CommandHandler类型的子类注解 name的值
     *  value是对应的具体的Handler
     */
    @SuppressWarnings("rawtypes")
    private static final Map<String, CommandHandler> handlerMap = new ConcurrentHashMap<String, CommandHandler>();

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    private ExecutorService executor = Executors.newSingleThreadExecutor(
        new NamedThreadFactory("sentinel-command-center-executor"));
    private ExecutorService bizExecutor;

    private ServerSocket socketReference;

    @Override
    @SuppressWarnings("rawtypes")
    public void beforeStart() throws Exception {
        // Register handlers
        //此处是通过Java自带的ServiceLoad.load方法将 classpath下的META-INF/services/com.alibaba.csp.sentinel.command.CommandHandler配置文件中的类load进来
        //目前只有在sentinel-transport-common 下有配置
        Map<String, CommandHandler> handlers = CommandHandlerProvider.getInstance().namedHandlers();
        //进行解析注册-即存放到handlerMap中
        registerCommands(handlers);
    }

    @Override
    public void start() throws Exception {
        //获取当前机器可用cpu数量
        int nThreads = Runtime.getRuntime().availableProcessors();
        //创建一个线程池, 核心线程数与最大线程数一致，(类似于 Executors.newFixedThreadPool(nThreads))
        // 超时时间为0，当maximumPoolSize > corePoolSize时，多余corePoolSize的线程数的存活等待时间
        this.bizExecutor = new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(10),
            new NamedThreadFactory("sentinel-command-center-service-executor"),
                //采用拒绝策略，拒绝处理时，将会抛异常出来
                new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    CommandCenterLog.info("EventTask rejected");
                    throw new RejectedExecutionException();
                }
            });

        //创建一个服务初始化任务 线程 对象
        Runnable serverInitTask = new Runnable() {
            int port;

            //这里出现一个构造块 {  }，
            // 构造块在每一次new对象时都会被执行一次

            {
                try {
                    //从配置中获取通信端口，spring cloud alibaba sentinel中该端口默认为 8719
                    port = Integer.parseInt(TransportConfig.getPort());
                } catch (Exception e) {
                    port = DEFAULT_PORT;
                }
            }

            @Override
            public void run() {
                boolean success = false;
                //此处通过 ServerSocket(int port, int backlog) 进行探测，成功则创建一个serverSocket
                ServerSocket serverSocket = getServerSocketFromBasePort(port);

                if (serverSocket != null) {
                    CommandCenterLog.info("[CommandCenter] Begin listening at port " + serverSocket.getLocalPort());
                    socketReference = serverSocket;
                    // 此处为何使用一个 1个线程的线程池来提交执行线程任务呢，
                    // 直接采用   newServerThread(serverSocket).start() 也能达到同样的运行效果
                    executor.submit(new ServerThread(serverSocket));
                    success = true;
                    port = serverSocket.getLocalPort();
                } else {
                    CommandCenterLog.info("[CommandCenter] chooses port fail, http command center will not work");
                }

                if (!success) {
                    port = PORT_UNINITIALIZED;
                }
                //将本次代码逻辑执行完毕之后的port值设置到 transportConfig上
                TransportConfig.setRuntimePort(port);

                // executor.submit(new ServerThread(serverSocket))
                // 是通过这个executor执行了另外一个线程对象 -> ServerThread，该线程以 while(true) 的轮询方式监听端口，
                // 特别说明一点serverSocket，使用的是 bio socket,因此在accept()方法位置，阻塞等待客户端建立连接。
                // serverSocket线程执行后，关闭这个触发serverSocket线程的  newSingleThreadExecutor线程池
                // 正常情况下访问xxxCommandHandler的请求者，主要是sentinel-dashboard。该dashboard的调用频率可控，所以此处使用 bio 不会影响性能
                executor.shutdown();
            }

        };
        //通过一个线程将监听服务启动
        new Thread(serverInitTask).start();
    }

    /**
     * Get a server socket from an available port from a base port.<br>
     * Increasing on port number will occur when the port has already been used.
     *
     * @param basePort base port to start
     * @return new socket with available port
     */
    private static ServerSocket getServerSocketFromBasePort(int basePort) {
        int tryCount = 0;
        while (true) {
            try {
                //此处通过 ServerSocket(int port, int backlog) 构造函数设置监听端口，
                // 在springcloud alibaba sentinel中该端口默认是8719,通过该构造函数，尝试创建ServerSocket，
                // 若创建失败，则说明当前port被占用了，通过tryCount++；与3进行取整 的方式，改变当前客户端对外监听的端口号，
                // 从而根据计算后的值，进行探测创建ServerSocket，如 8720，8721，8722..
                //backlog参数的解释，参考博客：https://www.cnblogs.com/qiumingcheng/p/9492962.html
                ServerSocket server = new ServerSocket(basePort + tryCount / 3, 100);
                /*
                    启用/禁用SO_REUSEADDR套接字选项。
                    当TCP连接关闭时，连接可能会在连接关闭后一段时间内保持在超时状态（通常称为TIME_WAIT状态或2MSL等待状态）。
                    对于使用众所周知的套接字地址或端口的应用SocketAddress如果在涉及套接字地址或端口的超时状态中存在连接，则可能无法将套接字绑定到所需的SocketAddress 。
                    启用SO_REUSEADDR之前结合使用套接字bind(SocketAddress)允许在上一个连接处于超时状态时绑定套接字。

                    在使用bind(SocketAddress)绑定套接字之前启用SO_REUSEADDR,即使先前的连接处于超时状态，也可以绑定套接字
                 */
                server.setReuseAddress(true);
                return server;
            } catch (IOException e) {
                tryCount++;
                try {
                    TimeUnit.MILLISECONDS.sleep(30);
                } catch (InterruptedException e1) {
                    break;
                }
            }
        }
        return null;
    }

    @Override
    public void stop() throws Exception {
        if (socketReference != null) {
            try {
                socketReference.close();
            } catch (IOException e) {
                CommandCenterLog.warn("Error when releasing the server socket", e);
            }
        }
        bizExecutor.shutdownNow();
        executor.shutdownNow();
        TransportConfig.setRuntimePort(PORT_UNINITIALIZED);
        handlerMap.clear();
    }

    /**
     * Get the name set of all registered commands.
     */
    public static Set<String> getCommands() {
        return handlerMap.keySet();
    }

    //当前进程内启动一个线程，监听固定端口，如8019 (若被占用则递增探测)，来处理外部的请求
    class ServerThread extends Thread {

        private ServerSocket serverSocket;

        ServerThread(ServerSocket s) {
            this.serverSocket = s;
            setName("sentinel-courier-server-accept-thread");
        }

        @Override
        public void run() {
            while (true) {
                Socket socket = null;
                try {
                    //阻塞block 等待与client端建立连接
                    socket = this.serverSocket.accept();
                    //默认3秒钟超时时间
                    setSocketSoTimeout(socket);
                    //将建立连接返回的socket封装到HttpEventTask 线程对象中
                    HttpEventTask eventTask = new HttpEventTask(socket);
                    //使用业务线程池异步处理
                    bizExecutor.submit(eventTask);
                } catch (Exception e) {
                    CommandCenterLog.info("Server error", e);
                    if (socket != null) {
                        try {
                            socket.close();
                        } catch (Exception e1) {
                            CommandCenterLog.info("Error when closing an opened socket", e1);
                        }
                    }
                    try {
                        // In case of infinite log.
                        Thread.sleep(10);
                    } catch (InterruptedException e1) {
                        // Indicates the task should stop.
                        break;
                    }
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static CommandHandler getHandler(String commandName) {
        return handlerMap.get(commandName);
    }

    @SuppressWarnings("rawtypes")
    public static void registerCommands(Map<String, CommandHandler> handlerMap) {
        if (handlerMap != null) {
            for (Entry<String, CommandHandler> e : handlerMap.entrySet()) {
                registerCommand(e.getKey(), e.getValue());
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static void registerCommand(String commandName, CommandHandler handler) {
        if (StringUtil.isEmpty(commandName)) {
            return;
        }

        if (handlerMap.containsKey(commandName)) {
            CommandCenterLog.warn("Register failed (duplicate command): " + commandName);
            return;
        }

        handlerMap.put(commandName, handler);
    }

    /**
     * Avoid server thread hang, 3 seconds timeout by default.
     */
    private void setSocketSoTimeout(Socket socket) throws SocketException {
        if (socket != null) {
            socket.setSoTimeout(DEFAULT_SERVER_SO_TIMEOUT);
        }
    }
}
