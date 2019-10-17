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
package com.alibaba.csp.sentinel.transport.command.http;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.Charset;

import com.alibaba.csp.sentinel.command.CommandHandler;
import com.alibaba.csp.sentinel.command.CommandRequest;
import com.alibaba.csp.sentinel.command.CommandResponse;
import com.alibaba.csp.sentinel.config.SentinelConfig;
import com.alibaba.csp.sentinel.log.CommandCenterLog;
import com.alibaba.csp.sentinel.transport.command.SimpleHttpCommandCenter;
import com.alibaba.csp.sentinel.transport.util.HttpCommandUtils;
import com.alibaba.csp.sentinel.util.StringUtil;

/***
 *
 * 主要处理对该  ip:port 发起请求时，对本次请求进行封装。
 * 获取 socket数据并进一步处理，
 * The task handles incoming command request in HTTP protocol.
 *
 * @author youji.zj
 * @author Eric Zhao
 */
public class HttpEventTask implements Runnable {

    private final Socket socket;

    private boolean writtenHead = false;

    public HttpEventTask(Socket socket) {
        this.socket = socket;
    }

    public void close() throws Exception {
        socket.close();
    }

    @Override
    public void run() {
        if (socket == null) {
            return;
        }

        BufferedReader in = null;
        PrintWriter printWriter = null;
        try {
            long start = System.currentTimeMillis();
            //转换成字符流 (BufferedReader引入了缓冲机制)
            in = new BufferedReader(new InputStreamReader(socket.getInputStream(), SentinelConfig.charset()));
             /*
             >>站在socket的编码处的位置来看(客户端)
             socket.getInputStream()：获取的是从服务器端发回的数据。
             socket.getOutputStream()：发送给服务器端的数据。

             >>站在SocketServer的编码处的位置来看(服务端)
             socket.getInputStream()： 获取客户端(主要是 sentinel-dashboard或用户通过浏览器访问) 发送给服务器端的数据流
             socket.getOutputStream()： 输出流，引入sentinel-core的业务应用将数据发给客户端。
                                        如业务应用A ，通过浏览器访问 xxx:8019/apis  ，此时将所有apis结果通过这个outputStream发送给浏览器
             */
            OutputStream outputStream = socket.getOutputStream();

            printWriter = new PrintWriter(
                new OutputStreamWriter(outputStream, Charset.forName(SentinelConfig.charset())));

            String line = in.readLine();
            CommandCenterLog.info("[SimpleHttpCommandCenter] socket income: " + line
                + "," + socket.getInetAddress());
            CommandRequest request = parseRequest(line);

            if (line.length() > 4 && StringUtil.equalsIgnoreCase("POST", line.substring(0, 4))) {
                // Deal with post method
                // Now simple-http only support form-encoded post request.
                String bodyLine = null;
                boolean bodyNext = false;
                boolean supported = false;
                int maxLength = 8192;
                while (true) {
                    // Body processing
                    if (bodyNext) {
                        if (!supported) {
                            break;
                        }
                        char[] bodyBytes = new char[maxLength];
                        int read = in.read(bodyBytes);
                        String postData = new String(bodyBytes, 0, read);
                        parseParams(postData, request);
                        break;
                    }

                    bodyLine = in.readLine();
                    if (bodyLine == null) {
                        break;
                    }
                    // Body seperator
                    if (StringUtil.isEmpty(bodyLine)) {
                        bodyNext = true;
                        continue;
                    }
                    // Header processing
                    int index = bodyLine.indexOf(":");
                    if (index < 1) {
                        continue;
                    }
                    String headerName = bodyLine.substring(0, index);
                    String header = bodyLine.substring(index + 1).trim();
                    if (StringUtil.equalsIgnoreCase("content-type", headerName)) {
                        if (StringUtil.equals("application/x-www-form-urlencoded", header)) {
                            supported = true;
                        } else {
                            // not support request
                            break;
                        }
                    } else if (StringUtil.equalsIgnoreCase("content-length", headerName)) {
                        try {
                            int len = new Integer(header);
                            if (len > 0) {
                                maxLength = len;
                            }
                        } catch (Exception e) {
                        }
                    }
                }
            }

            // Validate the target command.
            String commandName = HttpCommandUtils.getTarget(request);
            if (StringUtil.isBlank(commandName)) {
                badRequest(printWriter, "Invalid command");
                return;
            }
            //此处通过commandName获取具体的 Commandler 对象
            // Find the matching command handler.
            CommandHandler<?> commandHandler = SimpleHttpCommandCenter.getHandler(commandName);
            if (commandHandler != null) {
                //根据从请求参数中解析出来的具体commandName，找到对应的Handler，然后执行该Handler获取数据
                CommandResponse<?> response = commandHandler.handle(request);
                //处理 handler 执行 handle 后返回的结果
                handleResponse(response, printWriter, outputStream);
            } else {
                // No matching command handler.
                badRequest(printWriter, "Unknown command `" + commandName + '`');
            }
            printWriter.flush();

            long cost = System.currentTimeMillis() - start;
            CommandCenterLog.info("[SimpleHttpCommandCenter] Deal a socket task: " + line
                + ", address: " + socket.getInetAddress() + ", time cost: " + cost + " ms");
        } catch (Throwable e) {
            CommandCenterLog.warn("[SimpleHttpCommandCenter] CommandCenter error", e);
            try {
                if (printWriter != null) {
                    String errorMessage = SERVER_ERROR_MESSAGE;
                    if (!writtenHead) {
                        internalError(printWriter, errorMessage);
                    } else {
                        printWriter.println(errorMessage);
                    }
                    printWriter.flush();
                }
            } catch (Exception e1) {
                CommandCenterLog.warn("[SimpleHttpCommandCenter] Close server socket failed", e);
            }
        } finally {
            closeResource(in);
            closeResource(printWriter);
            closeResource(socket);
        }
    }

    private void closeResource(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                CommandCenterLog.warn("[SimpleHttpCommandCenter] Close resource failed", e);
            }
        }
    }

    /**
     * 处理 某个CommonHandler.handle()的运行结果给到调用者
     * @param response
     * @param printWriter
     * @param rawOutputStream
     * @throws Exception
     */
    private void handleResponse(CommandResponse response, /*@NonNull*/ final PrintWriter printWriter,
        /*@NonNull*/ final OutputStream rawOutputStream) throws Exception {
        if (response.isSuccess()) {
            if (response.getResult() == null) {
                writeOkStatusLine(printWriter);
                return;
            }
            // Write 200 OK status line.
            writeOkStatusLine(printWriter);
            // Here we directly use `toString` to encode the result to plain text.
            byte[] buffer = response.getResult().toString().getBytes(SentinelConfig.charset());
            rawOutputStream.write(buffer);
            rawOutputStream.flush();
        } else {
            String msg = SERVER_ERROR_MESSAGE;
            if (response.getException() != null) {
                msg = response.getException().getMessage();
            }
            badRequest(printWriter, msg);
        }
    }

    /**
     * Write `400 Bad Request` HTTP response status line and message body, then flush.
     */
    private void badRequest(/*@NonNull*/ final PrintWriter out, String message) {
        out.print("HTTP/1.1 400 Bad Request\r\n"
            + "Connection: close\r\n\r\n");
        out.print(message);
        out.flush();
        writtenHead = true;
    }

    /**
     * Write `500 Internal Server Error` HTTP response status line and message body, then flush.
     */
    private void internalError(/*@NonNull*/ final PrintWriter out, String message) {
        out.print("HTTP/1.1 500 Internal Server Error\r\n"
            + "Connection: close\r\n\r\n");
        out.print(message);
        out.flush();
        writtenHead = true;
    }

    /**
     * Write `200 OK` HTTP response status line and flush.
     */
    private void writeOkStatusLine(/*@NonNull*/ final PrintWriter out) {
        out.print("HTTP/1.1 200 OK\r\n"
            + "Connection: close\r\n\r\n");
        out.flush();
        writtenHead = true;
    }

    /**
     * Parse raw HTTP request line to a {@link CommandRequest}.
     *
     * @param line HTTP request line
     * @return parsed command request
     */
    private CommandRequest parseRequest(String line) {
        CommandRequest request = new CommandRequest();
        if (StringUtil.isBlank(line)) {
            return request;
        }
        int start = line.indexOf('/');
        int ask = line.indexOf('?') == -1 ? line.lastIndexOf(' ') : line.indexOf('?');
        int space = line.lastIndexOf(' ');
        String target = line.substring(start != -1 ? start + 1 : 0, ask != -1 ? ask : line.length());
        request.addMetadata(HttpCommandUtils.REQUEST_TARGET, target);
        if (ask == -1 || ask == space) {
            return request;
        }
        String parameterStr = line.substring(ask != -1 ? ask + 1 : 0, space != -1 ? space : line.length());
        parseParams(parameterStr, request);
        return request;
    }
    
    private void parseParams(String queryString, CommandRequest request) {
        for (String parameter : queryString.split("&")) {
            if (StringUtil.isBlank(parameter)) {
                continue;
            }

            String[] keyValue = parameter.split("=");
            if (keyValue.length != 2) {
                continue;
            }

            String value = StringUtil.trim(keyValue[1]);
            try {
                value = URLDecoder.decode(value, SentinelConfig.charset());
            } catch (UnsupportedEncodingException e) {
            }

            request.addParam(StringUtil.trim(keyValue[0]), value);
        }
    }

    private static final String SERVER_ERROR_MESSAGE = "Command server error";

}
