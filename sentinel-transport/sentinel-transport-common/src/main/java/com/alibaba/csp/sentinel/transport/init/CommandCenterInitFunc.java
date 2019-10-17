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
package com.alibaba.csp.sentinel.transport.init;

import com.alibaba.csp.sentinel.command.CommandCenterProvider;
import com.alibaba.csp.sentinel.init.InitFunc;
import com.alibaba.csp.sentinel.init.InitOrder;
import com.alibaba.csp.sentinel.log.RecordLog;
import com.alibaba.csp.sentinel.transport.CommandCenter;

/**
 *  该类主要初始化客户端对外暴露出的endpoint
 *  这些endpoint主要给Sentinel-dashboard调用
 * @author Eric Zhao
 */
@InitOrder(-1)
public class CommandCenterInitFunc implements InitFunc {

    @Override
    public void init() throws Exception {
        //此处基于Sentinel自己实现的SpiLoader类来加载CommandCenter，
        // 最终获取的是sentinel-transport-simple-http中的 SimpleHttpCommandCenter
        //也就是说引入springCloud Alibaba sentinel 的客户端使用http与Sentinel-dashboard进行通讯
        //sentinel-transport组件下包含：
        //  sentinel-transport-common
        //  sentinel-transport-netty-http
        //  sentinel-transport-simple-http
        CommandCenter commandCenter = CommandCenterProvider.getCommandCenter();

        if (commandCenter == null) {
            RecordLog.warn("[CommandCenterInitFunc] Cannot resolve CommandCenter");
            return;
        }
        //从上面的spi机制加载出来的实例是SimpleHttpCommandCenter可知此处的beforeStart()方法
        // 是SimpleHttpCommandCenter里面的，该beforeStart()方法的功能是：
        //  用于从spi获取一堆的 com.alibaba.csp.sentinel.command.CommandHandler
        //位于 sentinel-transport /sentinel-transport-common 子项目的META-INF.services中
        //该Handler功能是一堆的处理请求响应 xxxHandler
        commandCenter.beforeStart();
        //通过创建SocketServer 与 dashboard进行通信
        commandCenter.start();
        RecordLog.info("[CommandCenterInit] Starting command center: "
                + commandCenter.getClass().getCanonicalName());
    }
}
