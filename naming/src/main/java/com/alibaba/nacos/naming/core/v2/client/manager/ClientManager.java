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

package com.alibaba.nacos.naming.core.v2.client.manager;

import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientVerifyInfo;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;

import java.util.Collection;

/**
 * 管理 naming 客户端
 * The manager of {@code Client} Nacos naming client.
 *
 * @author xiweng.yy
 */
public interface ClientManager {
    
    /**
     * 新的客户端连接
     * New client connected.
     *
     * @param clientId new client id
     * @param attributes client attributes, which can help create client
     * @return true if add successfully, otherwise false
     */
    boolean clientConnected(String clientId, ClientAttributes attributes);
    
    /**
     * 新的客户端连接
     * New client connected.
     *
     * @param client new client
     * @return true if add successfully, otherwise false
     */
    boolean clientConnected(Client client);
    
    /**
     * 新的同步客户端连接
     * New sync client connected.
     *
     * @param clientId   synced client id
     * @param attributes client sync attributes, which can help create sync client
     * @return true if add successfully, otherwise false
     */
    boolean syncClientConnected(String clientId, ClientAttributes attributes);
    
    /**
     * 客户端断开连接
     * Client disconnected.
     *
     * @param clientId client id
     * @return true if remove successfully, otherwise false
     */
    boolean clientDisconnected(String clientId);
    
    /**
     * 根据id获取客户端
     * Get client by id.
     *
     * @param clientId client id
     * @return client
     */
    Client getClient(String clientId);
    
    /**
     * 指定客户端是否存在
     * Whether the client id exists.
     *
     * @param clientId client id
     * @return client
     */
    boolean contains(final String clientId);
    
    /**
     * 返回全部客户端id
     * All client id.
     *
     * @return collection of client id
     */
    Collection<String> allClientId();
    
    /**
     * 当前客户端是否响应请求
     * Whether the client is responsible by current server.
     *
     * @param client client
     * @return true if responsible, otherwise false
     */
    boolean isResponsibleClient(Client client);
    
    /**
     * 校验客户端
     * verify client.
     *
     * @param verifyData verify data from remote responsible server
     * @return true if client is valid, otherwise is false.
     */
    boolean verifyClient(DistroClientVerifyInfo verifyData);
}
